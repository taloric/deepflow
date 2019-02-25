package mapreduce

import (
	"reflect"
	"strconv"
	"time"

	"math/rand"

	"gitlab.x.lan/application/droplet-app/pkg/mapper/usage"
	"gitlab.x.lan/yunshan/droplet-libs/app"
	"gitlab.x.lan/yunshan/droplet-libs/datatype"
	"gitlab.x.lan/yunshan/droplet-libs/queue"
	"gitlab.x.lan/yunshan/droplet-libs/stats"
)

func NewMeteringMapProcess(
	output queue.MultiQueueWriter, input queue.MultiQueueReader,
	inputCount, docsInBuffer, variedDocLimit, windowSize, windowMoveMargin int,
) *MeteringHandler {
	return NewMeteringHandler(
		[]app.MeteringProcessor{usage.NewProcessor()}, output, input,
		inputCount, docsInBuffer, variedDocLimit, windowSize, windowMoveMargin)
}

type MeteringHandler struct {
	numberOfApps int
	processors   []app.MeteringProcessor

	meteringQueue      queue.MultiQueueReader
	meteringQueueCount int
	zmqAppQueue        queue.MultiQueueWriter
	docsInBuffer       int
	variedDocLimit     int
	windowSize         int
	windowMoveMargin   int
}

func NewMeteringHandler(
	processors []app.MeteringProcessor,
	output queue.MultiQueueWriter, inputs queue.MultiQueueReader,
	inputCount, docsInBuffer, variedDocLimit, windowSize, windowMoveMargin int,
) *MeteringHandler {
	return &MeteringHandler{
		numberOfApps:       len(processors),
		processors:         processors,
		zmqAppQueue:        output,
		meteringQueue:      inputs,
		meteringQueueCount: inputCount,
		docsInBuffer:       docsInBuffer,
		variedDocLimit:     variedDocLimit,
		windowSize:         windowSize,
		windowMoveMargin:   windowMoveMargin,
	}
}

type subMeteringHandler struct {
	numberOfApps int
	names        []string
	processors   []app.MeteringProcessor
	stashes      []Stash

	meteringQueue queue.MultiQueueReader
	zmqAppQueue   queue.MultiQueueWriter

	queueIndex int
	hashKey    queue.HashKey

	lastFlush    time.Duration
	counterLatch int
	statItems    []stats.StatItem

	handlerCounter   []HandlerCounter
	processorCounter [][]ProcessorCounter
}

func (h *MeteringHandler) newSubMeteringHandler(index int) *subMeteringHandler {
	dupProcessors := make([]app.MeteringProcessor, h.numberOfApps)
	for i, proc := range h.processors {
		elem := reflect.ValueOf(proc).Elem()
		ref := reflect.New(elem.Type())
		ref.Elem().Set(elem)
		dupProcessors[i] = ref.Interface().(app.MeteringProcessor)
		dupProcessors[i].Prepare()
	}
	handler := subMeteringHandler{
		numberOfApps: h.numberOfApps,
		names:        make([]string, h.numberOfApps),
		processors:   dupProcessors,
		stashes:      make([]Stash, h.numberOfApps),

		meteringQueue: h.meteringQueue,
		zmqAppQueue:   h.zmqAppQueue,

		queueIndex: index,
		hashKey:    queue.HashKey(rand.Int()),

		lastFlush: time.Duration(time.Now().UnixNano()),

		statItems: make([]stats.StatItem, 0),

		handlerCounter:   make([]HandlerCounter, 2),
		processorCounter: make([][]ProcessorCounter, 2),
	}
	handler.processorCounter[0] = make([]ProcessorCounter, handler.numberOfApps)
	handler.processorCounter[1] = make([]ProcessorCounter, handler.numberOfApps)

	for i := 0; i < handler.numberOfApps; i++ {
		handler.names[i] = handler.processors[i].GetName()
		handler.stashes[i] = NewSlidingStash(h.docsInBuffer, h.variedDocLimit, h.windowSize, h.windowMoveMargin)
	}
	stats.RegisterCountable("metering-mapper", &handler, stats.OptionStatTags{"index": strconv.Itoa(index)})
	return &handler
}

func (h *subMeteringHandler) GetCounter() interface{} {
	oldLatch := h.counterLatch
	if h.counterLatch == 0 {
		h.counterLatch = 1
	} else {
		h.counterLatch = 0
	}
	h.statItems = h.statItems[:0]
	h.statItems = FillStatItems(h.statItems, h.handlerCounter[oldLatch], h.names, h.processorCounter[oldLatch])
	for i := 0; i < h.numberOfApps; i++ {
		h.processorCounter[oldLatch][i] = ProcessorCounter{}
	}
	h.handlerCounter[oldLatch] = HandlerCounter{}

	return h.statItems
}

func (h *subMeteringHandler) Closed() bool {
	return false // FIXME: never close?
}

// processorID = -1 for all stash
func (h *subMeteringHandler) putToQueue(processorID int) {
	for i, stash := range h.stashes {
		if processorID >= 0 && processorID != i {
			continue
		}
		docs := stash.Dump()
		for j := 0; j < len(docs); j += QUEUE_BATCH_SIZE {
			if j+QUEUE_BATCH_SIZE <= len(docs) {
				h.zmqAppQueue.Put(h.hashKey, docs[j:j+QUEUE_BATCH_SIZE]...)
			} else {
				h.zmqAppQueue.Put(h.hashKey, docs[j:]...)
			}
			h.hashKey++
		}
		h.processorCounter[h.counterLatch][i].emitCounter += uint64(len(docs))
		stash.Clear()
	}
}

func (h *MeteringHandler) Start() {
	for i := 0; i < h.meteringQueueCount; i++ {
		go h.newSubMeteringHandler(i).Process()
	}
}

func (h *subMeteringHandler) Process() error {
	elements := make([]interface{}, QUEUE_BATCH_SIZE)

	for {
		n := h.meteringQueue.Gets(queue.HashKey(h.queueIndex), elements)
		for _, e := range elements[:n] {
			if e == nil { // tick
				h.Flush(-1)
				continue
			}

			metering := e.(*datatype.MetaPacket)
			if metering.PolicyData == nil || metering.EndpointData == nil { // shouldn't happen
				log.Warningf("drop invalid packet with nil PolicyData or EndpointData %v", metering)
				datatype.ReleaseMetaPacket(metering)
				h.handlerCounter[h.counterLatch].dropCounter++
				continue
			}
			now := time.Duration(time.Now().UnixNano())
			if metering.Timestamp > now+time.Minute {
				datatype.ReleaseMetaPacket(metering)
				h.handlerCounter[h.counterLatch].dropCounter++
				continue
			}

			h.handlerCounter[h.counterLatch].flowCounter++
			for i, processor := range h.processors {
				docs := processor.Process(metering, false)
				rejected := uint64(0)
				h.processorCounter[h.counterLatch][i].docCounter += uint64(len(docs))
				if uint64(len(docs)) > h.processorCounter[h.counterLatch][i].maxCounter {
					h.processorCounter[h.counterLatch][i].maxCounter = uint64(len(docs))
				}
				for {
					docs, rejected = h.stashes[i].Add(docs)
					h.processorCounter[h.counterLatch][i].rejectionCounter += rejected
					if docs == nil {
						break
					}
					h.processorCounter[h.counterLatch][i].flushCounter++
					h.Flush(i)
				}
			}
			datatype.ReleaseMetaPacket(metering)
		}
		if time.Duration(time.Now().UnixNano())-h.lastFlush >= FLUSH_INTERVAL {
			h.Flush(-1)
		}
	}
}

func (h *subMeteringHandler) Flush(processorID int) {
	if processorID == -1 { // 单独Flush某个processor的stash时不更新
		h.lastFlush = time.Duration(time.Now().UnixNano())
	}
	h.putToQueue(processorID)
}
