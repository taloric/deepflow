package dd_import

import (
	flowlogCfg "github.com/deepflowio/deepflow/server/ingester/flow_log/config"
	"github.com/deepflowio/deepflow/server/ingester/flow_log/log_data"
	"github.com/deepflowio/deepflow/server/libs/grpc"
)

func DDogDataToL7FlowLogs(vtapID, orgId, teamId uint16, ddog, peerIP []byte, uri string, platformData *grpc.PlatformInfoTable, cfg *flowlogCfg.Config) []*log_data.L7FlowLog {
	return []*log_data.L7FlowLog{}
}
