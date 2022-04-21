/* Copyright (c) 2020 YunShan.net, Inc.
 *
 * Author: jiping@yunshan.net.cn
 */
#include <string.h>
#include <stdio.h>
#include <stdint.h>
#include <sys/uio.h>
#include <stdarg.h>
#include <stdlib.h>
#include <time.h>
#include "log.h"

FILE *log_stream;
bool log_to_stdout;

void os_panic(void) __attribute__ ((weak));

void os_panic(void)
{
	abort();
}

void os_exit(int) __attribute__ ((weak));

void os_exit(int code)
{
	exit(code);
}

void os_puts(char *string, uint32_t string_length, bool is_stdout)
{
	int fd;
	struct iovec iovs[2];
	int n_iovs = 0;

	iovs[n_iovs].iov_base = string;
	iovs[n_iovs].iov_len = string_length;
	n_iovs++;

	if (is_stdout)
		writev(1, iovs, n_iovs);

	if (!log_stream)
		return;

	fd = fileno(log_stream);
	writev(fd, iovs, n_iovs);
}

static void debugger(void)
{
	os_panic();
}

static void error_exit(int code)
{
	os_exit(code);
}

static char *dispatch_message(char *msg, uint16_t len)
{
	if (!msg)
		return msg;

	if (log_to_stdout)
		os_puts(msg, len, true);
	else
		os_puts(msg, len, false);

	return msg;
}

void _ebpf_error(int how_to_die,
		  char *function_name, uint32_t line_number, char *fmt, ...)
{
	char msg[MSG_SZ];
	uint16_t len = 0;
	uint16_t max = MSG_SZ;
	va_list va;

	if (function_name) {
		len += snprintf(msg + len, max - len, "%s:", function_name);
		if (line_number > 0)
			len +=
			    snprintf(msg + len, max - len, "%u:",
				     line_number);
	}
#ifdef HAVE_ERRNO
	if (how_to_die & ERROR_ERRNO_VALID)
		len += snprintf(msg + len, max - len,
				": %s (errno %d)", strerror(errno), errno);
#endif

	va_start(va, fmt);
	len += vsnprintf(msg + len, max - len, fmt, va);
	va_end(va);

	if (msg[len - 1] != '\n') {
		if (len < max)
			msg[len++] = '\n';
		else
			msg[len - 1] = '\n';
	}

	dispatch_message(msg, len);

	if (how_to_die & ERROR_ABORT)
		debugger();

	if (how_to_die & ERROR_FATAL)
		error_exit(1);
}

void _ebpf_info(char *fmt, ...)
{
	char msg[MSG_SZ];
	uint16_t len = 0;
	uint16_t max = MSG_SZ;
	time_t timep;
	struct tm *p;
	time(&timep);
	p = localtime(&timep);
	va_list va;

	len += snprintf(msg + len, max - len, "%d-%02d-%02d %d:%d:%d ",
			(1900 + p->tm_year), (1 + p->tm_mon), p->tm_mday,
			p->tm_hour, p->tm_min, p->tm_sec);

	va_start(va, fmt);
	len += vsnprintf(msg + len, max - len, fmt, va);
	va_end(va);
	if (msg[len - 1] != '\n') {
		if (len < max)
			msg[len++] = '\n';
		else
			msg[len - 1] = '\n';
	}

	dispatch_message(msg, len);
}
