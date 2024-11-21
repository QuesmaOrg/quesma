// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package tracing

import (
	"context"
	"fmt"
	"github.com/rs/zerolog"
	"quesma/util"
	"strings"
	"time"
)

const (
	DumpedCtxKey ContextKey = "DumpCtxKey"
)

// This is representation of logger hook which
// buffers async query log messages and flush them
// only in case of error or successful end of async query
type AsyncTraceLogger struct {
	AsyncQueryTrace *util.SyncMap[string, TraceCtx]
}

type TraceCtx struct {
	Messages []string
	Path     string // currently not used
	Added    time.Time
	Updated  time.Time
}

func FormatMessages(messages []string) string {
	var formattedLines strings.Builder
	formattedLines.WriteString("[\n")
	for _, line := range messages {
		formattedLines.WriteString(fmt.Sprintf("\t%s\n", line))
	}
	formattedLines.WriteString("]")
	return formattedLines.String()
}

func (h *AsyncTraceLogger) Run(e *zerolog.Event, level zerolog.Level, message string) {
	ctx := e.GetCtx()
	if asyncId, ok := ctx.Value(AsyncIdCtxKey).(string); ok {
		_, ok = ctx.Value(DumpedCtxKey).(bool)
		if ok {
			return
		}
		if len(asyncId) == 0 {
			return
		}

		if _, ok = ctx.Value(TraceEndCtxKey).(bool); ok {
			e.Discard()
			h.AsyncQueryTrace.Delete(asyncId)
		} else if level == zerolog.ErrorLevel || level == zerolog.FatalLevel || level == zerolog.PanicLevel {
			defer h.AsyncQueryTrace.Delete(asyncId)
			defer e.Discard()
			var traceCtx TraceCtx
			traceCtx.Path = ""
			if bufferedTraceCtx, ok := h.AsyncQueryTrace.Load(asyncId); ok {
				traceCtx.Messages = append(traceCtx.Messages, bufferedTraceCtx.Messages...)
			}
			traceCtx.Messages = append(traceCtx.Messages, message)
			traceCtx.Updated = time.Now()
			ctx = context.WithValue(ctx, DumpedCtxKey, true)
			e = e.Ctx(ctx)
			var formattedLines strings.Builder
			formattedLines.WriteString(FormatMessages(traceCtx.Messages))
			// Below e.Msgf call is recursive one which means that after it we are inside outer Run method
			// to avoid recursion we need to call Discard method
			// We could call Str() here and avoid some
			// recursion checks unnecessary
			// however it would prevent us from custom formatting
			e.Msgf("Async query error trace: %s", formattedLines.String())
		} else {
			// Buffer all non-error messages and discard them
			if h.AsyncQueryTrace != nil {
				if traceCtx, ok := h.AsyncQueryTrace.Load(asyncId); ok {
					traceCtx.Messages = append(traceCtx.Messages, message)
					traceCtx.Updated = time.Now()
					h.AsyncQueryTrace.Store(asyncId, traceCtx)
				} else {
					traceCtx = TraceCtx{
						Messages: []string{message},
						Path:     "",
						Added:    time.Now(),
					}
					h.AsyncQueryTrace.Store(asyncId, traceCtx)
				}
				e.Discard()
			}
		}
	}
}
