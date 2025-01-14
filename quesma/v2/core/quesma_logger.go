// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package quesma_api

import (
	"context"
	"github.com/QuesmaOrg/quesma/quesma/v2/core/tracing"
	"github.com/rs/zerolog"
	"os"
	"time"
)

const (
	stdLogFileName = "quesma.log"
	errLogFileName = "err.log"
)

const (
	RID                              = "request_id" // request id key for the logger
	Reason                           = "reason"     // Known error reason key for the logger
	Path                             = "path"
	AsyncId                          = "async_id"
	OpaqueId                         = "opaque_id"
	ReasonPrefixUnsupportedQueryType = "unsupported_search_query: " // Reason for Error messages for unsupported queries will start with this prefix
)

const (
	initialBufferSize = 32 * 1024
	bufferSizeChannel = 1024
)

type QuesmaLogger interface {
	Debug() *zerolog.Event
	Info() *zerolog.Event
	Warn() *zerolog.Event
	Error() *zerolog.Event
	Fatal() *zerolog.Event
	Panic() *zerolog.Event

	DebugWithCtx(ctx context.Context) *zerolog.Event
	InfoWithCtx(ctx context.Context) *zerolog.Event
	WarnWithCtx(ctx context.Context) *zerolog.Event
	ErrorWithCtx(ctx context.Context) *zerolog.Event

	WarnWithCtxAndReason(ctx context.Context, reason string) *zerolog.Event
	ErrorWithCtxAndReason(ctx context.Context, reason string) *zerolog.Event

	MarkTraceEndWithCtx(ctx context.Context) *zerolog.Event

	WithComponent(name string) QuesmaLogger
}

type QuesmaLoggerImpl struct {
	zerolog.Logger
}

func NewQuesmaLogger(log zerolog.Logger) QuesmaLogger {
	return &QuesmaLoggerImpl{
		Logger: log,
	}
}

func (l *QuesmaLoggerImpl) WithComponent(name string) QuesmaLogger {
	return &QuesmaLoggerImpl{
		Logger: l.Logger.With().Str("component", name).Logger(),
	}
}

func (l *QuesmaLoggerImpl) MarkTraceEndWithCtx(ctx context.Context) *zerolog.Event {
	event := l.Info().Ctx(ctx)
	event = l.addKnownContextValues(event, ctx)
	ctx = context.WithValue(ctx, tracing.TraceEndCtxKey, true)
	event = event.Ctx(ctx)
	return event
}

func (l *QuesmaLoggerImpl) WarnWithCtxAndReason(ctx context.Context, reason string) *zerolog.Event {
	return l.WarnWithCtx(context.WithValue(ctx, tracing.ReasonCtxKey, reason))
}

func (l *QuesmaLoggerImpl) ErrorWithCtxAndReason(ctx context.Context, reason string) *zerolog.Event {
	return l.ErrorWithCtx(context.WithValue(ctx, tracing.ReasonCtxKey, reason))
}

func (l *QuesmaLoggerImpl) addKnownContextValues(event *zerolog.Event, ctx context.Context) *zerolog.Event {

	if requestId, ok := ctx.Value(tracing.RequestIdCtxKey).(string); ok {
		event = event.Str(RID, requestId)
	}
	if path, ok := ctx.Value(tracing.RequestPath).(string); ok {
		event = event.Str(Path, path)
	}
	if reason, ok := ctx.Value(tracing.ReasonCtxKey).(string); ok {
		event = event.Str(Reason, reason)
	}
	if asyncId, ok := ctx.Value(tracing.AsyncIdCtxKey).(string); ok {
		if asyncId != "" {
			event = event.Str(AsyncId, asyncId)
		}
	}

	if requestId, ok := ctx.Value(tracing.OpaqueIdCtxKey).(string); ok {
		event = event.Str(OpaqueId, requestId)
	}

	return event
}

func (l *QuesmaLoggerImpl) DebugWithCtx(ctx context.Context) *zerolog.Event {
	event := l.Debug().Ctx(ctx)
	event = l.addKnownContextValues(event, ctx)
	return event
}

func (l *QuesmaLoggerImpl) InfoWithCtx(ctx context.Context) *zerolog.Event {
	event := l.Info().Ctx(ctx)
	event = l.addKnownContextValues(event, ctx)
	return event
}

func (l *QuesmaLoggerImpl) WarnWithCtx(ctx context.Context) *zerolog.Event {
	event := l.Warn().Ctx(ctx)
	event = l.addKnownContextValues(event, ctx)
	return event

}

func (l *QuesmaLoggerImpl) ErrorWithCtx(ctx context.Context) *zerolog.Event {
	event := l.Error().Ctx(ctx)
	event = l.addKnownContextValues(event, ctx)
	return event
}

func EmptyQuesmaLogger() QuesmaLogger {
	// not so empty :D
	return NewQuesmaLogger(zerolog.New(
		zerolog.ConsoleWriter{
			Out:        os.Stderr,
			TimeFormat: time.StampMilli,
		}).
		Level(zerolog.DebugLevel).
		With().
		Timestamp().
		Logger())

}
