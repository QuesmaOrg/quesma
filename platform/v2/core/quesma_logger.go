// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package quesma_api

import (
	"context"
	"fmt"
	"github.com/QuesmaOrg/quesma/platform/v2/core/tracing"
	"github.com/hashicorp/golang-lru/v2/expirable"
	"github.com/rs/zerolog"
	"hash/fnv"
	"os"
	"time"
)

const (
	RID      = "request_id" // request id key for the logger
	Reason   = "reason"     // Known error reason key for the logger
	Path     = "path"
	AsyncId  = "async_id"
	OpaqueId = "opaque_id"

	DefaultBurstSamplerPeriodSeconds    = 20  // burst up to 600 lines of logs per 20 seconds period
	DefaultBurstSamplerMaxLogsPerSecond = 30  // ~100k lines of logs per hour
	DefaultSheddingFrequency            = 100 // when the limit is exhausted, log every ~ 100 log lines

	DeduplicatedLogsCacheSize  = 1000
	DeduplicatedLogsExpiryTime = 1 * time.Minute
)

type QuesmaLogger interface {
	Debug() *zerolog.Event
	Info() *zerolog.Event
	Warn() *zerolog.Event
	Error() *zerolog.Event
	Fatal() *zerolog.Event
	Panic() *zerolog.Event

	// TODO: Add similar for other log levels
	DeduplicatedInfo() DeduplicatedEvent
	DeduplicatedWarn() DeduplicatedEvent

	DebugWithCtx(ctx context.Context) *zerolog.Event
	InfoWithCtx(ctx context.Context) *zerolog.Event
	WarnWithCtx(ctx context.Context) *zerolog.Event
	ErrorWithCtx(ctx context.Context) *zerolog.Event

	DebugWithReason(reason string) *zerolog.Event
	InfoWithReason(reason string) *zerolog.Event
	WarnWithReason(reason string) *zerolog.Event
	ErrorWithReason(reason string) *zerolog.Event

	DebugWithCtxAndReason(ctx context.Context, reason string) *zerolog.Event
	InfoWithCtxAndReason(ctx context.Context, reason string) *zerolog.Event
	WarnWithCtxAndReason(ctx context.Context, reason string) *zerolog.Event
	ErrorWithCtxAndReason(ctx context.Context, reason string) *zerolog.Event

	MarkTraceEndWithCtx(ctx context.Context) *zerolog.Event

	WithComponent(name string) QuesmaLogger
}

type QuesmaLoggerImpl struct {
	zerolog.Logger

	deduplicatedLogs *expirable.LRU[any, struct{}]
}

func NewQuesmaLogger(log zerolog.Logger) QuesmaLogger {
	return &QuesmaLoggerImpl{
		Logger:           log,
		deduplicatedLogs: expirable.NewLRU[any, struct{}](DeduplicatedLogsCacheSize, nil, DeduplicatedLogsExpiryTime),
	}
}

func (l *QuesmaLoggerImpl) WithComponent(name string) QuesmaLogger {
	return NewQuesmaLogger(l.Logger.With().Str("component", name).Logger())
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

func (l *QuesmaLoggerImpl) DeduplicatedInfo() DeduplicatedEvent {
	return DeduplicatedEvent{
		event: l.Info(),
		l:     l,
	}
}

func (l *QuesmaLoggerImpl) DeduplicatedWarn() DeduplicatedEvent {
	return DeduplicatedEvent{
		event: l.Warn(),
		l:     l,
	}
}

type DeduplicatedEvent struct {
	event *zerolog.Event
	l     *QuesmaLoggerImpl
}

func hashMsgf(format string, v ...interface{}) uint32 {
	// []interface{} is not hashable, so we need to hash it manually
	// For the convenience sake we just hash a Print representation
	h := fnv.New32a()
	fmt.Fprint(h, format, v)
	return h.Sum32()
}

// TODO: Add wrappers for other *zerolog.Event methods
func (m DeduplicatedEvent) Msgf(format string, v ...interface{}) {
	hash := hashMsgf(format, v)

	if m.l.deduplicatedLogs.Contains(hash) {
		return
	}

	m.l.deduplicatedLogs.Add(hash, struct{}{})
	m.event.Msgf(format, v...)
}

func EmptyQuesmaLogger() QuesmaLogger {
	// not so empty :D
	return NewQuesmaLogger(zerolog.New(
		zerolog.ConsoleWriter{
			Out:        os.Stderr,
			TimeFormat: time.StampMilli,
		}).
		Level(zerolog.DebugLevel).
		Sample(&zerolog.BurstSampler{
			Burst:       DefaultBurstSamplerMaxLogsPerSecond * DefaultBurstSamplerPeriodSeconds,
			Period:      DefaultBurstSamplerPeriodSeconds * time.Second,
			NextSampler: zerolog.RandomSampler(DefaultSheddingFrequency),
		}).
		With().
		Timestamp().
		Logger())

}
