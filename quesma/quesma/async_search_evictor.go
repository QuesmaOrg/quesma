package quesma

import (
	"context"
	"quesma/concurrent"
	"quesma/logger"
	"quesma/quesma/recovery"
	"quesma/tracing"
	"strings"
	"time"
)

const EvictionInterval = 15 * time.Minute
const GCInterval = 1 * time.Minute

func elapsedTime(t time.Time) time.Duration {
	return time.Since(t)
}

type AsyncQueryIdWithTime struct {
	id   string
	time time.Time
}

func (e *AsyncQueriesEvictor) tryEvictAsyncRequests(timeFun func(time.Time) time.Duration) {
	var ids []AsyncQueryIdWithTime
	e.AsyncRequestStorage.Range(func(key string, value AsyncRequestResult) bool {
		if timeFun(value.added) > EvictionInterval {
			ids = append(ids, AsyncQueryIdWithTime{id: key, time: value.added})
		}
		return true
	})
	for _, id := range ids {
		e.AsyncRequestStorage.Delete(id.id)
	}
	var asyncQueriesContexts []*AsyncQueryContext
	e.AsyncQueriesContexts.Range(func(key string, value *AsyncQueryContext) bool {
		if timeFun(value.added) > EvictionInterval {
			if value != nil {
				asyncQueriesContexts = append(asyncQueriesContexts, value)
			}
		}
		return true
	})
	evictedIds := make([]string, 0)
	for _, asyncQueryContext := range asyncQueriesContexts {
		e.AsyncQueriesContexts.Delete(asyncQueryContext.id)
		if asyncQueryContext.cancel != nil {
			evictedIds = append(evictedIds, asyncQueryContext.id)
			asyncQueryContext.cancel()
		}
	}
	if len(evictedIds) > 0 {
		logger.Info().Msgf("Evicted %d async queries : %s", len(evictedIds), strings.Join(evictedIds, ","))
	}
}

type AsyncQueriesEvictor struct {
	ctx                  context.Context
	cancel               context.CancelFunc
	AsyncRequestStorage  *concurrent.Map[string, AsyncRequestResult]
	AsyncQueriesContexts *concurrent.Map[string, *AsyncQueryContext]
}

func NewAsyncQueriesEvictor(AsyncRequestStorage *concurrent.Map[string, AsyncRequestResult], AsyncQueriesContexts *concurrent.Map[string, *AsyncQueryContext]) *AsyncQueriesEvictor {
	ctx, cancel := context.WithCancel(context.Background())
	return &AsyncQueriesEvictor{ctx: ctx, cancel: cancel, AsyncRequestStorage: AsyncRequestStorage, AsyncQueriesContexts: AsyncQueriesContexts}
}

func (e *AsyncQueriesEvictor) asyncQueriesGC() {
	defer recovery.LogPanic()
	for {
		select {
		case <-e.ctx.Done():
			logger.Debug().Msg("evictor stopped")
			return
		case <-time.After(GCInterval):
			e.tryEvictAsyncRequests(elapsedTime)
		}
	}
}

func (e *AsyncQueriesEvictor) Close() {
	e.cancel()
	logger.Info().Msg("AsyncQueriesEvictor Stopped")
}

type AsyncQueryTraceLoggerEvictor struct {
	AsyncQueryTrace *concurrent.Map[string, tracing.TraceCtx]
	ctx             context.Context
	cancel          context.CancelFunc
}

func (e *AsyncQueryTraceLoggerEvictor) Start() {
	e.ctx, e.cancel = context.WithCancel(context.Background())

	go e.FlushHangingAsyncQueryTrace(elapsedTime)
}

func (e *AsyncQueryTraceLoggerEvictor) Stop() {
	e.cancel()
	logger.Info().Msg("AsyncQueryTraceLoggerEvictor Stopped")
}

func (e *AsyncQueryTraceLoggerEvictor) TryFlushHangingAsyncQueryTrace(timeFun func(time.Time) time.Duration) {
	asyncIds := []string{}
	e.AsyncQueryTrace.Range(func(key string, value tracing.TraceCtx) bool {
		if timeFun(value.Added) > EvictionInterval {
			asyncIds = append(asyncIds, key)
			logger.Error().Msgf("Async query %s was not finished", key)
			var formattedLines strings.Builder
			formattedLines.WriteString(tracing.FormatMessages(value.Messages))
			logger.Info().Msg(formattedLines.String())
		}
		return true
	})
	for _, asyncId := range asyncIds {
		e.AsyncQueryTrace.Delete(asyncId)
	}
}

func (e *AsyncQueryTraceLoggerEvictor) FlushHangingAsyncQueryTrace(timeFun func(time.Time) time.Duration) {
	go func() {
		recovery.LogPanic()
		for {
			select {
			case <-time.After(GCInterval):
				e.TryFlushHangingAsyncQueryTrace(timeFun)
			case <-e.ctx.Done():
				logger.Debug().Msg("AsyncQueryTraceLoggerEvictor stopped")
				e.AsyncQueryTrace.Range(func(key string, value tracing.TraceCtx) bool {
					logger.Error().Msgf("Async query %s was not finished", key)
					var formattedLines strings.Builder
					formattedLines.WriteString(tracing.FormatMessages(value.Messages))
					logger.Info().Msg(formattedLines.String())
					return true
				})
				return
			}
		}
	}()
}
