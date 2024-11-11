// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package async_search_storage

import (
	"context"
	"fmt"
	"math"
	"quesma/concurrent"
	"quesma/logger"
	"quesma/quesma/recovery"
	"quesma/tracing"
	"strings"
	"time"
)

type AsyncSearchStorageInMemory struct {
	idToResult *concurrent.Map[string, *AsyncRequestResult]
}

func NewAsyncSearchStorageInMemory() AsyncSearchStorageInMemory { // change result type to AsyncRequestResultStorage interface
	return AsyncSearchStorageInMemory{
		idToResult: concurrent.NewMap[string, *AsyncRequestResult](),
	}
}

func (s AsyncSearchStorageInMemory) Store(id string, result *AsyncRequestResult) {
	s.idToResult.Store(id, result)
}

func (s AsyncSearchStorageInMemory) Range(f func(key string, value *AsyncRequestResult) bool) {
	s.idToResult.Range(f)
}

func (s AsyncSearchStorageInMemory) Load(id string) (*AsyncRequestResult, error) {
	if val, ok := s.idToResult.Load(id); ok {
		return val, nil
	}
	return nil, fmt.Errorf("key %s not found", id)
}

func (s AsyncSearchStorageInMemory) Delete(id string) {
	s.idToResult.Delete(id)
}

func (s AsyncSearchStorageInMemory) DocCount() int {
	return s.idToResult.Size()
}

// in bytes
func (s AsyncSearchStorageInMemory) SpaceInUse() int64 {
	size := int64(0)
	s.Range(func(key string, value *AsyncRequestResult) bool {
		size += int64(len(value.GetResponseBody()))
		return true
	})
	return size
}

func (s AsyncSearchStorageInMemory) SpaceMaxAvailable() int64 {
	return math.MaxInt64 / 16 // some huge number for now, can be changed if we want to limit in-memory storage
}

func (s AsyncSearchStorageInMemory) evict(evictOlderThan time.Duration) {
	var ids []string
	s.Range(func(key string, value *AsyncRequestResult) bool {
		if time.Since(value.added) > evictOlderThan {
			ids = append(ids, key)
		}
		return true
	})
	for _, id := range ids {
		s.Delete(id)
	}
}

type AsyncQueryContextStorageInMemory struct {
	idToContext *concurrent.Map[string, *AsyncQueryContext]
}

func NewAsyncQueryContextStorageInMemory() AsyncQueryContextStorageInMemory {
	return AsyncQueryContextStorageInMemory{
		idToContext: concurrent.NewMap[string, *AsyncQueryContext](),
	}
}

func (s AsyncQueryContextStorageInMemory) Store(id string, context *AsyncQueryContext) {
	s.idToContext.Store(id, context)
}

func (s AsyncQueryContextStorageInMemory) evict(evictOlderThan time.Duration) {
	var asyncQueriesContexts []*AsyncQueryContext
	s.idToContext.Range(func(key string, value *AsyncQueryContext) bool {
		if time.Since(value.added) > evictOlderThan {
			if value != nil {
				asyncQueriesContexts = append(asyncQueriesContexts, value)
			}
		}
		return true
	})
	evictedIds := make([]string, 0)
	for _, asyncQueryContext := range asyncQueriesContexts {
		s.idToContext.Delete(asyncQueryContext.id)
		if asyncQueryContext.cancel != nil {
			evictedIds = append(evictedIds, asyncQueryContext.id)
			asyncQueryContext.cancel()
		}
	}
	if len(evictedIds) > 0 {
		logger.Info().Msgf("Evicted %d async queries : %s", len(evictedIds), strings.Join(evictedIds, ","))
	}
}

func elapsedTime(t time.Time) time.Duration {
	return time.Since(t)
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
