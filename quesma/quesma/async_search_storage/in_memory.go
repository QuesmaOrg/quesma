// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package async_search_storage

import (
	"context"
	"fmt"
	"math"
	"quesma/logger"
	"quesma/quesma/recovery"
	"quesma/tracing"
	"quesma/util"
	"strings"
	"time"
)

type AsyncRequestResultStorageInMemory struct {
	idToResult *util.SyncMap[string, *AsyncRequestResult]
}

func NewAsyncRequestResultStorageInMemory() AsyncRequestResultStorage { // change result type to AsyncRequestResultStorage interface
	return AsyncRequestResultStorageInMemory{
		idToResult: util.NewSyncMap[string, *AsyncRequestResult](),
	}
}

func (s AsyncRequestResultStorageInMemory) Store(id string, result *AsyncRequestResult) {
	s.idToResult.Store(id, result)
}

func (s AsyncRequestResultStorageInMemory) Range(f func(key string, value *AsyncRequestResult) bool) {
	s.idToResult.Range(f)
}

func (s AsyncRequestResultStorageInMemory) Load(id string) (*AsyncRequestResult, error) {
	if val, ok := s.idToResult.Load(id); ok {
		return val, nil
	}
	return nil, fmt.Errorf("key %s not found", id)
}

func (s AsyncRequestResultStorageInMemory) Delete(id string) {
	s.idToResult.Delete(id)
}

func (s AsyncRequestResultStorageInMemory) DocCount() int {
	return s.idToResult.Size()
}

// in bytes
func (s AsyncRequestResultStorageInMemory) SpaceInUse() int64 {
	size := int64(0)
	s.Range(func(key string, value *AsyncRequestResult) bool {
		size += int64(len(value.ResponseBody))
		return true
	})
	return size
}

func (s AsyncRequestResultStorageInMemory) SpaceMaxAvailable() int64 {
	return math.MaxInt64 / 16 // some huge number for now, can be changed if we want to limit in-memory storage
}

func (s AsyncRequestResultStorageInMemory) evict(evictOlderThan time.Duration) {
	var ids []string
	s.Range(func(key string, value *AsyncRequestResult) bool {
		if time.Since(value.Added) > evictOlderThan {
			ids = append(ids, key)
		}
		return true
	})
	for _, id := range ids {
		s.Delete(id)
	}
}

type AsyncQueryContextStorageInMemory struct {
	idToContext *util.SyncMap[string, *AsyncQueryContext]
}

func NewAsyncQueryContextStorageInMemory() AsyncQueryContextStorage {
	return AsyncQueryContextStorageInMemory{
		idToContext: util.NewSyncMap[string, *AsyncQueryContext](),
	}
}

func (s AsyncQueryContextStorageInMemory) Store(context *AsyncQueryContext) {
	s.idToContext.Store(context.id, context)
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
	AsyncQueryTrace *util.SyncMap[string, tracing.TraceCtx]
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
		if timeFun(value.Added) > evictionInterval {
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
		defer recovery.LogPanic()
		for {
			select {
			case <-time.After(gcInterval):
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
