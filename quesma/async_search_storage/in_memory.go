// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package async_search_storage

import (
	"context"
	"github.com/QuesmaOrg/quesma/quesma/logger"
	"github.com/QuesmaOrg/quesma/quesma/recovery"
	"github.com/QuesmaOrg/quesma/quesma/util"
	"strings"
	"time"
)

const EvictionInterval = 15 * time.Minute
const GCInterval = 1 * time.Minute

type AsyncSearchStorageInMemory struct {
	idToResult *util.SyncMap[string, *AsyncRequestResult]
}

func NewAsyncSearchStorageInMemory() AsyncSearchStorageInMemory {
	return AsyncSearchStorageInMemory{
		idToResult: util.NewSyncMap[string, *AsyncRequestResult](),
	}
}

func (s AsyncSearchStorageInMemory) Store(id string, result *AsyncRequestResult) {
	s.idToResult.Store(id, result)
}

func (s AsyncSearchStorageInMemory) Range(f func(key string, value *AsyncRequestResult) bool) {
	s.idToResult.Range(f)
}

func (s AsyncSearchStorageInMemory) Load(id string) (*AsyncRequestResult, bool) {
	return s.idToResult.Load(id)
}

func (s AsyncSearchStorageInMemory) Delete(id string) {
	s.idToResult.Delete(id)
}

func (s AsyncSearchStorageInMemory) Size() int {
	return s.idToResult.Size()
}

type AsyncQueryContextStorageInMemory struct {
	idToContext *util.SyncMap[string, *AsyncQueryContext]
}

func NewAsyncQueryContextStorageInMemory() AsyncQueryContextStorageInMemory {
	return AsyncQueryContextStorageInMemory{
		idToContext: util.NewSyncMap[string, *AsyncQueryContext](),
	}
}

func (s AsyncQueryContextStorageInMemory) Store(id string, context *AsyncQueryContext) {
	s.idToContext.Store(id, context)
}

type AsyncQueriesEvictor struct {
	ctx                  context.Context
	cancel               context.CancelFunc
	AsyncRequestStorage  AsyncSearchStorageInMemory
	AsyncQueriesContexts AsyncQueryContextStorageInMemory
}

func NewAsyncQueriesEvictor(AsyncRequestStorage AsyncSearchStorageInMemory, AsyncQueriesContexts AsyncQueryContextStorageInMemory) *AsyncQueriesEvictor {
	ctx, cancel := context.WithCancel(context.Background())
	return &AsyncQueriesEvictor{ctx: ctx, cancel: cancel, AsyncRequestStorage: AsyncRequestStorage, AsyncQueriesContexts: AsyncQueriesContexts}
}

func elapsedTime(t time.Time) time.Duration {
	return time.Since(t)
}

type asyncQueryIdWithTime struct {
	id   string
	time time.Time
}

func (e *AsyncQueriesEvictor) tryEvictAsyncRequests(timeFun func(time.Time) time.Duration) {
	var ids []asyncQueryIdWithTime
	e.AsyncRequestStorage.Range(func(key string, value *AsyncRequestResult) bool {
		if timeFun(value.added) > EvictionInterval {
			ids = append(ids, asyncQueryIdWithTime{id: key, time: value.added})
		}
		return true
	})
	for _, id := range ids {
		e.AsyncRequestStorage.idToResult.Delete(id.id)
	}
	var asyncQueriesContexts []*AsyncQueryContext
	e.AsyncQueriesContexts.idToContext.Range(func(key string, value *AsyncQueryContext) bool {
		if timeFun(value.added) > EvictionInterval {
			if value != nil {
				asyncQueriesContexts = append(asyncQueriesContexts, value)
			}
		}
		return true
	})
	evictedIds := make([]string, 0)
	for _, asyncQueryContext := range asyncQueriesContexts {
		e.AsyncQueriesContexts.idToContext.Delete(asyncQueryContext.id)
		if asyncQueryContext.cancel != nil {
			evictedIds = append(evictedIds, asyncQueryContext.id)
			asyncQueryContext.cancel()
		}
	}
	if len(evictedIds) > 0 {
		logger.Info().Msgf("Evicted %d async queries : %s", len(evictedIds), strings.Join(evictedIds, ","))
	}
}

func (e *AsyncQueriesEvictor) AsyncQueriesGC() {
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
