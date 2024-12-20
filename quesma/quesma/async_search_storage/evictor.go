// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package async_search_storage

import (
	"context"
	"quesma/logger"
	"quesma/quesma/recovery"
	"time"
)

type AsyncQueriesEvictor struct {
	ctx                  context.Context
	cancel               context.CancelFunc
	AsyncRequestStorage  AsyncRequestResultStorage
	AsyncQueriesContexts AsyncQueryContextStorage
}

func NewAsyncQueriesEvictor(AsyncRequestStorage AsyncRequestResultStorage, AsyncQueriesContexts AsyncQueryContextStorage) *AsyncQueriesEvictor {
	ctx, cancel := context.WithCancel(context.Background())
	return &AsyncQueriesEvictor{ctx: ctx, cancel: cancel, AsyncRequestStorage: AsyncRequestStorage, AsyncQueriesContexts: AsyncQueriesContexts}
}

func (e *AsyncQueriesEvictor) tryEvictAsyncRequests(olderThan time.Duration) {
	e.AsyncRequestStorage.evict(olderThan)
	e.AsyncQueriesContexts.evict(olderThan)
}

func (e *AsyncQueriesEvictor) AsyncQueriesGC() {
	defer recovery.LogPanic()
	for {
		select {
		case <-e.ctx.Done():
			logger.Debug().Msg("evictor stopped")
			return
		case <-time.After(gcInterval):
			e.tryEvictAsyncRequests(evictionInterval)
		}
	}
}

func (e *AsyncQueriesEvictor) Close() {
	e.cancel()
	logger.Info().Msg("AsyncQueriesEvictor Stopped")
}
