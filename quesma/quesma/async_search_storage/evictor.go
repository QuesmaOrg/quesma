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
	AsyncRequestStorage  AsyncSearchStorageInMemory
	AsyncQueriesContexts AsyncQueryContextStorageInMemory
}

func NewAsyncQueriesEvictor(AsyncRequestStorage AsyncSearchStorageInMemory, AsyncQueriesContexts AsyncQueryContextStorageInMemory) *AsyncQueriesEvictor {
	ctx, cancel := context.WithCancel(context.Background())
	return &AsyncQueriesEvictor{ctx: ctx, cancel: cancel, AsyncRequestStorage: AsyncRequestStorage, AsyncQueriesContexts: AsyncQueriesContexts}
}

func (e *AsyncQueriesEvictor) tryEvictAsyncRequests(timeFun func(time.Time) time.Duration) {
	e.AsyncRequestStorage.evict(timeFun)
	e.AsyncQueriesContexts.evict(timeFun)
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
