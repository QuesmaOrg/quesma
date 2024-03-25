package quesma

import (
	"context"
	"mitmproxy/quesma/concurrent"
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/quesma/recovery"
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
}

type AsyncQueriesEvictor struct {
	ctx                 context.Context
	cancel              context.CancelFunc
	AsyncRequestStorage *concurrent.Map[string, AsyncRequestResult]
}

func NewAsyncQueriesEvictor(AsyncRequestStorage *concurrent.Map[string, AsyncRequestResult]) *AsyncQueriesEvictor {
	ctx, cancel := context.WithCancel(context.Background())
	return &AsyncQueriesEvictor{ctx: ctx, cancel: cancel, AsyncRequestStorage: AsyncRequestStorage}
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
