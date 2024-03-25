package quesma

import (
	"github.com/stretchr/testify/assert"
	"mitmproxy/quesma/concurrent"
	"testing"
	"time"
)

func TestAsyncQueriesEvictorTimePassed(t *testing.T) {
	evictor := NewAsyncQueriesEvictor(concurrent.NewMap[string, AsyncRequestResult]())
	evictor.AsyncRequestStorage.Store("1", AsyncRequestResult{added: time.Now()})
	evictor.AsyncRequestStorage.Store("2", AsyncRequestResult{added: time.Now()})
	evictor.AsyncRequestStorage.Store("3", AsyncRequestResult{added: time.Now()})
	evictor.tryEvictAsyncRequests(func(time.Time) time.Duration {
		return 20 * time.Minute
	})

	assert.Equal(t, 0, evictor.AsyncRequestStorage.Size())
}

func TestAsyncQueriesEvictorStillAlive(t *testing.T) {
	evictor := NewAsyncQueriesEvictor(concurrent.NewMap[string, AsyncRequestResult]())
	evictor.AsyncRequestStorage = concurrent.NewMap[string, AsyncRequestResult]()
	evictor.AsyncRequestStorage.Store("1", AsyncRequestResult{added: time.Now()})
	evictor.AsyncRequestStorage.Store("2", AsyncRequestResult{added: time.Now()})
	evictor.AsyncRequestStorage.Store("3", AsyncRequestResult{added: time.Now()})
	evictor.tryEvictAsyncRequests(func(time.Time) time.Duration {
		return time.Second
	})

	assert.Equal(t, 3, evictor.AsyncRequestStorage.Size())
}
