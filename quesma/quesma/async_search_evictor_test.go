package quesma

import (
	"github.com/stretchr/testify/assert"
	"mitmproxy/quesma/concurrent"
	"testing"
	"time"
)

func TestAsyncQueriesEvictorTimePassed(t *testing.T) {
	AsyncRequestStorage = concurrent.NewMap[string, AsyncRequestResult]()
	AsyncRequestStorage.Store("1", AsyncRequestResult{added: time.Now()})
	AsyncRequestStorage.Store("2", AsyncRequestResult{added: time.Now()})
	AsyncRequestStorage.Store("3", AsyncRequestResult{added: time.Now()})
	tryEvictAsyncRequests(func(time.Time) time.Duration {
		return 20 * time.Minute
	})

	assert.Equal(t, 0, AsyncRequestStorage.Size())
}

func TestAsyncQueriesEvictorStillAlive(t *testing.T) {
	AsyncRequestStorage = concurrent.NewMap[string, AsyncRequestResult]()
	AsyncRequestStorage.Store("1", AsyncRequestResult{added: time.Now()})
	AsyncRequestStorage.Store("2", AsyncRequestResult{added: time.Now()})
	AsyncRequestStorage.Store("3", AsyncRequestResult{added: time.Now()})
	tryEvictAsyncRequests(func(time.Time) time.Duration {
		return time.Second
	})

	assert.Equal(t, 3, AsyncRequestStorage.Size())
}
