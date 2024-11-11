// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package async_search_storage

import (
	"context"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
	"quesma/logger"
	"testing"
	"time"
)

func TestAsyncQueriesEvictorTimePassed(t *testing.T) {
	// TODO: add also 3rd storage and nice test for it (remove from memory, but still in elastic)
	logger.InitSimpleLoggerForTests()
	for _, storage := range []AsyncRequestResultStorage{NewAsyncSearchStorageInMemory(), NewAsyncSearchStorageInElastic()} {
		queryContextStorage := NewAsyncQueryContextStorageInMemory()
		queryContextStorage.idToContext.Store("1", &AsyncQueryContext{})
		evictor := NewAsyncQueriesEvictor(storage, queryContextStorage)
		evictor.AsyncRequestStorage.Store("1", &AsyncRequestResult{added: time.Now()})
		evictor.AsyncRequestStorage.Store("2", &AsyncRequestResult{added: time.Now()})
		evictor.AsyncRequestStorage.Store("3", &AsyncRequestResult{added: time.Now()})
		evictor.tryEvictAsyncRequests(func(time.Time) time.Duration {
			return 20 * time.Minute
		})

		if _, ok := storage.(*AsyncSearchStorageInElastic); ok {
			time.Sleep(1 * time.Second)
		}
		assert.Equal(t, 0, evictor.AsyncRequestStorage.DocCount())
	}
}

func TestAsyncQueriesEvictorStillAlive(t *testing.T) {
	logger.InitSimpleLoggerForTests()
	for _, storage := range []AsyncRequestResultStorage{NewAsyncSearchStorageInMemory(), NewAsyncSearchStorageInElastic()} {
		t.Run(fmt.Sprintf("storage: %T", storage), func(t *testing.T) {
			queryContextStorage := NewAsyncQueryContextStorageInMemory()
			queryContextStorage.idToContext.Store("1", &AsyncQueryContext{})
			evictor := NewAsyncQueriesEvictor(storage, queryContextStorage)
			evictor.AsyncRequestStorage.Store("1", &AsyncRequestResult{added: time.Now()})
			evictor.AsyncRequestStorage.Store("2", &AsyncRequestResult{added: time.Now()})
			evictor.AsyncRequestStorage.Store("3", &AsyncRequestResult{added: time.Now()})
			evictor.tryEvictAsyncRequests(func(time.Time) time.Duration {
				return time.Second
			})

			assert.Equal(t, 3, evictor.AsyncRequestStorage.DocCount())
		})
	}
}

const qid = "abc"

func TestKK(t *testing.T) {
	t.Skip()
	options := clickhouse.Options{Addr: []string{"localhost:9000"}}
	a := clickhouse.OpenDB(&options)
	ctx := clickhouse.Context(context.Background(), clickhouse.WithQueryID(qid))

	b, err := a.QueryContext(ctx, "SELECT number FROM (SELECT number FROM numbers(100_000_000_000)) ORDER BY number DESC LIMIT 10")
	var q int64
	for b.Next() {
		b.Scan(&q)
		fmt.Println(q)
	}

	fmt.Println(b, "q:", q, err)
}

func TestCancel(t *testing.T) {
	t.Skip()
	options := clickhouse.Options{Addr: []string{"localhost:9000"}}
	a := clickhouse.OpenDB(&options)

	b, err := a.Query("KILL QUERY WHERE query_id=	'dupa'")
	fmt.Println(b, err)
}
