// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package async_search_storage

import (
	"context"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	"quesma/concurrent"
	"testing"
	"time"
)

func TestAsyncQueriesEvictorTimePassed(t *testing.T) {
	queryContextStorage := NewAsyncQueryContextStorageInMemory()
	queryContextStorage.idToContext.Store("1", &AsyncQueryContext{})
	evictor := NewAsyncQueriesEvictor(NewAsyncSearchStorageInMemory(), queryContextStorage)
	evictor.AsyncRequestStorage.Store("1", &AsyncRequestResult{added: time.Now()})
	evictor.AsyncRequestStorage.Store("2", &AsyncRequestResult{added: time.Now()})
	evictor.AsyncRequestStorage.Store("3", &AsyncRequestResult{added: time.Now()})
	evictor.tryEvictAsyncRequests(func(time.Time) time.Duration {
		return 20 * time.Minute
	})

	//assert.Equal(t, 0, evictor.AsyncRequestStorage.Size())
}

func TestAsyncQueriesEvictorStillAlive(t *testing.T) {
	queryContextStorage := NewAsyncQueryContextStorageInMemory()
	queryContextStorage.idToContext.Store("1", &AsyncQueryContext{})
	evictor := NewAsyncQueriesEvictor(NewAsyncSearchStorageInMemory(), queryContextStorage)
	evictor.AsyncRequestStorage.idToResult = concurrent.NewMap[string, *AsyncRequestResult]()
	evictor.AsyncRequestStorage.Store("1", &AsyncRequestResult{added: time.Now()})
	evictor.AsyncRequestStorage.Store("2", &AsyncRequestResult{added: time.Now()})
	evictor.AsyncRequestStorage.Store("3", &AsyncRequestResult{added: time.Now()})
	evictor.tryEvictAsyncRequests(func(time.Time) time.Duration {
		return time.Second
	})

	//assert.Equal(t, 3, evictor.AsyncRequestStorage.Size())
}

const qid = "abc"

func TestKK(t *testing.T) {
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
	options := clickhouse.Options{Addr: []string{"localhost:9000"}}
	a := clickhouse.OpenDB(&options)

	b, err := a.Query("KILL QUERY WHERE query_id=	'dupa'")
	fmt.Println(b, err)
}
