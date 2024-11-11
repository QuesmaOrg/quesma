// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package async_search_storage

import (
	"context"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/k0kubun/pp"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestAsyncQueriesEvictorTimePassed(t *testing.T) {
	// TODO: add also 3rd storage and nice test for it (remove from memory, but still in elastic)
	storageKinds := []AsyncRequestResultStorage{
		NewAsyncSearchStorageInMemory(),
		NewAsyncSearchStorageInElastic(),
		NewAsyncSearchStorageInMemoryFallbackElastic(),
	}
	for _, storage := range storageKinds {
		queryContextStorage := NewAsyncQueryContextStorageInMemory()
		queryContextStorage.idToContext.Store("1", &AsyncQueryContext{})
		evictor := NewAsyncQueriesEvictor(storage, queryContextStorage)
		evictor.AsyncRequestStorage.Store("1", &AsyncRequestResult{added: time.Now()})
		evictor.AsyncRequestStorage.Store("2", &AsyncRequestResult{added: time.Now()})
		evictor.AsyncRequestStorage.Store("3", &AsyncRequestResult{added: time.Now()})

		time.Sleep(2 * time.Second)
		evictor.tryEvictAsyncRequests(1 * time.Second)
		time.Sleep(2 * time.Second)

		assert.Equal(t, 0, evictor.AsyncRequestStorage.DocCount())
	}
}

func TestAsyncQueriesEvictorStillAlive(t *testing.T) {
	// TODO: add also 3rd storage and nice test for it (remove from memory, but still in elastic)
	storageKinds := []AsyncRequestResultStorage{
		NewAsyncSearchStorageInMemory(),
		NewAsyncSearchStorageInElastic(),
		NewAsyncSearchStorageInMemoryFallbackElastic(),
	}
	for _, storage := range storageKinds {
		queryContextStorage := NewAsyncQueryContextStorageInMemory()
		queryContextStorage.idToContext.Store("1", &AsyncQueryContext{})
		evictor := NewAsyncQueriesEvictor(storage, queryContextStorage)
		evictor.AsyncRequestStorage.Store("1", &AsyncRequestResult{added: time.Now()})
		evictor.AsyncRequestStorage.Store("2", &AsyncRequestResult{added: time.Now()})
		evictor.AsyncRequestStorage.Store("3", &AsyncRequestResult{added: time.Now()})

		time.Sleep(2 * time.Second)
		evictor.tryEvictAsyncRequests(10 * time.Second)
		time.Sleep(2 * time.Second)

		assert.Equal(t, 3, evictor.AsyncRequestStorage.DocCount())
	}
}

func TestInMemoryFallbackElasticStorage(t *testing.T) {
	storage := NewAsyncSearchStorageInMemoryFallbackElastic()
	storage.Store("1", &AsyncRequestResult{})
	storage.Store("2", &AsyncRequestResult{})
	storage.Store("3", &AsyncRequestResult{})

	assert.Equal(t, 0, storage.elastic.DocCount()) // elastic is async, probably shouldn't be updated yet
	assert.Equal(t, 3, storage.inMemory.DocCount())
	time.Sleep(2 * time.Second)
	assert.Equal(t, 3, storage.elastic.DocCount())
	assert.Equal(t, 3, storage.DocCount())

	storage.Delete("1")
	storage.Delete("2")
	assert.Equal(t, 1, storage.DocCount())
	assert.Equal(t, 1, storage.inMemory.DocCount())
	assert.Equal(t, 3, storage.elastic.DocCount()) // elastic is async, probably shouldn't be updated yet
	time.Sleep(2 * time.Second)
	assert.Equal(t, 1, storage.elastic.DocCount())
	assert.Equal(t, 1, storage.DocCount())

	// simulate Quesma, and inMemory storage restart
	storage.inMemory = NewAsyncSearchStorageInMemory()
	assert.Equal(t, 0, storage.DocCount())
	assert.Equal(t, 1, storage.elastic.DocCount())

	doc, err := storage.Load("1")
	pp.Println(err, doc)
	assert.Nil(t, doc)
	assert.NotNil(t, err)
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
