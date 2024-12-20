// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package async_search_storage

import (
	"context"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/k0kubun/pp"
	"github.com/stretchr/testify/assert"
	"net/url"
	"quesma/quesma/config"
	"testing"
	"time"
)

func TestAsyncQueriesEvictorTimePassed(t *testing.T) {
	// TODO: add also 3rd storage and nice test for it (remove from memory, but still in elastic)
	//realUrl, _ := url.Parse("http://localhost:9201")
	//cfgUrl := config.Url(*realUrl)
	//cfg := config.ElasticsearchConfiguration{Url: &cfgUrl}
	storageKinds := []AsyncRequestResultStorage{
		NewAsyncRequestResultStorageInMemory(),
		//NewAsyncRequestResultStorageInElasticsearch(cfg),  // passes, reskip after merge
		//NewAsyncSearchStorageInMemoryFallbackElastic(cfg), // passes, reskip after merge
	}
	for _, storage := range storageKinds {
		queryContextStorage := NewAsyncQueryContextStorageInMemory().(AsyncQueryContextStorageInMemory)
		queryContextStorage.idToContext.Store("1", &AsyncQueryContext{})
		evictor := NewAsyncQueriesEvictor(storage, queryContextStorage)
		evictor.AsyncRequestStorage.Store("1", &AsyncRequestResult{Added: time.Now()})
		evictor.AsyncRequestStorage.Store("2", &AsyncRequestResult{Added: time.Now()})
		evictor.AsyncRequestStorage.Store("3", &AsyncRequestResult{Added: time.Now()})

		time.Sleep(2 * time.Second)
		evictor.tryEvictAsyncRequests(1 * time.Second)
		time.Sleep(2 * time.Second)

		assert.Equal(t, 0, evictor.AsyncRequestStorage.DocCount())
	}
}

func TestAsyncQueriesEvictorStillAlive(t *testing.T) {
	// TODO: add also 3rd storage and nice test for it (remove from memory, but still in elastic)
	//realUrl, _ := url.Parse("http://localhost:9201")
	//cfgUrl := config.Url(*realUrl)
	//cfg := config.ElasticsearchConfiguration{Url: &cfgUrl}
	storageKinds := []AsyncRequestResultStorage{
		NewAsyncRequestResultStorageInMemory(),
		//NewAsyncRequestResultStorageInElasticsearch(cfg),  // passes, reskip after merge
		//NewAsyncSearchStorageInMemoryFallbackElastic(cfg), // passes, reskip after merge
	}
	for _, storage := range storageKinds {
		queryContextStorage := NewAsyncQueryContextStorageInMemory().(AsyncQueryContextStorageInMemory)
		queryContextStorage.idToContext.Store("1", &AsyncQueryContext{})
		evictor := NewAsyncQueriesEvictor(storage, queryContextStorage)
		evictor.AsyncRequestStorage.Store("1", &AsyncRequestResult{Added: time.Now()})
		evictor.AsyncRequestStorage.Store("2", &AsyncRequestResult{Added: time.Now()})
		evictor.AsyncRequestStorage.Store("3", &AsyncRequestResult{Added: time.Now()})

		time.Sleep(2 * time.Second)
		evictor.tryEvictAsyncRequests(10 * time.Second)
		time.Sleep(2 * time.Second)

		assert.Equal(t, 3, evictor.AsyncRequestStorage.DocCount())
	}
}

func TestInMemoryFallbackElasticStorage(t *testing.T) {
	//t.Skip("passes locally, but requires elasticsearch to be running, so skipping")
	storage := NewAsyncSearchStorageInMemoryFallbackElastic(testConfig())
	storage.Store("1", &AsyncRequestResult{})
	storage.Store("2", &AsyncRequestResult{})
	storage.Store("3", &AsyncRequestResult{})

	assert.Equal(t, 0, storage.inElasticsearch.DocCount()) // inElasticsearch is async, probably shouldn't be updated yet
	assert.Equal(t, 3, storage.inMemory.DocCount())
	time.Sleep(2 * time.Second)
	assert.Equal(t, 3, storage.inElasticsearch.DocCount())
	assert.Equal(t, 3, storage.DocCount())

	storage.Delete("1")
	storage.Delete("2")
	assert.Equal(t, 1, storage.DocCount())
	assert.Equal(t, 1, storage.inMemory.DocCount())
	assert.Equal(t, 3, storage.inElasticsearch.DocCount()) // inElasticsearch is async, probably shouldn't be updated yet
	time.Sleep(2 * time.Second)
	assert.Equal(t, 1, storage.inElasticsearch.DocCount())
	assert.Equal(t, 1, storage.DocCount())

	// simulate Quesma, and inMemory storage restart
	storage.inMemory = NewAsyncRequestResultStorageInMemory().(AsyncRequestResultStorageInMemory)
	assert.Equal(t, 0, storage.DocCount())
	assert.Equal(t, 1, storage.inElasticsearch.DocCount())

	doc, err := storage.Load("1")
	pp.Println(err, doc)
	assert.Nil(t, doc)
	assert.NotNil(t, err)

	doc, err = storage.Load("3")
	pp.Println(err, doc)
	assert.NotNil(t, doc)
	assert.Nil(t, err)
}

const qid = "abc"

func testConfig() config.ElasticsearchConfiguration {
	realUrl, err := url.Parse("http://localhost:9201")
	if err != nil {
		fmt.Println("ERR", err)
	}
	cfgUrl := config.Url(*realUrl)
	return config.ElasticsearchConfiguration{
		Url:      &cfgUrl,
		User:     "",
		Password: "",
	}
}

func TestEvictingAsyncQuery_1(t *testing.T) {
	t.Skip("TODO: automize this test after evicting from Clickhouse from UI works")
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

func TestEvictingAsyncQuery_2(t *testing.T) {
	t.Skip("TODO: automize this test after evicting from Clickhouse from UI works")
	options := clickhouse.Options{Addr: []string{"localhost:9000"}}
	a := clickhouse.OpenDB(&options)

	b, err := a.Query("KILL QUERY WHERE query_id=	'x'")
	fmt.Println(b, err)
}
