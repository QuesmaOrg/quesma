// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package async_search_storage

import (
	"github.com/QuesmaOrg/quesma/quesma/quesma/config"
	"time"
)

type AsyncRequestResultStorageInMemoryFallbackElastic struct {
	inMemory        AsyncRequestResultStorageInMemory
	inElasticsearch AsyncRequestResultStorageInElasticsearch
}

func NewAsyncSearchStorageInMemoryFallbackElastic(cfg config.ElasticsearchConfiguration) AsyncRequestResultStorageInMemoryFallbackElastic {
	return AsyncRequestResultStorageInMemoryFallbackElastic{
		inMemory:        NewAsyncRequestResultStorageInMemory().(AsyncRequestResultStorageInMemory),
		inElasticsearch: NewAsyncRequestResultStorageInElasticsearch(cfg).(AsyncRequestResultStorageInElasticsearch),
	}
}

func (s AsyncRequestResultStorageInMemoryFallbackElastic) Store(id string, result *AsyncRequestResult) {
	s.inMemory.Store(id, result)
	s.inElasticsearch.Store(id, result)
}

func (s AsyncRequestResultStorageInMemoryFallbackElastic) Load(id string) (*AsyncRequestResult, error) {
	result, err := s.inMemory.Load(id)
	if err == nil {
		return result, nil
	}
	return s.inElasticsearch.Load(id)
}

func (s AsyncRequestResultStorageInMemoryFallbackElastic) Delete(id string) {
	s.inMemory.Delete(id)
	s.inElasticsearch.Delete(id)
}

// DocCount returns inMemory doc count
func (s AsyncRequestResultStorageInMemoryFallbackElastic) DocCount() int {
	return s.inMemory.DocCount()
}

// SpaceInUse returns inMemory size in bytes
func (s AsyncRequestResultStorageInMemoryFallbackElastic) SpaceInUse() int64 {
	return s.inMemory.SpaceInUse()
}

// SpaceMaxAvailable returns inMemory size in bytes limit
func (s AsyncRequestResultStorageInMemoryFallbackElastic) SpaceMaxAvailable() int64 {
	return s.inMemory.SpaceMaxAvailable()
}

func (s AsyncRequestResultStorageInMemoryFallbackElastic) evict(olderThan time.Duration) {
	s.inMemory.evict(olderThan)
	s.inElasticsearch.DeleteOld(olderThan)
}

type AsyncQueryContextStorageInMemoryFallbackElasticsearch struct {
	inMemory        AsyncQueryContextStorageInMemory
	inElasticsearch AsyncQueryContextStorageInElasticsearch
}

func NewAsyncQueryContextStorageInMemoryFallbackElasticsearch(cfg config.ElasticsearchConfiguration) AsyncQueryContextStorage {
	return AsyncQueryContextStorageInMemoryFallbackElasticsearch{
		inMemory:        NewAsyncQueryContextStorageInMemory().(AsyncQueryContextStorageInMemory),
		inElasticsearch: NewAsyncQueryContextStorageInElasticsearch(cfg).(AsyncQueryContextStorageInElasticsearch),
	}
}

func (s AsyncQueryContextStorageInMemoryFallbackElasticsearch) Store(context *AsyncQueryContext) {
	s.inMemory.Store(context)
	s.inElasticsearch.Store(context)
}

func (s AsyncQueryContextStorageInMemoryFallbackElasticsearch) evict(evictOlderThan time.Duration) {
	s.inMemory.evict(evictOlderThan)
	s.inElasticsearch.evict(evictOlderThan)
}
