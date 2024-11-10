// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package async_search_storage

import (
	"context"
	"quesma/persistence"
	"quesma/quesma/config"
)

type AsyncSearchStorageInElastic struct {
	db *persistence.ElasticDatabaseWithEviction
}

func NewAsyncSearchStorageInElastic() AsyncSearchStorageInElastic {
	return AsyncSearchStorageInElastic{
		db: persistence.NewElasticDatabaseWithEviction(
			config.ElasticsearchConfiguration{}, "async_search", 1_000_000_000),
	}
}

func (s AsyncSearchStorageInElastic) Store(ctx context.Context, id string, result *AsyncRequestResult) {
	s.db.Put(ctx, nil)
}

func (s AsyncSearchStorageInElastic) Load(ctx context.Context, id string) (*AsyncRequestResult, bool) {
	_, ok := s.db.Get(ctx, id)
	return nil, ok
}

func (s AsyncSearchStorageInElastic) Delete(id string) {
	s.db.Delete(id)
}

func (s AsyncSearchStorageInElastic) DocCount() int {
	cnt, ok := s.db.DocCount()
	if !ok {
		return -1
	}
	return cnt
}

func (s AsyncSearchStorageInElastic) SizeInBytes() int64 {
	size, ok := s.db.SizeInBytes()
	if !ok {
		return -1
	}
	return size
}

type AsyncQueryContextStorageInElastic struct {
	db *persistence.ElasticDatabaseWithEviction
}

func NewAsyncQueryContextStorageInElastic() AsyncQueryContextStorageInElastic {
	return AsyncQueryContextStorageInElastic{
		db: persistence.NewElasticDatabaseWithEviction(
			config.ElasticsearchConfiguration{}, "async_search", 1_000_000_000),
	}
}

func (s AsyncQueryContextStorageInElastic) Store(ctx context.Context, id string, context *AsyncQueryContext) {
	s.db.Put(ctx, nil)
}
