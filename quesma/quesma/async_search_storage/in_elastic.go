// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package async_search_storage

import (
	"quesma/logger"
	"quesma/persistence"
	"quesma/quesma/config"
	"time"
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

func (s AsyncSearchStorageInElastic) Store(id string, result *AsyncRequestResult) {
	err := s.db.Put(result.toJSON(id))
	if err != nil {
		logger.Warn().Err(err).Msg("failed to store document")
	}
}

func (s AsyncSearchStorageInElastic) Load(id string) (*AsyncRequestResult, error) {
	_, err := s.db.Get(id)
	return nil, err
}

func (s AsyncSearchStorageInElastic) Delete(id string) {
	err := s.db.Delete(id)
	if err != nil {
		logger.Warn().Err(err).Msg("failed to delete document")
	}
}

func (s AsyncSearchStorageInElastic) DeleteOld(t time.Duration) {
	err := s.db.DeleteOld(t)
	if err != nil {
		logger.Warn().Err(err).Msg("failed to delete old documents")
	}
}

// DocCount returns the number of documents in the database, or -1 if the count could not be retrieved.
func (s AsyncSearchStorageInElastic) DocCount() int {
	cnt, err := s.db.DocCount()
	if err != nil {
		logger.Warn().Err(err).Msg("failed to get document count")
		return -1
	}
	return cnt
}

// StorageSizeInBytes returns the total size of all documents in the database, or -1 if the size could not be retrieved.
func (s AsyncSearchStorageInElastic) StorageSizeInBytes() int64 {
	size, err := s.db.SizeInBytes()
	if err != nil {
		logger.Warn().Err(err).Msg("failed to get storage size")
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
