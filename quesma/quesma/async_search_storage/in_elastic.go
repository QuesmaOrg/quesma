// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package async_search_storage

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net/url"
	"quesma/logger"
	"quesma/persistence"
	"quesma/quesma/config"
	"strconv"
	"time"
)

type AsyncSearchStorageInElastic struct {
	db *persistence.ElasticDatabaseWithEviction
}

func NewAsyncSearchStorageInElastic() AsyncSearchStorageInElastic {
	// TODO use passed config
	realUrl, err := url.Parse("http://localhost:9200")
	if err != nil {
		fmt.Println("ERR", err)
	}
	cfgUrl := config.Url(*realUrl)
	cfg := config.ElasticsearchConfiguration{
		Url:      &cfgUrl,
		User:     "",
		Password: "",
	}
	i := rand.Int()
	return AsyncSearchStorageInElastic{
		db: persistence.NewElasticDatabaseWithEviction(cfg, "quesma_async_storage-"+strconv.Itoa(i), 1_000_000_000),
	}
}

func (s AsyncSearchStorageInElastic) Store(id string, result *AsyncRequestResult) {
	err := s.db.Put(result.toJSON(id))
	if err != nil {
		logger.Warn().Err(err).Msg("failed to store document")
	}
}

func (s AsyncSearchStorageInElastic) Load(id string) (*AsyncRequestResult, error) {
	resultAsBytes, err := s.db.Get(id)
	if err != nil {
		return nil, err
	}

	result := AsyncRequestResult{}
	err = json.Unmarshal(resultAsBytes, &result)
	if err != nil {
		return nil, err
	}

	return &result, nil
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
func (s AsyncSearchStorageInElastic) SpaceInUse() int64 {
	size, err := s.db.SizeInBytes()
	if err != nil {
		logger.Warn().Err(err).Msg("failed to get storage size")
		return -1
	}
	return size
}

func (s AsyncSearchStorageInElastic) SpaceMaxAvailable() int64 {
	return s.db.SizeInBytesLimit()
}

func (s AsyncSearchStorageInElastic) evict(evictOlderThan time.Duration) {
	err := s.db.DeleteOld(evictOlderThan)
	if err != nil {
		logger.Warn().Err(err).Msgf("failed to evict documents, err: %v", err)
	}
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
