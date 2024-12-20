// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package async_search_storage

import (
	"encoding/json"
	"fmt"
	"quesma/logger"
	"quesma/persistence"
	"quesma/quesma/config"
	"time"
)

type AsyncRequestResultStorageInElasticsearch struct {
	db *persistence.ElasticDatabaseWithEviction
}

func NewAsyncRequestResultStorageInElasticsearch(cfg config.ElasticsearchConfiguration) AsyncRequestResultStorage {
	/* some test config, maybe you'll find it easier to debug with it
	realUrl, err := url.Parse("http://localhost:9201")
	if err != nil {
		fmt.Println("ERR", err)
	}
	cfgUrl := config.Url(*realUrl)
	cfg := config.ElasticsearchConfiguration{
		Url:      &cfgUrl,
		User:     "",
		Password: "",
	}
		fmt.Println("kk dbg NewAsyncRequestResultStorageInElasticsearch() i:", cfg)
		return AsyncRequestResultStorageInElasticsearch{
			db: persistence.NewElasticDatabaseWithEviction(cfg, "quesma_async_storage-"+strconv.Itoa(i), 1_000_000_000),
		}
	*/
	return AsyncRequestResultStorageInElasticsearch{
		db: persistence.NewElasticDatabaseWithEviction(cfg, defaultElasticDbName, defaultElasticDbStorageLimitInBytes),
	}
}

func (s AsyncRequestResultStorageInElasticsearch) Store(id string, result *AsyncRequestResult) {
	err := s.db.Put(result.toJSON(id))
	if err != nil {
		logger.Warn().Err(err).Msg("failed to store document")
	}
}

func (s AsyncRequestResultStorageInElasticsearch) Load(id string) (*AsyncRequestResult, error) {
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

func (s AsyncRequestResultStorageInElasticsearch) Delete(id string) {
	err := s.db.Delete(id)
	if err != nil {
		logger.Warn().Err(err).Msg("failed to delete document")
	}
}

func (s AsyncRequestResultStorageInElasticsearch) DeleteOld(t time.Duration) {
	err := s.db.DeleteOld(t)
	if err != nil {
		logger.Warn().Err(err).Msg("failed to delete old documents")
	}
}

// DocCount returns the number of documents in the database, or -1 if the count could not be retrieved.
func (s AsyncRequestResultStorageInElasticsearch) DocCount() int {
	cnt, err := s.db.DocCount()
	if err != nil {
		logger.Warn().Err(err).Msg("failed to get document count")
		return -1
	}
	return cnt
}

// StorageSizeInBytes returns the total size of all documents in the database, or -1 if the size could not be retrieved.
func (s AsyncRequestResultStorageInElasticsearch) SpaceInUse() int64 {
	size, err := s.db.SizeInBytes()
	if err != nil {
		logger.Warn().Err(err).Msg("failed to get storage size")
		return -1
	}
	return size
}

func (s AsyncRequestResultStorageInElasticsearch) SpaceMaxAvailable() int64 {
	return s.db.SizeInBytesLimit()
}

func (s AsyncRequestResultStorageInElasticsearch) evict(evictOlderThan time.Duration) {
	err := s.db.DeleteOld(evictOlderThan)
	if err != nil {
		logger.Warn().Err(err).Msgf("failed to evict documents, err: %v", err)
	}
}

type AsyncQueryContextStorageInElasticsearch struct {
	db *persistence.ElasticDatabaseWithEviction
}

func NewAsyncQueryContextStorageInElasticsearch(cfg config.ElasticsearchConfiguration) AsyncQueryContextStorage {
	fmt.Println("kk dbg NewAsyncQueryContextStorageInElasticsearch() i:", cfg)
	return AsyncQueryContextStorageInElasticsearch{
		db: persistence.NewElasticDatabaseWithEviction(cfg, "async_search", 1_000_000_000),
	}
}

func (s AsyncQueryContextStorageInElasticsearch) Store(context *AsyncQueryContext) {
	err := s.db.Put(context.toJSON())
	if err != nil {
		logger.Warn().Err(err).Msg("failed to store document")
	}
}

func (s AsyncQueryContextStorageInElasticsearch) evict(evictOlderThan time.Duration) {
	err := s.db.DeleteOld(evictOlderThan)
	if err != nil {
		logger.Warn().Err(err).Msg("failed to delete old documents")
	}
}
