// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package async_search_storage

import (
	"fmt"
	"github.com/QuesmaOrg/quesma/quesma/logger"
	"github.com/QuesmaOrg/quesma/quesma/util"
	"math"
	"strings"
	"time"
)

type AsyncRequestResultStorageInMemory struct {
	idToResult *util.SyncMap[string, *AsyncRequestResult]
}

func NewAsyncRequestResultStorageInMemory() AsyncRequestResultStorage { // change result type to AsyncRequestResultStorage interface
	return AsyncRequestResultStorageInMemory{
		idToResult: util.NewSyncMap[string, *AsyncRequestResult](),
	}
}

func (s AsyncRequestResultStorageInMemory) Store(id string, result *AsyncRequestResult) {
	s.idToResult.Store(id, result)
}

func (s AsyncRequestResultStorageInMemory) Range(f func(key string, value *AsyncRequestResult) bool) {
	s.idToResult.Range(f)
}

func (s AsyncRequestResultStorageInMemory) Load(id string) (*AsyncRequestResult, error) {
	if val, ok := s.idToResult.Load(id); ok {
		return val, nil
	}
	return nil, fmt.Errorf("key %s not found", id)
}

func (s AsyncRequestResultStorageInMemory) Delete(id string) {
	s.idToResult.Delete(id)
}

func (s AsyncRequestResultStorageInMemory) DocCount() int {
	return s.idToResult.Size()
}

// in bytes
func (s AsyncRequestResultStorageInMemory) SpaceInUse() int64 {
	size := int64(0)
	s.Range(func(key string, value *AsyncRequestResult) bool {
		size += int64(len(value.ResponseBody))
		return true
	})
	return size
}

func (s AsyncRequestResultStorageInMemory) SpaceMaxAvailable() int64 {
	return math.MaxInt64 / 16 // some huge number for now, can be changed if we want to limit in-memory storage
}

func (s AsyncRequestResultStorageInMemory) evict(evictOlderThan time.Duration) {
	var ids []string
	s.Range(func(key string, value *AsyncRequestResult) bool {
		if time.Since(value.Added) > evictOlderThan {
			ids = append(ids, key)
		}
		return true
	})
	for _, id := range ids {
		s.Delete(id)
	}
}

type AsyncQueryContextStorageInMemory struct {
	idToContext *util.SyncMap[string, *AsyncQueryContext]
}

func NewAsyncQueryContextStorageInMemory() AsyncQueryContextStorage {
	return AsyncQueryContextStorageInMemory{
		idToContext: util.NewSyncMap[string, *AsyncQueryContext](),
	}
}

func (s AsyncQueryContextStorageInMemory) Store(context *AsyncQueryContext) {
	s.idToContext.Store(context.id, context)
}

func (s AsyncQueryContextStorageInMemory) evict(evictOlderThan time.Duration) {
	var asyncQueriesContexts []*AsyncQueryContext
	s.idToContext.Range(func(key string, value *AsyncQueryContext) bool {
		if time.Since(value.added) > evictOlderThan {
			if value != nil {
				asyncQueriesContexts = append(asyncQueriesContexts, value)
			}
		}
		return true
	})
	evictedIds := make([]string, 0)
	for _, asyncQueryContext := range asyncQueriesContexts {
		s.idToContext.Delete(asyncQueryContext.id)
		if asyncQueryContext.cancel != nil {
			evictedIds = append(evictedIds, asyncQueryContext.id)
			asyncQueryContext.cancel()
		}
	}
	if len(evictedIds) > 0 {
		logger.Info().Msgf("Evicted %d async queries : %s", len(evictedIds), strings.Join(evictedIds, ","))
	}
}
