package async_search_storage

import "time"

type AsyncSearchStorageInMemoryFallbackElastic struct {
	inMemory AsyncSearchStorageInMemory
	elastic  AsyncSearchStorageInElastic
}

func NewAsyncSearchStorageInMemoryFallbackElastic() AsyncSearchStorageInMemoryFallbackElastic {
	return AsyncSearchStorageInMemoryFallbackElastic{
		inMemory: NewAsyncSearchStorageInMemory(),
		elastic:  NewAsyncSearchStorageInElastic(),
	}
}

func (s AsyncSearchStorageInMemoryFallbackElastic) Store(id string, result *AsyncRequestResult) {
	s.inMemory.Store(id, result)
	go s.elastic.Store(id, result)
}

func (s AsyncSearchStorageInMemoryFallbackElastic) Load(id string) (*AsyncRequestResult, error) {
	result, err := s.inMemory.Load(id)
	if err == nil {
		return result, nil
	}
	return s.elastic.Load(id)
}

func (s AsyncSearchStorageInMemoryFallbackElastic) Delete(id string) {
	s.inMemory.Delete(id)
	go s.elastic.Delete(id)
}

// DocCount returns inMemory doc count
func (s AsyncSearchStorageInMemoryFallbackElastic) DocCount() int {
	return s.inMemory.DocCount()
}

// SizeInBytes returns inMemory size in bytes
func (s AsyncSearchStorageInMemoryFallbackElastic) SpaceInUse() int64 {
	return s.inMemory.SpaceInUse()
}

// SizeInBytesLimit returns inMemory size in bytes limit
func (s AsyncSearchStorageInMemoryFallbackElastic) SpaceMaxAvailable() int64 {
	return s.inMemory.SpaceMaxAvailable()
}

func (s AsyncSearchStorageInMemoryFallbackElastic) evict(timeFun func(time.Time) time.Duration) {
	s.inMemory.evict(timeFun)
	go s.elastic.DeleteOld(timeFun(time.Now()))
}
