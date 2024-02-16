package stats

import (
	"math"
	"sort"
	"sync"
	"time"
)

type (
	RequestStatisticStore struct {
		mutex sync.Mutex
		store map[string]*requestStatistic
	}
	requestStatistic struct {
		requests []request
		mutex    sync.Mutex
	}
	request struct {
		seen   time.Time
		tookMs uint64
		error  bool
	}
	RequestsStats struct {
		RatePerMinute        float64
		ErrorRate            float64
		Duration99Percentile uint64
	}
)

const (
	// You can tweak it, but it has to be multiple of a minute
	storeLastTime       = time.Minute
	cleanupEveryRequest = 1000
)

func (rs *requestStatistic) removeOlderRequests(now time.Time) {
	// assumes that the caller has locked the mutex
	var newRequests []request
	for _, r := range rs.requests {
		if now.Sub(r.seen) < storeLastTime {
			newRequests = append(newRequests, r)
		}
	}
	rs.requests = newRequests
}

func (rs *requestStatistic) recordRequest(tookMs uint64, error bool) {
	rs.mutex.Lock()
	defer rs.mutex.Unlock()
	now := time.Now()

	if len(rs.requests) > 0 && len(rs.requests)%cleanupEveryRequest == 0 {
		rs.removeOlderRequests(now)
	}

	rs.requests = append(rs.requests, request{seen: now, tookMs: tookMs, error: error})
}

func (rs *requestStatistic) getRequestsStats() (result RequestsStats) {
	rs.mutex.Lock()
	defer rs.mutex.Unlock()
	now := time.Now()
	rs.removeOlderRequests(now)

	var errorCount uint64
	durations := make([]uint64, len(rs.requests))
	for i, r := range rs.requests {
		durations[i] = r.tookMs
		if r.error {
			errorCount++
		}
	}

	result.RatePerMinute = float64(len(rs.requests)) / float64(storeLastTime.Milliseconds()/time.Minute.Milliseconds())
	if len(rs.requests) > 0 {
		result.ErrorRate = float64(errorCount) / float64(len(rs.requests))
		sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })
		index := int(math.Floor(float64(len(durations)) * 0.99))
		result.Duration99Percentile = durations[index]
	}

	return result
}

func NewRequestStatisticStore() *RequestStatisticStore {
	return &RequestStatisticStore{
		store: make(map[string]*requestStatistic),
	}
}

func (store *RequestStatisticStore) RecordRequest(typeName string, took uint64, error bool) {
	store.mutex.Lock()
	var ok bool
	var rs *requestStatistic

	if rs, ok = store.store[typeName]; !ok {
		rs = &requestStatistic{}
		store.store[typeName] = rs
	}
	store.mutex.Unlock()

	rs.recordRequest(took, error)
}

func (store *RequestStatisticStore) GetRequestsStats(typeName string) RequestsStats {
	store.mutex.Lock()
	var ok bool
	var rs *requestStatistic

	if rs, ok = store.store[typeName]; !ok {
		rs = &requestStatistic{}
		store.store[typeName] = rs
	}
	store.mutex.Unlock()

	return rs.getRequestsStats()
}
