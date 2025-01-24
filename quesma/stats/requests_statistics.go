// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package stats

import (
	"github.com/QuesmaOrg/quesma/quesma/logger"
	"math"
	"sort"
	"sync"
	"time"
)

type (
	RequestStatisticStore struct {
		mutex     sync.Mutex
		createdAt time.Time
		store     map[string]*requestStatistic
	}
	requestStatistic struct {
		// we assume requests time is increasing
		requests []request
		mutex    sync.Mutex
	}
	request struct {
		seen  time.Time
		took  time.Duration
		error bool
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
	// we now rs.requests is sorted by time
	i := 0
	for i < len(rs.requests) && now.Sub(rs.requests[i].seen) > storeLastTime {
		i++
	}

	if i > 0 {
		rs.requests = rs.requests[i:]
	}
}

func (rs *requestStatistic) removeFutureRequests(now time.Time) {
	// assumes that the caller has locked the mutex
	i := len(rs.requests) - 1
	for i >= 0 && now.Sub(rs.requests[i].seen) < 0 {
		i--
	}

	rs.requests = rs.requests[:i+1]
}

func (rs *requestStatistic) recordRequest(took time.Duration, error bool) {
	rs.mutex.Lock()
	defer rs.mutex.Unlock()
	now := time.Now()
	if len(rs.requests) > 0 {
		last := rs.requests[len(rs.requests)-1]
		if now.Compare(last.seen) < 0 {
			// Should not happen. Only when system time was incorrect in future and moved back.
			// Still let's handle it gracefully.
			logger.Warn().Msgf("request time is not increasing, last seen: %v, now: %v, removing future requests",
				last.seen, now)
			rs.removeFutureRequests(now)
		}

		if len(rs.requests) > 0 && len(rs.requests)%cleanupEveryRequest == 0 {
			rs.removeOlderRequests(now)
		}
	}

	rs.requests = append(rs.requests, request{seen: now, took: took, error: error})
}

func (rs *requestStatistic) getRequestsStats(storeCreatedAt time.Time) (result RequestsStats) {
	rs.mutex.Lock()
	now := time.Now()
	rs.removeOlderRequests(now)

	var errorCount uint64
	durations := make([]int64, len(rs.requests))
	for i, r := range rs.requests {
		durations[i] = r.took.Milliseconds()
		if r.error {
			errorCount++
		}
	}
	lenRequests := float64(len(rs.requests))
	rs.mutex.Unlock()

	if createdAgo := now.Sub(storeCreatedAt); createdAgo < time.Minute {
		agoMs := math.Max(1000, float64(createdAgo.Milliseconds()))
		result.RatePerMinute = lenRequests / (agoMs / float64(time.Minute.Milliseconds()))
	} else {
		result.RatePerMinute = lenRequests / float64(storeLastTime.Milliseconds()/time.Minute.Milliseconds())
	}

	if lenRequests > 0 {
		result.ErrorRate = float64(errorCount) / lenRequests
		sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })
		index := int(math.Floor(float64(len(durations)) * 0.99))
		value := durations[index]
		if value < 0 {
			value = 0
		}
		result.Duration99Percentile = uint64(value)
	}

	return result
}

func NewRequestStatisticStore() *RequestStatisticStore {
	return &RequestStatisticStore{
		store:     make(map[string]*requestStatistic),
		createdAt: time.Now(),
	}
}

func newRequestStatisticStoreForTest(created time.Time) *RequestStatisticStore {
	return &RequestStatisticStore{
		store:     make(map[string]*requestStatistic),
		createdAt: created,
	}
}

func (store *RequestStatisticStore) RecordRequest(typeName string, took time.Duration, error bool) {
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
	createdAt := store.createdAt
	store.mutex.Unlock()

	return rs.getRequestsStats(createdAt)
}
