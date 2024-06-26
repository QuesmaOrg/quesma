// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package stats

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func ms(d int) time.Duration {
	return time.Duration(d) * time.Millisecond
}

func TestRequestsStatistics_typical(t *testing.T) {
	store := newRequestStatisticStoreForTest(time.Now().AddDate(0, 0, -1))
	store.RecordRequest("test", ms(100), false)
	store.RecordRequest("test", ms(200), false)
	store.RecordRequest("test", ms(300), false)
	store.RecordRequest("test", ms(400), true)
	store.RecordRequest("test", ms(500), true)

	stats := store.GetRequestsStats("test")
	assert.Equal(t, float64(5), stats.RatePerMinute)
	assert.Equal(t, 0.4, stats.ErrorRate)
	assert.Equal(t, uint64(500), stats.Duration99Percentile)

	const count = 1000
	for i := 0; i < count; i++ {
		store.RecordRequest("test b", ms(1000-i-1), false)
	}
	stats2 := store.GetRequestsStats("test b")
	assert.Equal(t, float64(count), stats2.RatePerMinute)
	assert.Equal(t, 0.0, stats2.ErrorRate)
	assert.Equal(t, uint64(990), stats2.Duration99Percentile)
}

func TestRequestsStatistics_empty(t *testing.T) {
	t.Run("empty store", func(t *testing.T) {
		store := newRequestStatisticStoreForTest(time.Now().AddDate(0, 0, -1))
		stats := store.GetRequestsStats("test")
		assert.Equal(t, float64(0), stats.RatePerMinute)
		assert.Equal(t, 0.0, stats.ErrorRate)
		assert.Equal(t, uint64(0), stats.Duration99Percentile)
	})
}
