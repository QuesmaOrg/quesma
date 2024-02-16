package stats

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestRequestsStatistics_typical(t *testing.T) {
	store := NewRequestStatisticStore()
	store.RecordRequest("test", 100, false)
	store.RecordRequest("test", 200, false)
	store.RecordRequest("test", 300, false)
	store.RecordRequest("test", 400, true)
	store.RecordRequest("test", 500, true)

	stats := store.GetRequestsStats("test")
	assert.Equal(t, float64(5), stats.RatePerMinute)
	assert.Equal(t, 0.4, stats.ErrorRate)
	assert.Equal(t, uint64(500), stats.Duration99Percentile)

	const count = 1000
	for i := 0; i < count; i++ {
		store.RecordRequest("test b", uint64(1000-i-1), false)
	}
	stats2 := store.GetRequestsStats("test b")
	assert.Equal(t, float64(count), stats2.RatePerMinute)
	assert.Equal(t, 0.0, stats2.ErrorRate)
	assert.Equal(t, uint64(990), stats2.Duration99Percentile)
}

func TestRequestsStatistics_empty(t *testing.T) {
	store := NewRequestStatisticStore()
	stats := store.GetRequestsStats("test")
	assert.Equal(t, float64(0), stats.RatePerMinute)
	assert.Equal(t, 0.0, stats.ErrorRate)
	assert.Equal(t, uint64(0), stats.Duration99Percentile)
}
