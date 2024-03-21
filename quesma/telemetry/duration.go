package telemetry

import (
	"context"
	"slices"
	"strconv"
	"sync"
	"time"
)

var overThresholds = []int{1, 5, 10, 30, 60}

const percentileSamplePoolSize = 10000

var percentiles = []int{5, 25, 50, 75, 95}

// public API

type Span interface {
	End(err error) time.Duration
}

type DurationMeasurement interface {
	Begin() Span
	Aggregate() DurationStats
}

type DurationStats struct {
	Count          int64              `json:"count"`
	Avg            float64            `json:"avg_time_sec"`
	Failed         int64              `json:"failed"`
	OverThresholds map[string]int64   `json:"over_thresholds"`
	Percentiles    map[string]float32 `json:"percentiles"`
}

// implementation

type durationSample struct {
	elapsed float64
	ok      bool
}

type durationMeasurement struct {
	ctx context.Context
	m   sync.Mutex

	count                 int64
	sum                   float64
	failed                int64
	overThresholdCounters map[int]int64
	samplePoolSize        [percentileSamplePoolSize]float32

	ingest chan durationSample
}

func newDurationMeasurement(ctx context.Context) *durationMeasurement {

	overThresholdCounters := make(map[int]int64, len(overThresholds))
	for _, threshold := range overThresholds {
		overThresholdCounters[threshold] = 0
	}

	tm := &durationMeasurement{ctx: ctx, overThresholdCounters: overThresholdCounters}

	tm.ingest = make(chan durationSample, 100)
	go tm.ingestLoop()
	return tm
}

func (a *durationMeasurement) ingestSample(sample durationSample) {

	a.m.Lock()
	defer a.m.Unlock()

	if sample.ok {
		a.count++
		a.sum += sample.elapsed

		for _, threshold := range overThresholds {
			if sample.elapsed > float64(threshold) {
				a.overThresholdCounters[threshold]++
			}
		}

		a.samplePoolSize[(a.count-1)%percentileSamplePoolSize] = float32(sample.elapsed)

	} else {
		a.failed++
	}

}

func (a *durationMeasurement) ingestLoop() {

	for {
		select {
		case <-a.ctx.Done():
			return
		case sample := <-a.ingest:
			a.ingestSample(sample)
		}
	}

}

func (a *durationMeasurement) Aggregate() (stats DurationStats) {
	a.m.Lock()
	defer a.m.Unlock()

	stats.Count = a.count
	if a.count == 0 {
		stats.Avg = 0
	} else {
		stats.Avg = a.sum / float64(a.count)
	}
	stats.Failed = a.failed

	stats.OverThresholds = a.computeOverThresholdCounter()
	stats.Percentiles = a.computePercentiles()

	return stats
}

func (a *durationMeasurement) computeOverThresholdCounter() (stats map[string]int64) {

	stats = make(map[string]int64)

	for _, threshold := range overThresholds {
		stats[strconv.Itoa(threshold)] = a.overThresholdCounters[threshold]
	}

	return stats
}

func (a *durationMeasurement) computePercentiles() (stats map[string]float32) {

	stats = make(map[string]float32)
	for _, p := range percentiles {
		stats[strconv.Itoa(p)] = 0
	}

	// no samples no percentiles
	if a.count == 0 {
		return stats
	}

	// percentiles are calculated from the sample pool
	realLen := min(percentileSamplePoolSize, int(a.count))

	sorted := make([]float32, realLen)
	copy(sorted, a.samplePoolSize[0:realLen])
	slices.Sort(sorted)

	percentile := func(p int) float32 {
		// this is naive
		idx := (p * realLen) / 100
		return sorted[idx]
	}

	for _, p := range percentiles {
		stats[strconv.Itoa(p)] = percentile(p)
	}

	return stats
}

type span struct {
	measurement *durationMeasurement
	startedAt   time.Time
}

func (a *durationMeasurement) Begin() Span {
	return &span{measurement: a, startedAt: time.Now()}
}

func (s *span) End(err error) time.Duration {
	duration := time.Since(s.startedAt)
	sample := durationSample{elapsed: duration.Seconds(), ok: err == nil}
	s.measurement.ingest <- sample
	return duration
}
