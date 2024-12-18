// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package telemetry

import (
	"context"
	"quesma_v2/core/diag"
	"slices"
	"strconv"
	"sync"
	"time"
)

var overThresholds = []int{1, 5, 10, 30, 60}

const percentileSamplePoolSize = 10000

var percentiles = []int{5, 25, 50, 75, 95}

// public API

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

	// for tests only
	ingestDoneCh chan interface{}
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

	if a.ingestDoneCh != nil {
		a.ingestDoneCh <- struct{}{}
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

func (a *durationMeasurement) AggregateAndReset() (stats diag.DurationStats) {
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

	a.count = 0
	a.sum = 0
	a.failed = 0
	for _, threshold := range overThresholds {
		a.overThresholdCounters[threshold] = 0
	}

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

func (a *durationMeasurement) Begin() diag.Span {
	return &span{measurement: a, startedAt: time.Now()}
}

func (s *span) End(err error) time.Duration {
	duration := time.Since(s.startedAt)
	sample := durationSample{elapsed: duration.Seconds(), ok: err == nil}
	s.measurement.ingest <- sample
	return duration
}
