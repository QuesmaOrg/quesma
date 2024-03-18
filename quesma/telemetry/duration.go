package telemetry

import (
	"context"
	"sync"
	"time"
)

// public API

type Span interface {
	End(err error)
}

type DurationMeasurement interface {
	Begin() Span
	Aggregate() DurationStats
}

type DurationStats struct {
	Count   int64   `json:"count"`
	Avg     float64 `json:"avg_time_sec"`
	Over1s  int64   `json:"over_1sec"`
	Over10s int64   `json:"over_10sec"`
	Failed  int64   `json:"failed"`
	// FIXME add percentiles here 50%, 75%, 95%
	//
}

// implementation

type durationSample struct {
	elapsed float64
	ok      bool
}

type durationMeasurement struct {
	ctx     context.Context
	m       sync.Mutex
	count   int64
	over1s  int64
	over10s int64
	sum     float64
	failed  int64

	ingest chan durationSample
}

func newDurationMeasurement(ctx context.Context) *durationMeasurement {
	tm := &durationMeasurement{ctx: ctx}
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
		if sample.elapsed > 1.0 {
			a.over1s++
		}
		if sample.elapsed > 10.0 {
			a.over10s++
		}
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
	stats.Over1s = a.over1s
	stats.Over10s = a.over10s

	return stats
}

type span struct {
	measurement *durationMeasurement
	startedAt   time.Time
}

func (a *durationMeasurement) Begin() Span {
	return &span{measurement: a, startedAt: time.Now()}
}

func (s *span) End(err error) {

	sample := durationSample{elapsed: time.Since(s.startedAt).Seconds(), ok: err == nil}
	s.measurement.ingest <- sample

}
