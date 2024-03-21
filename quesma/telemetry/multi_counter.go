package telemetry

import (
	"context"
	"sync"
)

type MultiCounterStats map[string]int64

type MultiCounter interface {
	Add(key string, value int64)
	Aggregate() MultiCounterStats
}

type sampleMultiCounter struct {
	key   string
	value int64
}

type multiCounter struct {
	m        sync.Mutex
	ctx      context.Context
	counters map[string]int64
	ingest   chan sampleMultiCounter
}

func NewMultiCounter(ctx context.Context) MultiCounter {
	mc := &multiCounter{
		ctx:      ctx,
		counters: make(map[string]int64),
	}
	mc.ingest = make(chan sampleMultiCounter, 100)
	go mc.ingressLoop()
	return mc
}

func (mc *multiCounter) ingress(key string, value int64) {
	mc.m.Lock()
	defer mc.m.Unlock()
	mc.counters[key] += value
}

func (mc *multiCounter) ingressLoop() {
	for {
		select {
		case <-mc.ctx.Done():
			return
		case sample := <-mc.ingest:
			mc.ingress(sample.key, sample.value)
		}
	}
}

func (mc *multiCounter) Add(key string, value int64) {
	mc.ingest <- sampleMultiCounter{key, value}
}

func (mc *multiCounter) Aggregate() (stats MultiCounterStats) {
	mc.m.Lock()
	defer mc.m.Unlock()
	stats = make(map[string]int64, len(mc.counters))
	for k, v := range mc.counters {
		stats[k] = v
	}
	return stats
}
