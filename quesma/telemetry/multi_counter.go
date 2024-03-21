package telemetry

import (
	"context"
	"slices"
	"sync"
)

type MultiCounterStats map[string]int64

type MultiCounterTopValuesStats []string

type MultiCounter interface {
	Add(key string, value int64)
	Aggregate() MultiCounterStats
	AggregateTopValues() MultiCounterTopValuesStats
}

type sampleMultiCounter struct {
	key   string
	value int64
}

type multiCounter struct {
	m          sync.Mutex
	ctx        context.Context
	counters   map[string]int64
	ingest     chan sampleMultiCounter
	processKey func(string) string
	// this channel is used in tests only
	ingestDoneCh chan interface{}
}

func NewMultiCounter(ctx context.Context, processKeyFn func(string) string) MultiCounter {
	mc := &multiCounter{
		ctx:        ctx,
		counters:   make(map[string]int64),
		processKey: processKeyFn,
	}
	mc.ingest = make(chan sampleMultiCounter, 100)
	go mc.ingressLoop()
	return mc
}

func (mc *multiCounter) ingress(key string, value int64) {
	mc.m.Lock()
	defer mc.m.Unlock()
	if mc.processKey != nil {
		key = mc.processKey(key)
	}
	mc.counters[key] += value
	if mc.ingestDoneCh != nil {
		mc.ingestDoneCh <- struct{}{}
	}
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

func (mc *multiCounter) AggregateTopValues() (stats MultiCounterTopValuesStats) {
	mc.m.Lock()
	defer mc.m.Unlock()
	stats = make(MultiCounterTopValuesStats, 0, len(mc.counters))

	type userAgentHit struct {
		userAgent string
		counter   int
	}

	var userAgents []userAgentHit
	for k, v := range mc.counters {
		userAgents = append(userAgents, userAgentHit{k, int(v)})
	}

	slices.SortFunc(userAgents, func(i, j userAgentHit) int {
		return i.counter - j.counter
	})

	if len(userAgents) > 10 {
		userAgents = userAgents[:10]
	}

	for _, ua := range userAgents {
		stats = append(stats, ua.userAgent)
	}

	// let's conserve some memory
	mc.counters = make(map[string]int64)

	return stats
}
