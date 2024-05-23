package telemetry

import "time"
import "context"

type emptyTimer struct {
}

type emptySpan struct {
}

func (d emptySpan) End(err error) time.Duration {
	return 0
}

func (d emptyTimer) Begin() Span {
	return emptySpan{}
}

func (d emptyTimer) Aggregate() DurationStats {
	return DurationStats{}
}

type emptyMultiCounter struct{}

func (d emptyMultiCounter) Add(key string, value int64) {
	// do nothing
}

func (d emptyMultiCounter) Aggregate() MultiCounterStats {
	return MultiCounterStats{}
}

func (d emptyMultiCounter) AggregateTopValues() MultiCounterTopValuesStats {
	return MultiCounterTopValuesStats{}
}

func NoopPhoneHomeAgent() PhoneHomeAgent {
	return &emptyAgent{}
}

type emptyAgent struct {
}

func (d emptyAgent) Start() {
	// do nothing
}

func (d emptyAgent) Stop(ctx context.Context) {
	// do nothing
}

func (d emptyAgent) RecentStats() (recent PhoneHomeStats, available bool) {
	return PhoneHomeStats{}, false
}

func (d emptyAgent) ClickHouseQueryDuration() DurationMeasurement {
	return &emptyTimer{}
}

func (d emptyAgent) ClickHouseInsertDuration() DurationMeasurement {
	return &emptyTimer{}
}

func (d emptyAgent) ElasticReadRequestsDuration() DurationMeasurement {
	return &emptyTimer{}
}

func (d emptyAgent) ElasticWriteRequestsDuration() DurationMeasurement {
	return &emptyTimer{}
}

func (d emptyAgent) ElasticBypassedReadRequestsDuration() DurationMeasurement {
	return &emptyTimer{}
}

func (d emptyAgent) ElasticBypassedWriteRequestsDuration() DurationMeasurement {
	return &emptyTimer{}
}

func (d emptyAgent) IngestCounters() MultiCounter {
	return &emptyMultiCounter{}
}

func (d emptyAgent) UserAgentCounters() MultiCounter {
	return &emptyMultiCounter{}
}

func (d emptyAgent) FailedRequestsCollector(func() int64) {

}

func NewPhoneHomeEmptyAgent() PhoneHomeAgent {
	return &emptyAgent{}
}
