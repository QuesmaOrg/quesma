// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package diag

import (
	"time"
)
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

func (d emptyTimer) AggregateAndReset() DurationStats {
	return DurationStats{}
}

type emptyMultiCounter struct{}

func (d emptyMultiCounter) Add(key string, value int64) {
	// do nothing
}

func (d emptyMultiCounter) AggregateAndReset() MultiCounterStats {
	return MultiCounterStats{}
}

func (d emptyMultiCounter) AggregateTopValuesAndReset() MultiCounterTopValuesStats {
	return MultiCounterTopValuesStats{}
}

func NoopPhoneHomeAgent() PhoneHomeClient {
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

func NewPhoneHomeEmptyAgent() PhoneHomeClient {
	return &emptyAgent{}
}

type emptyPhoneHomeRecentStatsProvider struct {
}

func (d emptyPhoneHomeRecentStatsProvider) RecentStats() (PhoneHomeStats, bool) {
	return PhoneHomeStats{}, true
}

func EmptyPhoneHomeRecentStatsProvider() PhoneHomeRecentStatsProvider {
	return &emptyPhoneHomeRecentStatsProvider{}
}

type emptyDebugInfoCollector struct {
}

func EmptyDebugInfoCollector() DebugInfoCollector {
	return &emptyDebugInfoCollector{}
}

func (e *emptyDebugInfoCollector) PushPrimaryInfo(qdebugInfo *QueryDebugPrimarySource) {
}

func (e *emptyDebugInfoCollector) PushSecondaryInfo(qdebugInfo *QueryDebugSecondarySource) {
}

func (e *emptyDebugInfoCollector) RecordRequest(typeName string, took time.Duration, error bool) {
}
