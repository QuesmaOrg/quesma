// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package diag

import "time"

type MultiCounterStats map[string]int64

type MultiCounterTopValuesStats []string

type MultiCounter interface {
	Add(key string, value int64)
	AggregateAndReset() MultiCounterStats
	AggregateTopValuesAndReset() MultiCounterTopValuesStats
}

type Span interface {
	End(err error) time.Duration
}

type DurationStats struct {
	Count          int64              `json:"count"`
	Avg            float64            `json:"avg_time_sec"`
	Failed         int64              `json:"failed"`
	OverThresholds map[string]int64   `json:"over_thresholds"`
	Percentiles    map[string]float32 `json:"percentiles"`
}

type DurationMeasurement interface {
	Begin() Span
	AggregateAndReset() DurationStats
}

type PhoneHomeClient interface {
	ClickHouseQueryDuration() DurationMeasurement
	ClickHouseInsertDuration() DurationMeasurement
	ElasticReadRequestsDuration() DurationMeasurement
	ElasticWriteRequestsDuration() DurationMeasurement

	ElasticBypassedReadRequestsDuration() DurationMeasurement
	ElasticBypassedWriteRequestsDuration() DurationMeasurement

	IngestCounters() MultiCounter
	UserAgentCounters() MultiCounter
	FailedRequestsCollector(f func() int64)
}

type ClickHouseStats struct {
	Status            string `json:"status"`
	NumberOfRows      int64  `json:"number_of_rows" db:"number_of_rows"`
	DiskSpace         int64  `json:"disk_space"`
	OpenConnection    int    `json:"open_connection"`
	MaxOpenConnection int    `json:"max_open_connection"`
	ServerVersion     string `json:"server_version"`
	DbInfoHash        string `json:"db_info_hash"`
	BillableSize      int64  `json:"billable_size"`
	TopTablesSizeInfo string `json:"top_tables_size_info"`
}

type ElasticStats struct {
	Status        string `json:"status"`
	NumberOfDocs  int64  `json:"number_of_docs"`
	Size          int64  `json:"size"`
	ServerVersion string `json:"server_version"`
	HealthStatus  string `json:"health_status"`
}

type RuntimeStats struct {
	MemoryUsed         uint64 `json:"memory_used"`
	MemoryAvailable    uint64 `json:"memory_available"`
	NumberOfGoroutines int    `json:"number_of_goroutines"`
	NumberOfCPUs       int    `json:"number_of_cpus"`
	NumberOfGC         uint32 `json:"number_of_gc"`
}

type PhoneHomeStats struct {
	AgentStartedAt int64  `json:"started_at"`
	Hostname       string `json:"hostname"`
	QuesmaVersion  string `json:"quesma_version"`
	BuildHash      string `json:"build_hash"`
	BuildDate      string `json:"build_date"`
	InstanceID     string `json:"instanceId"`

	// add more diag here about running

	ClickHouse    ClickHouseStats `json:"clickhouse"`
	Elasticsearch ElasticStats    `json:"elasticsearch"`

	ClickHouseQueriesDuration DurationStats `json:"clickhouse_queries"`
	ClickHouseInsertsDuration DurationStats `json:"clickhouse_inserts"`
	ElasticReadsDuration      DurationStats `json:"elastic_read_requests"`
	ElasticWritesDuration     DurationStats `json:"elastic_write_requests"`

	ElasticBypassedReadsDuration  DurationStats `json:"elastic_bypassed_read_requests"`
	ElasticBypassedWritesDuration DurationStats `json:"elastic_bypassed_write_requests"`

	// Due to schema issues, we are not using this for now
	IngestCounters    MultiCounterStats          `json:"-"`
	UserAgentCounters MultiCounterTopValuesStats `json:"top_user_agents"`

	RuntimeStats           RuntimeStats `json:"runtime"`
	NumberOfPanics         int64        `json:"number_of_panics"`
	TopErrors              []string     `json:"top_errors"`
	NumberOfFailedRequests int64        `json:"number_of_failed_requests"`

	ReportType string `json:"report_type"`
	TakenAt    int64  `json:"taken_at"`
	ConfigMode string `json:"config_mode"`
}

type PhoneHomeRecentStatsProvider interface {
	RecentStats() (recent PhoneHomeStats, available bool)
}
