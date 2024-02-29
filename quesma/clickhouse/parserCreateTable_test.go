package clickhouse

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseSignozSchema_1(t *testing.T) {
	q := `CREATE TABLE signoz_logs
		(
			"timestamp" UInt64 CODEC(DoubleDelta, LZ4),
			"observed_timestamp" UInt64 CODEC(DoubleDelta, LZ4),
			"id" String CODEC(ZSTD(1)),
			"trace_id" String CODEC(ZSTD(1)),
			"span_id" String CODEC(ZSTD(1)),
			"trace_flags" UInt32,
			"severity_text" LowCardinality(String) CODEC(ZSTD(1)),
			"severity_number" UInt8,
			"body" String CODEC(ZSTD(2)),
			"resources_string_key" Array(String) CODEC(ZSTD(1)),
			"resources_string_value" Array(String) CODEC(ZSTD(1)),
			"attributes_string_key" Array(String) CODEC(ZSTD(1)),
			"attributes_string_value" Array(String) CODEC(ZSTD(1)),
			"attributes_int64_key" Array(String) CODEC(ZSTD(1)),
			"attributes_int64_value" Array(Int64) CODEC(ZSTD(1)),
			"attributes_float64_key" Array(String) CODEC(ZSTD(1)),
			"attributes_float64_value" Array(Float64) CODEC(ZSTD(1)),
			"attributes_bool_key" Array(String) CODEC(ZSTD(1)),
			"attributes_bool_value" Array(Bool) CODEC(ZSTD(1)),
		)`
	fieldNames := []string{"timestamp", "observed_timestamp", "id", "trace_id", "span_id", "trace_flags", "severity_text", "severity_number", "body", "resources_string_key", "resources_string_value", "attributes_string_key", "attributes_string_value", "attributes_int64_key", "attributes_int64_value", "attributes_float64_key", "attributes_float64_value", "attributes_bool_key", "attributes_bool_value"}
	table, err := NewTable(q, nil)
	assert.NoError(t, err)
	assert.Equal(t, len(fieldNames), len(table.Cols))
	for _, fieldName := range fieldNames {
		assert.Contains(t, table.Cols, fieldName)
	}
}

func TestParseSignozSchema_2(t *testing.T) {
	// we test here using both "name" and `name` for column names
	q := `CREATE TABLE IF NOT EXISTS db.signoz_logs ON CLUSTER cluster
		(
			` + "`" + "@timestamp" + "` " + `UInt64 CODEC(DoubleDelta, LZ4),
			"observed_timestamp" UInt64 CODEC(DoubleDelta, LZ4),
			"timestampDT64_1" DateTime64(6, 'UTC') DEFAULT toDateTime64(timestamp, 6, 'UTC') CODEC(DoubleDelta, LZ4),
			"timestampDT64_2" DateTime64(6, 'UTC') DEFAULT now() + toDateTime64(timestamp, 6, 'UTC'),
			"timestampDT64_3" DateTime64(6, 'UTC'),
			"id" String NOT NULL CODEC(ZSTD(1)),
			"trace_id" String DEFAULT "hehe" CODEC(ZSTD(1)),
			"span_id" String NULL CODEC(ZSTD(1)),
			"trace_flags" Uint32 NOT NULL DEFAULT 5,
			"severity_text" LowCardinality(String) CODEC(ZSTD(1)),
			"severity_number" UInt8,
			"body" String CODEC(ZSTD(2)),
			"resources_string_key" Array(String) CODEC(ZSTD(1)),
			"resources_string_value" Array(String) CODEC(ZSTD(1)) TTL 0,
			"attributes_string_key" Array(String) CODEC(ZSTD(1)),
			"attributes_string_value" Array(String) CODEC(ZSTD(1)),
			"attributes_int64_key" Array(String) CODEC(ZSTD(1)),
			"attributes_int64_value" Array(Int64) CODEC(ZSTD(1)) TTL 5555,
			"attributes_float64_key" Array(String) CODEC(ZSTD(1)),
			"attributes_float64_value" Array(Float64) CODEC(ZSTD(1)),
			"attributes_bool_key" Array(String) CODEC(ZSTD(1)) TTL 10 + 50 * 80 + now(),
			"attributes_bool_value" Array(Bool) CODEC(ZSTD(1)),
			"tuple1" Tuple(a String, b String, c Tuple(c String, d Uint128)) CODEC(ZSTD(1)),
		)`
	fieldNames := []string{"@timestamp", "observed_timestamp", "timestampDT64_1", "timestampDT64_2", "timestampDT64_3", "id", "trace_id", "span_id", "trace_flags", "severity_text", "severity_number", "body", "resources_string_key", "resources_string_value", "attributes_string_key", "attributes_string_value", "attributes_int64_key", "attributes_int64_value", "attributes_float64_key", "attributes_float64_value", "attributes_bool_key", "attributes_bool_value", "tuple1"}
	table, err := NewTable(q, nil)
	assert.NoError(t, err)
	assert.Equal(t, len(fieldNames), len(table.Cols))
	for _, fieldName := range fieldNames {
		assert.Contains(t, table.Cols, fieldName)
	}
	assert.Equal(t, "db", table.DatabaseName)
	assert.Equal(t, "cluster", table.Cluster)
}

func TestParseQuotedTablename(t *testing.T) {
	q := `CREATE TABLE IF NOT EXISTS "logs-generic-default"
		(
			"source" String,
			"host.name" String,
			"message" String,
			"service.name" String,
			"severity" String
		)
		ENGINE = MergeTree
		ORDER BY (timestamp)`
	fieldNames := []string{"source", "host.name", "message", "service.name", "severity"}
	table, err := NewTable(q, nil)
	assert.NoError(t, err)
	assert.Equal(t, len(fieldNames), len(table.Cols))
	for _, fieldName := range fieldNames {
		assert.Contains(t, table.Cols, fieldName)
	}
}

func TestParseNonLetterNames(t *testing.T) {
	q := `CREATE TABLE IF NOT EXISTS "/_monitoring/bulk?system_id=kibana&system_api_version=7&interval=10000ms_1"
		(
			"index" Tuple
			(
				"_type" String
			)
		)
		ENGINE = MergeTree
		ORDER BY (timestamp)`
	fieldNames := []string{"index"}
	table, err := NewTable(q, nil)
	assert.NoError(t, err)
	assert.Equal(t, len(fieldNames), len(table.Cols))
	for _, fieldName := range fieldNames {
		assert.Contains(t, table.Cols, fieldName)
	}
}

func TestParseLongNestedSchema(t *testing.T) {
	q := `CREATE TABLE IF NOT EXISTS "/_monitoring/bulk?system_id=kibana&system_api_version=7&interval=10000ms_2"
		(
			"processes" String,
			"os" Tuple
			(
				"uptime_in_millis" String,
				"distro" String,
				"cpuacct" Tuple
				(
					"control_group" String,
					"usage_nanos" String
				),
				"distroRelease" String,
				"cpu" Tuple
				(
					"control_group" String,
					"stat" Tuple
					(
						"number_of_elapsed_periods" String,
						"number_of_times_throttled" String,
						"time_throttled_nanos" String
					),
					"cfs_quota_micros" String,
					"cfs_period_micros" String
				),
				"platform" String,
				"platformRelease" String,
				"load" Tuple
				(
					"1m" String,
					"5m" String,
					"15m" String
				),
				"memory" Tuple
				(
					"total_in_bytes" String,
					"free_in_bytes" String,
					"used_in_bytes" String
				)
			),
			"concurrent_connections" String,
			"requests" Tuple
			(
				"disconnects" String,
				"total" String
			),
			"kibana" Tuple
			(
				"name" String,
				"index" String,
				"host" String,
				"transport_address" String,
				"version" String,
				"snapshot" Bool,
				"status" String,
				"uuid" String
			),
			"elasticsearch_client" Tuple
			(
				"totalActiveSockets" String,
				"totalIdleSockets" String,
				"totalQueuedRequests" String
			),
			"response_times" Tuple
			(
				"average" String,
				"max" String
			),
			"process" Tuple
			(
				"event_loop_delay" String,
				"event_loop_delay_histogram" Tuple
				(
					"mean" String,
					"exceeds" String,
					"stddev" String,
					"fromTimestamp" DateTime64,
					"lastUpdatedAt" DateTime64,
					"percentiles" Tuple
					(
						"99" String,
						"50" String,
						"75" String,
						"95" String
					),
					"min" String,
					"max" String
				),
				"event_loop_utilization" Tuple
				(
					"active" String,
					"idle" String,
					"utilization" String
				),
				"uptime_in_millis" String,
				"memory" Tuple
				(
					"heap" Tuple
					(
						"total_in_bytes" String,
						"used_in_bytes" String,
						"size_limit" String
					),
					"resident_set_size_in_bytes" String
				)
			),
			"timestamp" DateTime64 DEFAULT now64()
		)
		ENGINE = MergeTree
		ORDER BY (timestamp)`
	fieldNames := []string{"processes", "os", "concurrent_connections", "requests", "kibana", "elasticsearch_client", "response_times", "process", "timestamp"}
	table, err := NewTable(q, nil)
	assert.NoError(t, err)
	assert.Equal(t, len(fieldNames), len(table.Cols))
	for _, fieldName := range fieldNames {
		assert.Contains(t, table.Cols, fieldName)
	}
	assert.Equal(t, 3, len(table.Cols["elasticsearch_client"].Type.(MultiValueType).Cols))
	assert.Equal(t, 5, len(table.Cols["process"].Type.(MultiValueType).Cols))
}
