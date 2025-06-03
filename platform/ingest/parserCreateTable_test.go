// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package ingest

import (
	"github.com/QuesmaOrg/quesma/platform/clickhouse"
	"github.com/QuesmaOrg/quesma/platform/util"
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
	table, err := clickhouse.NewTable(q, nil)
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
	table, err := clickhouse.NewTable(q, nil)
	assert.NoError(t, err)
	assert.Equal(t, len(fieldNames), len(table.Cols))
	for _, fieldName := range fieldNames {
		assert.Contains(t, table.Cols, fieldName)
	}
	assert.Equal(t, "db", table.DatabaseName)
	assert.Equal(t, "cluster", table.ClusterName)
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
	table, err := clickhouse.NewTable(q, nil)
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
	table, err := clickhouse.NewTable(q, nil)
	assert.NoError(t, err)
	assert.Equal(t, len(fieldNames), len(table.Cols))
	for _, fieldName := range fieldNames {
		assert.Contains(t, table.Cols, fieldName)
	}
}

func TestParseCreateSampleDataEcommerce(t *testing.T) {
	q := `CREATE TABLE IF NOT EXISTS "kibana_sample_data_ecommerce" ON CLUSTER "quesma_cluster" 
(
	"@timestamp" DateTime64(3) DEFAULT now64(),
	"attributes_values" Map(String,String),
	"attributes_metadata" Map(String,String),


	"taxful_total_price" Nullable(Float64) COMMENT  'quesmaMetadataV1:fieldName=taxful_total_price',
	"sku" Array(String) COMMENT  'quesmaMetadataV1:fieldName=sku',
	"taxless_total_price" Nullable(Float64) COMMENT  'quesmaMetadataV1:fieldName=taxless_total_price',
	"total_unique_products" Nullable(Int64) COMMENT  'quesmaMetadataV1:fieldName=total_unique_products',
	"geoip_region_name" Nullable(String) COMMENT  'quesmaMetadataV1:fieldName=geoip.region_name',
	"category" Array(String) COMMENT  'quesmaMetadataV1:fieldName=category',
	"products_created_on" Array(DateTime64) COMMENT  'quesmaMetadataV1:fieldName=products.created_on',
	"products_taxful_price" Array(Float64) COMMENT  'quesmaMetadataV1:fieldName=products.taxful_price',
	"user" Nullable(String) COMMENT  'quesmaMetadataV1:fieldName=user',
	"currency" Nullable(String) COMMENT  'quesmaMetadataV1:fieldName=currency',
	"day_of_week" Nullable(String) COMMENT  'quesmaMetadataV1:fieldName=day_of_week',
	"geoip_location_lat" Nullable(String) COMMENT  'quesmaMetadataV1:fieldName=geoip.location.lat',
	"geoip_city_name" Nullable(String) COMMENT  'quesmaMetadataV1:fieldName=geoip.city_name',
	"products__id" Array(String) COMMENT  'quesmaMetadataV1:fieldName=products._id',
	"products_discount_amount" Array(Int64) COMMENT  'quesmaMetadataV1:fieldName=products.discount_amount',
	"customer_last_name" Nullable(String) COMMENT  'quesmaMetadataV1:fieldName=customer_last_name',
	"total_quantity" Nullable(Int64) COMMENT  'quesmaMetadataV1:fieldName=total_quantity',
	"geoip_continent_name" Nullable(String) COMMENT  'quesmaMetadataV1:fieldName=geoip.continent_name',
	"products_price" Array(Float64) COMMENT  'quesmaMetadataV1:fieldName=products.price',
	"products_sku" Array(String) COMMENT  'quesmaMetadataV1:fieldName=products.sku',
	"products_discount_percentage" Array(Int64) COMMENT  'quesmaMetadataV1:fieldName=products.discount_percentage',
	"type" Nullable(String) COMMENT  'quesmaMetadataV1:fieldName=type',
	"customer_full_name" Nullable(String) COMMENT  'quesmaMetadataV1:fieldName=customer_full_name',
	"products_min_price" Array(Float64) COMMENT  'quesmaMetadataV1:fieldName=products.min_price',
	"products_unit_discount_amount" Array(Int64) COMMENT  'quesmaMetadataV1:fieldName=products.unit_discount_amount',
	"products_manufacturer" Array(String) COMMENT  'quesmaMetadataV1:fieldName=products.manufacturer',
	"products_base_unit_price" Array(Float64) COMMENT  'quesmaMetadataV1:fieldName=products.base_unit_price',
	"day_of_week_i" Nullable(Int64) COMMENT  'quesmaMetadataV1:fieldName=day_of_week_i',
	"manufacturer" Array(String) COMMENT  'quesmaMetadataV1:fieldName=manufacturer',
	"customer_first_name" Nullable(String) COMMENT  'quesmaMetadataV1:fieldName=customer_first_name',
	"products_product_id" Array(Int64) COMMENT  'quesmaMetadataV1:fieldName=products.product_id',
	"customer_gender" Nullable(String) COMMENT  'quesmaMetadataV1:fieldName=customer_gender',
	"email" Nullable(String) COMMENT  'quesmaMetadataV1:fieldName=email',
	"order_id" Nullable(String) COMMENT  'quesmaMetadataV1:fieldName=order_id',
	"customer_phone" Nullable(String) COMMENT  'quesmaMetadataV1:fieldName=customer_phone',
	"order_date" Nullable(DateTime64) COMMENT  'quesmaMetadataV1:fieldName=order_date',
	"geoip_location_lon" Nullable(String) COMMENT  'quesmaMetadataV1:fieldName=geoip.location.lon',
	"products_base_price" Array(Float64) COMMENT  'quesmaMetadataV1:fieldName=products.base_price',
	"products_category" Array(String) COMMENT  'quesmaMetadataV1:fieldName=products.category',
	"products_tax_amount" Array(Int64) COMMENT  'quesmaMetadataV1:fieldName=products.tax_amount',
	"products_product_name" Array(String) COMMENT  'quesmaMetadataV1:fieldName=products.product_name',
	"customer_id" Nullable(String) COMMENT  'quesmaMetadataV1:fieldName=customer_id',
	"event_dataset" Nullable(String) COMMENT  'quesmaMetadataV1:fieldName=event.dataset',
	"geoip_country_iso_code" Nullable(String) COMMENT  'quesmaMetadataV1:fieldName=geoip.country_iso_code',
	"products_taxless_price" Array(Float64) COMMENT  'quesmaMetadataV1:fieldName=products.taxless_price',
	"products_quantity" Array(Int64) COMMENT  'quesmaMetadataV1:fieldName=products.quantity',
	"customer_birth_date" Nullable(DateTime64) COMMENT  'quesmaMetadataV1:fieldName=customer_birth_date',

)
ENGINE = MergeTree
ORDER BY ("@timestamp")

COMMENT 'created by Quesma'`

	fieldNames := []string{"@timestamp", "attributes_values", "attributes_metadata", "taxful_total_price", "sku", "taxless_total_price", "total_unique_products", "geoip_region_name", "category", "products_created_on", "products_taxful_price", "user", "currency", "day_of_week", "geoip_location_lat", "geoip_city_name", "products__id", "products_discount_amount", "customer_last_name", "total_quantity", "geoip_continent_name", "products_price", "products_sku", "products_discount_percentage", "type", "customer_full_name", "products_min_price", "products_unit_discount_amount", "products_manufacturer", "products_base_unit_price", "day_of_week_i", "manufacturer", "customer_first_name", "products_product_id", "customer_gender", "email", "order_id", "customer_phone", "order_date", "geoip_location_lon", "products_base_price", "products_category", "products_tax_amount", "products_product_name", "customer_id", "event_dataset", "geoip_country_iso_code", "products_taxless_price", "products_quantity", "customer_birth_date"}
	table, err := clickhouse.NewTable(q, nil)
	assert.NoError(t, err)
	assert.Equal(t, len(fieldNames), len(table.Cols))
	for _, fieldName := range fieldNames {
		assert.Contains(t, table.Cols, fieldName)
	}
	assert.Equal(t, "kibana_sample_data_ecommerce", table.Name)
	assert.Equal(t, "quesma_cluster", table.ClusterName)
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
	table, err := clickhouse.NewTable(q, nil)
	assert.NoError(t, err)
	assert.Equal(t, len(fieldNames), len(table.Cols))
	for _, fieldName := range fieldNames {
		assert.Contains(t, table.Cols, fieldName)
	}
	assert.Equal(t, 3, len(table.Cols["elasticsearch_client"].Type.(clickhouse.MultiValueType).Cols))
	assert.Equal(t, 5, len(table.Cols["process"].Type.(clickhouse.MultiValueType).Cols))
}

func Test_parseMultiValueType(t *testing.T) {
	tupleQueryPart := []string{"(d DateTime64(3) )", "(d DateTime64(3))"}
	for i, tuple := range tupleQueryPart {
		t.Run(util.PrettyTestName(tuple, i), func(t *testing.T) {
			indexAfterMatch, columns := parseMultiValueType(tuple, 0)
			assert.NotEqual(t, -1, indexAfterMatch)
			assert.Len(t, columns, 1)
		})
	}
}

func TestParseCreateTableWithNullable(t *testing.T) {
	const columnNr = 9
	q := `CREATE TABLE IF NOT EXISTS "logs-generic-default"
		(
			"nullable-string" Nullable(String),
			"nullable-date-time-1" Nullable(DateTime64(6, 'UTC') ),
    		"nullable-date-time-2" Nullable(DateTime64),
    		"nullable-date-time-3" Nullable(DateTime('UTC') ),
			"non-nullable-string" String,
			"nullable-array" Array(Nullable(String)),
    		"non-nullable-array" Array(Int64),
    		"tuple" Tuple(a String, b Nullable(String), c Tuple(c String, d Nullable(UInt128))),
    		"array-tuple" Array(Tuple(nullable Nullable(String), "non-nullable" String))
		)
		ENGINE = Log`
	table, err := clickhouse.NewTable(q, nil)
	assert.NoError(t, err)
	assert.Equal(t, columnNr, len(table.Cols))
	for _, colName := range []string{"nullable-string", "nullable-date-time-1", "nullable-date-time-2", "nullable-date-time-3"} {
		assert.True(t, table.Cols[colName].Type.IsNullable(), colName)
	}
	for _, colName := range []string{"non-nullable-string", "nullable-array", "non-nullable-array", "tuple", "array-tuple"} {
		assert.False(t, table.Cols[colName].Type.IsNullable(), colName)
	}
	// base types
	assert.True(t, table.Cols["nullable-array"].Type.(clickhouse.CompoundType).BaseType.IsNullable())
	assert.False(t, table.Cols["non-nullable-array"].Type.(clickhouse.CompoundType).BaseType.IsNullable())

	// tuple
	assert.False(t, table.Cols["tuple"].Type.(clickhouse.MultiValueType).Cols[0].Type.IsNullable())
	assert.True(t, table.Cols["tuple"].Type.(clickhouse.MultiValueType).Cols[1].Type.IsNullable())
	assert.False(t, table.Cols["tuple"].Type.(clickhouse.MultiValueType).Cols[2].Type.(clickhouse.MultiValueType).Cols[0].Type.IsNullable())
	assert.True(t, table.Cols["tuple"].Type.(clickhouse.MultiValueType).Cols[2].Type.(clickhouse.MultiValueType).Cols[1].Type.IsNullable())

	// array(tuple)
	assert.True(t, table.Cols["array-tuple"].Type.(clickhouse.CompoundType).BaseType.(clickhouse.MultiValueType).Cols[0].Type.IsNullable())
	assert.False(t, table.Cols["array-tuple"].Type.(clickhouse.CompoundType).BaseType.(clickhouse.MultiValueType).Cols[1].Type.IsNullable())
}

func TestParseCreateTableWithDots(t *testing.T) {
	const columnNr = 5
	q := `CREATE TABLE IF NOT EXISTS "my-index-2.3" 
			(
				"attributes_values" Map(String,String),
				"attributes_metadata" Map(String,String),
			
				"level" Nullable(String) COMMENT 'quesmaMetadataV1:fieldName=level',
				"@timestamp" DateTime64 DEFAULT now64() COMMENT 'quesmaMetadataV1:fieldName=%40timestamp',
				"message" Nullable(String) COMMENT 'quesmaMetadataV1:fieldName=message'
			)
			ENGINE = MergeTree
			ORDER BY ("@timestamp")
			
			COMMENT 'created by Quesma'`
	table, err := clickhouse.NewTable(q, nil)
	assert.NoError(t, err)
	assert.Equal(t, columnNr, len(table.Cols))
	assert.Equal(t, "my-index-2.3", table.Name)
}
