
-- This is a temporary solution. Quesma type discovery is somehow limited.
-- We need to create a table with the correct types and then insert data into it.
-- So we create a table on database startup.

CREATE TABLE IF NOT EXISTS "device_logs" (
	"client_id" String CODEC(ZSTD(1)),
	"properties::pv_event" LowCardinality(String),
	"properties::signed_state" LowCardinality(String),
	"properties::user_os_ver" LowCardinality(String),
	"client_ip" String CODEC(ZSTD(1)),
	"ts_day" LowCardinality(String),
	"properties::isreg" Bool,
	"properties::enriched_client_ip" String CODEC(ZSTD(1)),
	"properties::referrer_action" LowCardinality(String),
	"properties::user_language_primary" LowCardinality(String),
	"properties::user_feed_type" LowCardinality(String),
	"properties::tabtype" LowCardinality(String),
	"et_day" String CODEC(ZSTD(1)),
	"epoch_time_original" Int64 CODEC(DoubleDelta, LZ4),
	"epoch_time" DateTime64 CODEC(DoubleDelta, LZ4),
	"properties::enriched_app_id" String CODEC(ZSTD(1)),
	"properties::ab_NewsStickyType" LowCardinality(String),
	"properties::user_handset_model" LowCardinality(String),
	"event_section" LowCardinality(String),
	"dedup_id" String CODEC(ZSTD(1)),
	"user_id" String CODEC(ZSTD(1)),
	"ts_time_druid" DateTime64 CODEC(DoubleDelta, LZ4),
	"et_day_hour" String CODEC(ZSTD(1)),
	"properties::server_loc" String CODEC(ZSTD(1)),
	"properties::enriched_user_language_primary" LowCardinality(String),
	"properties::user_os_name" LowCardinality(String),
	"properties::user_app_ver" LowCardinality(String),
	"ftd_session_time" Int64 CODEC(DoubleDelta, LZ4),
	"properties::country_detection_mechanism" LowCardinality(String),
	"properties::user_type" LowCardinality(String),
	"properties::user_handset_maker" LowCardinality(String),
	"event_name" LowCardinality(String),
	"timestamps::topology_entry_time" String CODEC(ZSTD(1)),
	"properties::selected_country" LowCardinality(String),
	"properties::network_service_provider" LowCardinality(String),
	"ts_day_hour" String CODEC(ZSTD(1)),
	"properties::enriched_user_id" String CODEC(ZSTD(1)),
	"properties::app_id" String CODEC(ZSTD(1)),
	"attributes_string_key" Array(String),
	"attributes_string_value" Array(String),
	INDEX event_name_idx event_name TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY epoch_time
PARTITION BY toYYYYMM(epoch_time)
TTL toDateTime(epoch_time) + INTERVAL 20 MINUTE
SETTINGS index_granularity = 8192
