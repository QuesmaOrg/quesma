CREATE TABLE IF NOT EXISTS "windows_logs"
(
    "attributes_string_key" Array(String),
    "attributes_string_value" Array(String),

    "@timestamp" DateTime64 DEFAULT now64(),

    "event::category" Nullable(String),
    "event::type" Nullable(String),

    "dll::name" Nullable(String),
    "dll::path" Nullable(String),

    "registry::path" Nullable(String),
    "registry::value" Nullable(String),
    "registry::key" Nullable(String),

    "destination::address" Nullable(String),
    "destination::port" Nullable(String),

    "network::protocol" Nullable(String),
    "network::direction" Nullable(String),

    "source::address" Nullable(String),
    "source::port" Nullable(String),

    "process::pid" Nullable(Int64),
    "process::entity_id" Nullable(String),
    "process::executable" Nullable(String),
    "process::name" Nullable(String),

    "user::id" Nullable(String),
    "user::domain" Nullable(String),
    "user::full_name" Nullable(String),

)
ENGINE = MergeTree
ORDER BY ("@timestamp")
COMMENT 'Windows Security Logs. Created by clean-clickhouse.'