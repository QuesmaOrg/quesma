
CREATE TABLE IF NOT EXISTS "type_logs"
(
    "attributes_string_key" Array(String),
    "attributes_string_value" Array(String),

    "@timestamp" DateTime64 DEFAULT now64(),

    `map_string_string` Map(String, Nullable(String)),
    `point` Point,
    `ipv4`  IPv4,
    `ipv6`  IPv6,
)
ENGINE = MergeTree
ORDER BY ("@timestamp")
COMMENT 'Table for type testing. Created by clean-clickhouse.'
;








