Skip to content
Navigation Menu
QuesmaOrg
quesma

Type / to search
Code
Issues
23
Pull requests
29
Discussions
Actions
Security
Insights
Settings
Your recovery codes have not been saved in the past year. Make sure you still have them stored somewhere safe by viewing and downloading them again.


E2E tests
E2E tests #1541
Jobs
Run details
Annotations
1 error and 1 warning
e2e-test-run
failed 32 minutes ago in 3m 19s
Search logs
0s
1s
1s
1s
0s
1m 29s
2s
0s
0s
1m 40s
Run go test -race --tags=integration  -v ./...
go: downloading github.com/goccy/go-json v0.10.5
go: downloading github.com/coreos/go-semver v0.3.1
go: downloading github.com/ClickHouse/clickhouse-go/v2 v2.30.1
go: downloading github.com/go-sql-driver/mysql v1.8.1
go: downloading github.com/stretchr/testify v1.10.0
go: downloading github.com/jackc/pgx/v5 v5.7.2
go: downloading github.com/DATA-DOG/go-sqlmock v1.5.2
go: downloading vitess.io/vitess v0.21.2
go: downloading github.com/google/uuid v1.6.0
go: downloading github.com/rs/zerolog v1.33.0
go: downloading github.com/ucarion/urlpath v0.0.0-20200424170820-7ccc79b76bbb
go: downloading github.com/google/go-cmp v0.6.0
go: downloading github.com/hashicorp/go-multierror v1.1.1
go: downloading github.com/H0llyW00dzZ/cidr v1.2.1
go: downloading github.com/apparentlymart/go-cidr v1.1.0
go: downloading github.com/k0kubun/pp v3.0.1+incompatible
go: downloading github.com/pkg/errors v0.9.1
go: downloading github.com/knadh/koanf/parsers/json v0.1.0
go: downloading github.com/knadh/koanf/parsers/yaml v0.1.0
go: downloading github.com/knadh/koanf/providers/file v1.1.2
go: downloading github.com/knadh/koanf/v2 v2.1.2
go: downloading github.com/tidwall/sjson v1.2.5
go: downloading github.com/barkimedes/go-deepcopy v0.0.0-20220514131651-17c30cfc62df
go: downloading github.com/tailscale/hujson v0.0.0-20241010212012-29efb4a0184b
go: downloading github.com/gorilla/mux v1.8.1
go: downloading github.com/gorilla/securecookie v1.1.2
go: downloading github.com/shirou/gopsutil/v3 v3.24.5
go: downloading github.com/gorilla/sessions v1.4.0
go: downloading github.com/markbates/goth v1.80.0
go: downloading github.com/DataDog/go-sqllexer v0.0.20
go: downloading github.com/klauspost/compress v1.17.11
go: downloading golang.org/x/exp v0.0.0-20250106191152-7588d65b2ba8
go: downloading golang.org/x/oauth2 v0.25.0
go: downloading gopkg.in/yaml.v3 v3.0.1
go: downloading filippo.io/edwards25519 v1.1.0
go: downloading github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc
go: downloading github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2
go: downloading github.com/ClickHouse/ch-go v0.63.1
go: downloading github.com/andybalholm/brotli v1.1.1
go: downloading go.opentelemetry.io/otel/trace v1.33.0
go: downloading go.opentelemetry.io/otel v1.33.0
go: downloading github.com/mattn/go-colorable v0.1.14
go: downloading github.com/pires/go-proxyproto v0.7.0
go: downloading github.com/spf13/pflag v1.0.5
go: downloading google.golang.org/protobuf v1.34.2
go: downloading github.com/planetscale/vtprotobuf v0.6.1-0.20240319094008-0393e58bdf10
go: downloading github.com/hashicorp/errwrap v1.1.0
go: downloading github.com/fsnotify/fsnotify v1.8.0
go: downloading github.com/go-viper/mapstructure/v2 v2.2.1
go: downloading github.com/knadh/koanf/maps v0.1.1
go: downloading github.com/mitchellh/copystructure v1.2.0
go: downloading github.com/tidwall/gjson v1.18.0
go: downloading golang.org/x/sys v0.29.0
go: downloading github.com/tklauser/go-sysconf v0.3.14
go: downloading github.com/paulmach/orb v0.11.1
go: downloading github.com/shopspring/decimal v1.4.0
go: downloading github.com/go-faster/city v1.0.1
go: downloading github.com/go-faster/errors v0.7.1
go: downloading github.com/pierrec/lz4/v4 v4.1.22
go: downloading github.com/segmentio/asm v1.2.0
go: downloading github.com/jackc/pgpassfile v1.0.0
go: downloading github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761
go: downloading golang.org/x/crypto v0.32.0
go: downloading golang.org/x/text v0.21.0
go: downloading github.com/jackc/puddle/v2 v2.2.2
go: downloading github.com/mattn/go-isatty v0.0.20
go: downloading github.com/golang/glog v1.2.4
go: downloading google.golang.org/grpc v1.66.2
go: downloading github.com/mitchellh/reflectwalk v1.0.2
go: downloading github.com/tidwall/match v1.1.1
go: downloading github.com/tidwall/pretty v1.2.1
go: downloading github.com/tklauser/numcpus v0.9.0
go: downloading golang.org/x/sync v0.10.0
go: downloading google.golang.org/genproto/googleapis/rpc v0.0.0-20240903143218-8af14fe29dc1
?   	github.com/QuesmaOrg/quesma/quesma/ab_testing	[no test files]
?   	github.com/QuesmaOrg/quesma/quesma/ab_testing/collector	[no test files]
?   	github.com/QuesmaOrg/quesma/quesma/ab_testing/sender	[no test files]
?   	github.com/QuesmaOrg/quesma/quesma/backend_connectors	[no test files]
?   	github.com/QuesmaOrg/quesma/quesma/buildinfo	[no test files]
=== RUN   Test_Main
--- PASS: Test_Main (0.00s)
PASS
ok  	github.com/QuesmaOrg/quesma/quesma	1.041s
?   	github.com/QuesmaOrg/quesma/quesma/common_table	[no test files]
?   	github.com/QuesmaOrg/quesma/quesma/connectors	[no test files]
?   	github.com/QuesmaOrg/quesma/quesma/elasticsearch/elasticsearch_field_types	[no test files]
=== RUN   TestJsonToFieldsMap
--- PASS: TestJsonToFieldsMap (0.00s)
=== RUN   TestDifferenceMapSimple_1
--- PASS: TestDifferenceMapSimple_1 (0.00s)
=== RUN   TestDifferenceMapSimple_2
--- PASS: TestDifferenceMapSimple_2 (0.00s)
=== RUN   TestDifferenceMapNested
--- PASS: TestDifferenceMapNested (0.00s)
=== RUN   TestDifferenceMapSimpleAndNested_1
--- PASS: TestDifferenceMapSimpleAndNested_1 (0.00s)
=== RUN   TestDifferenceMapSimpleAndNested_2
--- PASS: TestDifferenceMapSimpleAndNested_2 (0.00s)
=== RUN   TestDifferenceMapBig
--- PASS: TestDifferenceMapBig (0.00s)
=== RUN   TestRemovingNonSchemaFields
--- PASS: TestRemovingNonSchemaFields (0.00s)
=== RUN   TestJsonFlatteningToStringAttr
--- PASS: TestJsonFlatteningToStringAttr (0.00s)
=== RUN   TestJsonConvertingBoolToStringAttr
--- PASS: TestJsonConvertingBoolToStringAttr (0.00s)
=== RUN   TestCreateTableString_1
--- PASS: TestCreateTableString_1 (0.00s)
=== RUN   TestCreateTableString_NewDateTypes
--- PASS: TestCreateTableString_NewDateTypes (0.00s)
=== RUN   TestLogManager_GetTable
=== RUN   TestLogManager_GetTable/empty
=== RUN   TestLogManager_GetTable/should_find_by_name
=== RUN   TestLogManager_GetTable/should_not_find_by_name
=== RUN   TestLogManager_GetTable/should_find_by_pattern
=== RUN   TestLogManager_GetTable/should_find_by_pattern#01
=== RUN   TestLogManager_GetTable/should_find_by_pattern#02
=== RUN   TestLogManager_GetTable/should_find_by_pattern#03
=== RUN   TestLogManager_GetTable/should_not_find_by_pattern
--- PASS: TestLogManager_GetTable (0.00s)
--- PASS: TestLogManager_GetTable/empty (0.00s)
--- PASS: TestLogManager_GetTable/should_find_by_name (0.00s)
--- PASS: TestLogManager_GetTable/should_not_find_by_name (0.00s)
--- PASS: TestLogManager_GetTable/should_find_by_pattern (0.00s)
--- PASS: TestLogManager_GetTable/should_find_by_pattern#01 (0.00s)
--- PASS: TestLogManager_GetTable/should_find_by_pattern#02 (0.00s)
--- PASS: TestLogManager_GetTable/should_find_by_pattern#03 (0.00s)
--- PASS: TestLogManager_GetTable/should_not_find_by_pattern (0.00s)
=== RUN   TestLogManager_ResolveIndexes
=== RUN   TestLogManager_ResolveIndexes/empty_table_map,_non-empty_pattern
=== RUN   TestLogManager_ResolveIndexes/empty_table_map,_empty_pattern
=== RUN   TestLogManager_ResolveIndexes/non-empty_table_map,_empty_pattern
=== RUN   TestLogManager_ResolveIndexes/non-empty_table_map,__all_pattern
=== RUN   TestLogManager_ResolveIndexes/non-empty_table_map,_*_pattern
=== RUN   TestLogManager_ResolveIndexes/non-empty_table_map,_*,*_pattern
=== RUN   TestLogManager_ResolveIndexes/non-empty_table_map,_table*_pattern
=== RUN   TestLogManager_ResolveIndexes/non-empty_table_map,_table1,table2_pattern
=== RUN   TestLogManager_ResolveIndexes/non-empty_table_map,_table1_pattern
=== RUN   TestLogManager_ResolveIndexes/non-empty_table_map,_table2_pattern
--- PASS: TestLogManager_ResolveIndexes (0.00s)
--- PASS: TestLogManager_ResolveIndexes/empty_table_map,_non-empty_pattern (0.00s)
--- PASS: TestLogManager_ResolveIndexes/empty_table_map,_empty_pattern (0.00s)
--- PASS: TestLogManager_ResolveIndexes/non-empty_table_map,_empty_pattern (0.00s)
--- PASS: TestLogManager_ResolveIndexes/non-empty_table_map,__all_pattern (0.00s)
--- PASS: TestLogManager_ResolveIndexes/non-empty_table_map,_*_pattern (0.00s)
--- PASS: TestLogManager_ResolveIndexes/non-empty_table_map,_*,*_pattern (0.00s)
--- PASS: TestLogManager_ResolveIndexes/non-empty_table_map,_table*_pattern (0.00s)
--- PASS: TestLogManager_ResolveIndexes/non-empty_table_map,_table1,table2_pattern (0.00s)
--- PASS: TestLogManager_ResolveIndexes/non-empty_table_map,_table1_pattern (0.00s)
--- PASS: TestLogManager_ResolveIndexes/non-empty_table_map,_table2_pattern (0.00s)
=== RUN   Test_removeDotsFromJsons
--- PASS: Test_removeDotsFromJsons (0.00s)
=== RUN   TestGetDateTimeType
--- PASS: TestGetDateTimeType (0.00s)
=== RUN   Test_resolveColumn
=== RUN   Test_resolveColumn/Bool
=== RUN   Test_resolveColumn/UInt64
=== RUN   Test_resolveColumn/Int64
=== RUN   Test_resolveColumn/String
=== RUN   Test_resolveColumn/Nullable(String)
=== RUN   Test_resolveColumn/LowCardinality(String)
=== RUN   Test_resolveColumn/DateTime
=== RUN   Test_resolveColumn/DateTime64
=== RUN   Test_resolveColumn/Date
=== RUN   Test_resolveColumn/DateTime64(3)
=== RUN   Test_resolveColumn/Array(String)
=== RUN   Test_resolveColumn/Array(DateTime64)
=== RUN   Test_resolveColumn/Tuple
=== RUN   Test_resolveColumn/Array(Tuple(...))
=== RUN   Test_resolveColumn/Array(Tuple(...))_used_to_panic
=== RUN   Test_resolveColumn/Array(DateTime64(3))
--- PASS: Test_resolveColumn (0.00s)
--- PASS: Test_resolveColumn/Bool (0.00s)
--- PASS: Test_resolveColumn/UInt64 (0.00s)
--- PASS: Test_resolveColumn/Int64 (0.00s)
--- PASS: Test_resolveColumn/String (0.00s)
--- PASS: Test_resolveColumn/Nullable(String) (0.00s)
--- PASS: Test_resolveColumn/LowCardinality(String) (0.00s)
--- PASS: Test_resolveColumn/DateTime (0.00s)
--- PASS: Test_resolveColumn/DateTime64 (0.00s)
--- PASS: Test_resolveColumn/Date (0.00s)
--- PASS: Test_resolveColumn/DateTime64(3) (0.00s)
--- PASS: Test_resolveColumn/Array(String) (0.00s)
--- PASS: Test_resolveColumn/Array(DateTime64) (0.00s)
--- PASS: Test_resolveColumn/Tuple (0.00s)
--- PASS: Test_resolveColumn/Array(Tuple(...)) (0.00s)
--- PASS: Test_resolveColumn/Array(Tuple(...))_used_to_panic (0.00s)
--- PASS: Test_resolveColumn/Array(DateTime64(3)) (0.00s)
=== RUN   Test_resolveColumn_Nullable
=== RUN   Test_resolveColumn_Nullable/BaseType_1
=== RUN   Test_resolveColumn_Nullable/BaseType_2
=== RUN   Test_resolveColumn_Nullable/LowCardinality(String)
=== RUN   Test_resolveColumn_Nullable/DateTime64(3)
=== RUN   Test_resolveColumn_Nullable/Array(Nullable(String))
=== RUN   Test_resolveColumn_Nullable/Array(DateTime64)
=== RUN   Test_resolveColumn_Nullable/Tuple
=== RUN   Test_resolveColumn_Nullable/Array(Tuple(...))
--- PASS: Test_resolveColumn_Nullable (0.00s)
--- PASS: Test_resolveColumn_Nullable/BaseType_1 (0.00s)
--- PASS: Test_resolveColumn_Nullable/BaseType_2 (0.00s)
--- PASS: Test_resolveColumn_Nullable/LowCardinality(String) (0.00s)
--- PASS: Test_resolveColumn_Nullable/DateTime64(3) (0.00s)
--- PASS: Test_resolveColumn_Nullable/Array(Nullable(String)) (0.00s)
--- PASS: Test_resolveColumn_Nullable/Array(DateTime64) (0.00s)
--- PASS: Test_resolveColumn_Nullable/Tuple (0.00s)
--- PASS: Test_resolveColumn_Nullable/Array(Tuple(...)) (0.00s)
=== RUN   TestParseTypeFromShowColumns_1
--- PASS: TestParseTypeFromShowColumns_1 (0.00s)
=== RUN   TestParseTypeFromShowColumns_2
--- PASS: TestParseTypeFromShowColumns_2 (0.00s)
=== RUN   TestParseTypeFromShowColumns_3
--- PASS: TestParseTypeFromShowColumns_3 (0.00s)
=== RUN   TestParseTypeFromShowColumnsTuple_1
--- PASS: TestParseTypeFromShowColumnsTuple_1 (0.00s)
=== RUN   TestParseTypeFromShowColumnsTuple_2
--- PASS: TestParseTypeFromShowColumnsTuple_2 (0.00s)
=== RUN   TestWhatDriverWillReturn
--- PASS: TestWhatDriverWillReturn (0.00s)
PASS
ok  	github.com/QuesmaOrg/quesma/quesma/clickhouse	1.027s
=== RUN   TestCommentMetadata_Marshall
=== RUN   TestCommentMetadata_Marshall/test1
=== RUN   TestCommentMetadata_Marshall/test2
--- PASS: TestCommentMetadata_Marshall (0.00s)
--- PASS: TestCommentMetadata_Marshall/test1 (0.00s)
--- PASS: TestCommentMetadata_Marshall/test2 (0.00s)
=== RUN   TestUnmarshallCommentMetadata
=== RUN   TestUnmarshallCommentMetadata/simple
=== RUN   TestUnmarshallCommentMetadata/with_special_characters
=== RUN   TestUnmarshallCommentMetadata/with_human_comments_
=== RUN   TestUnmarshallCommentMetadata/with_human_comments_invalid_version_
=== RUN   TestUnmarshallCommentMetadata/no_metadata_
--- PASS: TestUnmarshallCommentMetadata (0.00s)
--- PASS: TestUnmarshallCommentMetadata/simple (0.00s)
--- PASS: TestUnmarshallCommentMetadata/with_special_characters (0.00s)
--- PASS: TestUnmarshallCommentMetadata/with_human_comments_ (0.00s)
--- PASS: TestUnmarshallCommentMetadata/with_human_comments_invalid_version_ (0.00s)
--- PASS: TestUnmarshallCommentMetadata/no_metadata_ (0.00s)
PASS
ok  	github.com/QuesmaOrg/quesma/quesma/comment_metadata	1.012s
=== RUN   TestSimpleClient_Request_AddsContentTypeAndDoesntAuthenticateWhenNotConfigured
--- PASS: TestSimpleClient_Request_AddsContentTypeAndDoesntAuthenticateWhenNotConfigured (0.00s)
=== RUN   TestSimpleClient_Request_AddsAuthHeadersIfElasticsearchAuthConfigured
--- PASS: TestSimpleClient_Request_AddsAuthHeadersIfElasticsearchAuthConfigured (0.00s)
=== RUN   TestSimpleClient_Authenticate_UsesAuthHeader
--- PASS: TestSimpleClient_Authenticate_UsesAuthHeader (0.00s)
=== RUN   TestSimpleClient_RequestWithHeaders_OverwritesContentType
--- PASS: TestSimpleClient_RequestWithHeaders_OverwritesContentType (0.00s)
=== RUN   TestIsValidIndexName
=== RUN   TestIsValidIndexName/foo
=== RUN   TestIsValidIndexName/foo_bar
=== RUN   TestIsValidIndexName/esc_base_agent_client_cloud_container_data_stream_destination_device_dll_dns_ecs_email_error_event_faas_file_group_host_http_log_network_observer_orchestrator_organization_package_process_registry_related_rule_server_service_source_threat_tls_url_user_user_agent_volume_vulnerability_windows
--- PASS: TestIsValidIndexName (0.00s)
--- PASS: TestIsValidIndexName/foo (0.00s)
--- PASS: TestIsValidIndexName/foo_bar (0.00s)
--- PASS: TestIsValidIndexName/esc_base_agent_client_cloud_container_data_stream_destination_device_dll_dns_ecs_email_error_event_faas_file_group_host_http_log_network_observer_orchestrator_organization_package_process_registry_related_rule_server_service_source_threat_tls_url_user_user_agent_volume_vulnerability_windows (0.00s)
=== RUN   TestIsWriteRequest
=== RUN   TestIsWriteRequest/POST_/_bulk
=== RUN   TestIsWriteRequest/POST_/_doc
=== RUN   TestIsWriteRequest/POST_/_create
=== RUN   TestIsWriteRequest/PUT_/_create
=== RUN   TestIsWriteRequest/POST_/_search
--- PASS: TestIsWriteRequest (0.00s)
--- PASS: TestIsWriteRequest/POST_/_bulk (0.00s)
--- PASS: TestIsWriteRequest/POST_/_doc (0.00s)
--- PASS: TestIsWriteRequest/POST_/_create (0.00s)
--- PASS: TestIsWriteRequest/PUT_/_create (0.00s)
--- PASS: TestIsWriteRequest/POST_/_search (0.00s)
PASS
ok  	github.com/QuesmaOrg/quesma/quesma/elasticsearch	1.017s
=== RUN   TestNewUnsupportedFeature_index
=== RUN   TestNewUnsupportedFeature_index//foo/_search
=== RUN   TestNewUnsupportedFeature_index//foo/_new_feature
=== RUN   TestNewUnsupportedFeature_index//bar/_search
=== RUN   TestNewUnsupportedFeature_index//foo/_search/template
Feb 10 13:50:17.000 WRN Not supported feature detected.  index: 'foo' request: 'GET /foo/_search''
Feb 10 13:50:17.000 WRN Not supported feature detected.  index: 'foo' request: 'GET /foo/_new_feature''
=== RUN   TestNewUnsupportedFeature_index//_scripts/foo
Feb 10 13:50:17.000 WRN Not supported feature detected.  index: foo, request: 'GET /foo/_search/template'
=== RUN   TestNewUnsupportedFeature_index//logs-elastic_agent-*/_search
Feb 10 13:50:17.000 WRN Not supported feature detected. Request  'GET /_scripts/foo'
=== RUN   TestNewUnsupportedFeature_index//foo/_search#01
--- PASS: TestNewUnsupportedFeature_index (0.00s)
--- PASS: TestNewUnsupportedFeature_index//foo/_search (0.00s)
--- PASS: TestNewUnsupportedFeature_index//foo/_new_feature (0.00s)
--- PASS: TestNewUnsupportedFeature_index//bar/_search (0.00s)
--- PASS: TestNewUnsupportedFeature_index//foo/_search/template (0.00s)
--- PASS: TestNewUnsupportedFeature_index//_scripts/foo (0.00s)
--- PASS: TestNewUnsupportedFeature_index//logs-elastic_agent-*/_search (0.00s)
--- PASS: TestNewUnsupportedFeature_index//foo/_search#01 (0.00s)
=== RUN   TestIndexRegexp
=== RUN   TestIndexRegexp//foo/bar
=== RUN   TestIndexRegexp//foo/_search
=== RUN   TestIndexRegexp//foo/_search/template
=== RUN   TestIndexRegexp//foo/_scripts
=== RUN   TestIndexRegexp//.banana_1.23.4/_doc/some_garbage_here_(Macintosh;_Intel_Mac_OS_X_10_15_7)_
=== RUN   TestIndexRegexp//.reporting-*/_search
=== RUN   TestIndexRegexp//traces-xx*,xx-*,traces-xx*,x-*,logs-xx*,xx-*/_search
--- PASS: TestIndexRegexp (0.00s)
--- PASS: TestIndexRegexp//foo/bar (0.00s)
--- PASS: TestIndexRegexp//foo/_search (0.00s)
--- PASS: TestIndexRegexp//foo/_search/template (0.00s)
--- PASS: TestIndexRegexp//foo/_scripts (0.00s)
--- PASS: TestIndexRegexp//.banana_1.23.4/_doc/some_garbage_here_(Macintosh;_Intel_Mac_OS_X_10_15_7)_ (0.00s)
--- PASS: TestIndexRegexp//.reporting-*/_search (0.00s)
--- PASS: TestIndexRegexp//traces-xx*,xx-*,traces-xx*,x-*,logs-xx*,xx-*/_search (0.00s)
PASS
ok  	github.com/QuesmaOrg/quesma/quesma/elasticsearch/feature	1.012s
=== RUN   TestEndUserError_error_as
--- PASS: TestEndUserError_error_as (0.00s)
PASS
ok  	github.com/QuesmaOrg/quesma/quesma/end_user_errors	1.008s
?   	github.com/QuesmaOrg/quesma/quesma/licensing	[no test files]
=== RUN   Test_OsdHeaders
--- PASS: Test_OsdHeaders (0.00s)
=== RUN   Test_EsHeaders
--- PASS: Test_EsHeaders (0.00s)
=== RUN   TestFindMissingElasticsearchHeaders
--- PASS: TestFindMissingElasticsearchHeaders (0.00s)
PASS
ok  	github.com/QuesmaOrg/quesma/quesma/frontend_connectors	1.021s
?   	github.com/QuesmaOrg/quesma/quesma/model/pipeline_aggregations	[no test files]
?   	github.com/QuesmaOrg/quesma/quesma/model/typical_queries	[no test files]
?   	github.com/QuesmaOrg/quesma/quesma/processors	[no test files]
?   	github.com/QuesmaOrg/quesma/quesma/processors/es_to_ch_common	[no test files]
?   	github.com/QuesmaOrg/quesma/quesma/processors/es_to_ch_ingest	[no test files]
?   	github.com/QuesmaOrg/quesma/quesma/processors/es_to_ch_query	[no test files]
?   	github.com/QuesmaOrg/quesma/quesma/queryparser/query_util	[no test files]
=== RUN   TestAlterTable
--- PASS: TestAlterTable (0.00s)
=== RUN   TestAlterTableHeuristic
--- PASS: TestAlterTableHeuristic (6.62s)
=== RUN   TestIngestToCommonTable
=== RUN   TestIngestToCommonTable/simple_single_insert
=== RUN   TestIngestToCommonTable/simple_inserts
=== RUN   TestIngestToCommonTable/simple_inserts_and_new_column
=== RUN   TestIngestToCommonTable/simple_inserts,_column_exists,_but_not_ingested
=== RUN   TestIngestToCommonTable/ingest_to_existing_column
=== RUN   TestIngestToCommonTable/ingest_to_existing_column_and_new_column
=== RUN   TestIngestToCommonTable/ingest_to_name_with_a_dot
--- PASS: TestIngestToCommonTable (0.01s)
--- PASS: TestIngestToCommonTable/simple_single_insert (0.00s)
--- PASS: TestIngestToCommonTable/simple_inserts (0.00s)
--- PASS: TestIngestToCommonTable/simple_inserts_and_new_column (0.00s)
--- PASS: TestIngestToCommonTable/simple_inserts,_column_exists,_but_not_ingested (0.00s)
--- PASS: TestIngestToCommonTable/ingest_to_existing_column (0.00s)
--- PASS: TestIngestToCommonTable/ingest_to_existing_column_and_new_column (0.00s)
--- PASS: TestIngestToCommonTable/ingest_to_name_with_a_dot (0.00s)
=== RUN   Test_removeDotsFromJsons
--- PASS: Test_removeDotsFromJsons (0.00s)
=== RUN   TestGetTypeName
=== RUN   TestGetTypeName/Int64
=== RUN   TestGetTypeName/Bool
=== RUN   TestGetTypeName/Array(Int64)
=== RUN   TestGetTypeName/Array(Array(Int64))
=== RUN   TestGetTypeName/Array(Array(Array(Int64)))
=== RUN   TestGetTypeName/Float64
=== RUN   TestGetTypeName/String
=== RUN   TestGetTypeName/Array(UInt64)
=== RUN   TestGetTypeName/UInt64
--- PASS: TestGetTypeName (0.00s)
--- PASS: TestGetTypeName/Int64 (0.00s)
--- PASS: TestGetTypeName/Bool (0.00s)
--- PASS: TestGetTypeName/Array(Int64) (0.00s)
--- PASS: TestGetTypeName/Array(Array(Int64)) (0.00s)
--- PASS: TestGetTypeName/Array(Array(Array(Int64))) (0.00s)
--- PASS: TestGetTypeName/Float64 (0.00s)
--- PASS: TestGetTypeName/String (0.00s)
--- PASS: TestGetTypeName/Array(UInt64) (0.00s)
--- PASS: TestGetTypeName/UInt64 (0.00s)
=== RUN   TestValidateIngest
--- PASS: TestValidateIngest (0.00s)
=== RUN   TestIngestValidation
--- PASS: TestIngestValidation (0.01s)
=== RUN   TestAutomaticTableCreationAtInsert
=== RUN   TestAutomaticTableCreationAtInsert/case_insertTest[0],_config[0],_ingestProcessor[0]
=== RUN   TestAutomaticTableCreationAtInsert/case_insertTest[0],_config[0],_ingestProcessor[1]
=== RUN   TestAutomaticTableCreationAtInsert/case_insertTest[0],_config[0],_ingestProcessor[2]
=== RUN   TestAutomaticTableCreationAtInsert/case_insertTest[0],_config[1],_ingestProcessor[0]
=== RUN   TestAutomaticTableCreationAtInsert/case_insertTest[0],_config[1],_ingestProcessor[1]
=== RUN   TestAutomaticTableCreationAtInsert/case_insertTest[0],_config[1],_ingestProcessor[2]
=== RUN   TestAutomaticTableCreationAtInsert/case_insertTest[1],_config[0],_ingestProcessor[0]
=== RUN   TestAutomaticTableCreationAtInsert/case_insertTest[1],_config[0],_ingestProcessor[1]
=== RUN   TestAutomaticTableCreationAtInsert/case_insertTest[1],_config[0],_ingestProcessor[2]
=== RUN   TestAutomaticTableCreationAtInsert/case_insertTest[1],_config[1],_ingestProcessor[0]
=== RUN   TestAutomaticTableCreationAtInsert/case_insertTest[1],_config[1],_ingestProcessor[1]
=== RUN   TestAutomaticTableCreationAtInsert/case_insertTest[1],_config[1],_ingestProcessor[2]
--- PASS: TestAutomaticTableCreationAtInsert (0.00s)
--- PASS: TestAutomaticTableCreationAtInsert/case_insertTest[0],_config[0],_ingestProcessor[0] (0.00s)
--- PASS: TestAutomaticTableCreationAtInsert/case_insertTest[0],_config[0],_ingestProcessor[1] (0.00s)
--- PASS: TestAutomaticTableCreationAtInsert/case_insertTest[0],_config[0],_ingestProcessor[2] (0.00s)
--- PASS: TestAutomaticTableCreationAtInsert/case_insertTest[0],_config[1],_ingestProcessor[0] (0.00s)
--- PASS: TestAutomaticTableCreationAtInsert/case_insertTest[0],_config[1],_ingestProcessor[1] (0.00s)
--- PASS: TestAutomaticTableCreationAtInsert/case_insertTest[0],_config[1],_ingestProcessor[2] (0.00s)
--- PASS: TestAutomaticTableCreationAtInsert/case_insertTest[1],_config[0],_ingestProcessor[0] (0.00s)
--- PASS: TestAutomaticTableCreationAtInsert/case_insertTest[1],_config[0],_ingestProcessor[1] (0.00s)
--- PASS: TestAutomaticTableCreationAtInsert/case_insertTest[1],_config[0],_ingestProcessor[2] (0.00s)
--- PASS: TestAutomaticTableCreationAtInsert/case_insertTest[1],_config[1],_ingestProcessor[0] (0.00s)
--- PASS: TestAutomaticTableCreationAtInsert/case_insertTest[1],_config[1],_ingestProcessor[1] (0.00s)
--- PASS: TestAutomaticTableCreationAtInsert/case_insertTest[1],_config[1],_ingestProcessor[2] (0.00s)
=== RUN   TestProcessInsertQuery
=== RUN   TestProcessInsertQuery/case_insertTest[0],_config[0],_ingestProcessor[0]
=== RUN   TestProcessInsertQuery/case_insertTest[0],_config[0],_ingestProcessor[1]
=== RUN   TestProcessInsertQuery/case_insertTest[0],_config[0],_ingestProcessor[2]
=== RUN   TestProcessInsertQuery/case_insertTest[0],_config[1],_ingestProcessor[0]
=== RUN   TestProcessInsertQuery/case_insertTest[0],_config[1],_ingestProcessor[1]
=== RUN   TestProcessInsertQuery/case_insertTest[0],_config[1],_ingestProcessor[2]
=== RUN   TestProcessInsertQuery/case_insertTest[1],_config[0],_ingestProcessor[0]
=== RUN   TestProcessInsertQuery/case_insertTest[1],_config[0],_ingestProcessor[1]
=== RUN   TestProcessInsertQuery/case_insertTest[1],_config[0],_ingestProcessor[2]
=== RUN   TestProcessInsertQuery/case_insertTest[1],_config[1],_ingestProcessor[0]
=== RUN   TestProcessInsertQuery/case_insertTest[1],_config[1],_ingestProcessor[1]
=== RUN   TestProcessInsertQuery/case_insertTest[1],_config[1],_ingestProcessor[2]
--- PASS: TestProcessInsertQuery (0.03s)
--- PASS: TestProcessInsertQuery/case_insertTest[0],_config[0],_ingestProcessor[0] (0.00s)
--- PASS: TestProcessInsertQuery/case_insertTest[0],_config[0],_ingestProcessor[1] (0.00s)
--- PASS: TestProcessInsertQuery/case_insertTest[0],_config[0],_ingestProcessor[2] (0.00s)
--- PASS: TestProcessInsertQuery/case_insertTest[0],_config[1],_ingestProcessor[0] (0.00s)
--- PASS: TestProcessInsertQuery/case_insertTest[0],_config[1],_ingestProcessor[1] (0.00s)
--- PASS: TestProcessInsertQuery/case_insertTest[0],_config[1],_ingestProcessor[2] (0.00s)
--- PASS: TestProcessInsertQuery/case_insertTest[1],_config[0],_ingestProcessor[0] (0.00s)
--- PASS: TestProcessInsertQuery/case_insertTest[1],_config[0],_ingestProcessor[1] (0.00s)
--- PASS: TestProcessInsertQuery/case_insertTest[1],_config[0],_ingestProcessor[2] (0.00s)
--- PASS: TestProcessInsertQuery/case_insertTest[1],_config[1],_ingestProcessor[0] (0.00s)
--- PASS: TestProcessInsertQuery/case_insertTest[1],_config[1],_ingestProcessor[1] (0.00s)
--- PASS: TestProcessInsertQuery/case_insertTest[1],_config[1],_ingestProcessor[2] (0.00s)
=== RUN   TestInsertVeryBigIntegers
insert_test.go:285: TODO not implemented yet. Need a custom unmarshaller, and maybe also a marshaller.
--- SKIP: TestInsertVeryBigIntegers (0.00s)
=== RUN   TestCreateTableIfSomeFieldsExistsInSchemaAlready
=== RUN   TestCreateTableIfSomeFieldsExistsInSchemaAlready/simple_single_insert
--- PASS: TestCreateTableIfSomeFieldsExistsInSchemaAlready (0.00s)
--- PASS: TestCreateTableIfSomeFieldsExistsInSchemaAlready/simple_single_insert (0.00s)
=== RUN   TestParseSignozSchema_1
--- PASS: TestParseSignozSchema_1 (0.00s)
=== RUN   TestParseSignozSchema_2
--- PASS: TestParseSignozSchema_2 (0.00s)
=== RUN   TestParseQuotedTablename
--- PASS: TestParseQuotedTablename (0.00s)
=== RUN   TestParseNonLetterNames
--- PASS: TestParseNonLetterNames (0.00s)
=== RUN   TestParseLongNestedSchema
--- PASS: TestParseLongNestedSchema (0.00s)
=== RUN   Test_parseMultiValueType
=== RUN   Test_parseMultiValueType/(d_DateTime64(3)_)
=== RUN   Test_parseMultiValueType/(d_DateTime64(3))
--- PASS: Test_parseMultiValueType (0.00s)
--- PASS: Test_parseMultiValueType/(d_DateTime64(3)_) (0.00s)
--- PASS: Test_parseMultiValueType/(d_DateTime64(3)) (0.00s)
=== RUN   TestParseCreateTableWithNullable
--- PASS: TestParseCreateTableWithNullable (0.00s)
=== RUN   TestInsertNonSchemaFieldsToOthers_1
--- PASS: TestInsertNonSchemaFieldsToOthers_1 (0.00s)
=== RUN   TestAddTimestamp
--- PASS: TestAddTimestamp (0.00s)
=== RUN   TestJsonToFieldsMap
--- PASS: TestJsonToFieldsMap (0.00s)
=== RUN   TestDifferenceMapSimple_1
--- PASS: TestDifferenceMapSimple_1 (0.00s)
=== RUN   TestDifferenceMapSimple_2
--- PASS: TestDifferenceMapSimple_2 (0.00s)
=== RUN   TestDifferenceMapNested
--- PASS: TestDifferenceMapNested (0.00s)
=== RUN   TestDifferenceMapSimpleAndNested_1
--- PASS: TestDifferenceMapSimpleAndNested_1 (0.00s)
=== RUN   TestDifferenceMapSimpleAndNested_2
--- PASS: TestDifferenceMapSimpleAndNested_2 (0.00s)
=== RUN   TestDifferenceMapBig
--- PASS: TestDifferenceMapBig (0.00s)
=== RUN   TestRemovingNonSchemaFields
--- PASS: TestRemovingNonSchemaFields (0.00s)
=== RUN   TestJsonFlatteningToStringAttr
--- PASS: TestJsonFlatteningToStringAttr (0.00s)
=== RUN   TestJsonConvertingBoolToStringAttr
--- PASS: TestJsonConvertingBoolToStringAttr (0.00s)
=== RUN   TestCreateTableString_1
--- PASS: TestCreateTableString_1 (0.00s)
=== RUN   TestCreateTableString_NewDateTypes
--- PASS: TestCreateTableString_NewDateTypes (0.00s)
=== RUN   TestLogManager_GetTable
=== RUN   TestLogManager_GetTable/empty
=== RUN   TestLogManager_GetTable/should_find_by_name
=== RUN   TestLogManager_GetTable/should_not_find_by_name
=== RUN   TestLogManager_GetTable/should_find_by_pattern
=== RUN   TestLogManager_GetTable/should_find_by_pattern#01
=== RUN   TestLogManager_GetTable/should_find_by_pattern#02
=== RUN   TestLogManager_GetTable/should_find_by_pattern#03
=== RUN   TestLogManager_GetTable/should_not_find_by_pattern
--- PASS: TestLogManager_GetTable (0.00s)
--- PASS: TestLogManager_GetTable/empty (0.00s)
--- PASS: TestLogManager_GetTable/should_find_by_name (0.00s)
--- PASS: TestLogManager_GetTable/should_not_find_by_name (0.00s)
--- PASS: TestLogManager_GetTable/should_find_by_pattern (0.00s)
--- PASS: TestLogManager_GetTable/should_find_by_pattern#01 (0.00s)
--- PASS: TestLogManager_GetTable/should_find_by_pattern#02 (0.00s)
--- PASS: TestLogManager_GetTable/should_find_by_pattern#03 (0.00s)
--- PASS: TestLogManager_GetTable/should_not_find_by_pattern (0.00s)
=== RUN   TestParseTypeFromShowColumns_1
--- PASS: TestParseTypeFromShowColumns_1 (0.00s)
=== RUN   TestParseTypeFromShowColumns_2
--- PASS: TestParseTypeFromShowColumns_2 (0.00s)
=== RUN   TestParseTypeFromShowColumns_3
--- PASS: TestParseTypeFromShowColumns_3 (0.00s)
=== RUN   TestParseTypeFromShowColumnsTuple_1
--- PASS: TestParseTypeFromShowColumnsTuple_1 (0.00s)
=== RUN   TestParseTypeFromShowColumnsTuple_2
--- PASS: TestParseTypeFromShowColumnsTuple_2 (0.00s)
PASS
ok  	github.com/QuesmaOrg/quesma/quesma/ingest	7.714s
=== RUN   TestLogForwarder
--- PASS: TestLogForwarder (0.11s)
=== RUN   TestLogSenderFlush
--- PASS: TestLogSenderFlush (0.00s)
=== RUN   TestLogSenderSmallBuffer
--- PASS: TestLogSenderSmallBuffer (0.40s)
=== RUN   TestLogSenderSmallElapsedTime
--- PASS: TestLogSenderSmallElapsedTime (0.54s)
PASS
ok  	github.com/QuesmaOrg/quesma/quesma/logger	2.070s
=== RUN   TestPartlyImplementedIsEqual
--- PASS: TestPartlyImplementedIsEqual (0.00s)
=== RUN   TestParenExpr
--- PASS: TestParenExpr (0.00s)
=== RUN   TestFieldCapability_Concat
=== RUN   TestFieldCapability_Concat/Two_text_FieldCapabilities,_different_indices
=== RUN   TestFieldCapability_Concat/Two_text_FieldCapabilities,_nil_MetadataField
=== RUN   TestFieldCapability_Concat/Two_text_FieldCapabilities,_different_indices,_one_non-aggregatable_and_non-searchable
=== RUN   TestFieldCapability_Concat/Two_text_FieldCapabilities,_same_index
=== RUN   TestFieldCapability_Concat/Two_incompatible_FieldCapabilities
--- PASS: TestFieldCapability_Concat (0.00s)
--- PASS: TestFieldCapability_Concat/Two_text_FieldCapabilities,_different_indices (0.00s)
--- PASS: TestFieldCapability_Concat/Two_text_FieldCapabilities,_nil_MetadataField (0.00s)
--- PASS: TestFieldCapability_Concat/Two_text_FieldCapabilities,_different_indices,_one_non-aggregatable_and_non-searchable (0.00s)
--- PASS: TestFieldCapability_Concat/Two_text_FieldCapabilities,_same_index (0.00s)
--- PASS: TestFieldCapability_Concat/Two_incompatible_FieldCapabilities (0.00s)
=== RUN   TestQueryResultCol_String
=== RUN   TestQueryResultCol_String/"name":_"test"
=== RUN   TestQueryResultCol_String/"name":_"test_\"GET\""
=== RUN   TestQueryResultCol_String/"name":_1
=== RUN   TestQueryResultCol_String/"name":_1#01
=== RUN   TestQueryResultCol_String/"name":_1#02
=== RUN   TestQueryResultCol_String/"name":_1#03
=== RUN   TestQueryResultCol_String/"name":_true
=== RUN   TestQueryResultCol_String/"name":_"0001-01-01_00:00:00_+0000_UTC"
=== RUN   TestQueryResultCol_String/"name":_""
=== RUN   TestQueryResultCol_String/#00
=== RUN   TestQueryResultCol_String/"name":_"0001-01-01_00:00:00_+0000_UTC"#01
=== RUN   TestQueryResultCol_String/#01
=== RUN   TestQueryResultCol_String/"name":_1#04
=== RUN   TestQueryResultCol_String/#02
=== RUN   TestQueryResultCol_String/"name":_1#05
=== RUN   TestQueryResultCol_String/#03
=== RUN   TestQueryResultCol_String/"name":_1#06
=== RUN   TestQueryResultCol_String/#04
=== RUN   TestQueryResultCol_String/"name":_true#01
=== RUN   TestQueryResultCol_String/#05
=== RUN   TestQueryResultCol_String/"name":_["a","b"]
=== RUN   TestQueryResultCol_String/"name":_[1,2]
=== RUN   TestQueryResultCol_String/"name":_[1,2]#01
=== RUN   TestQueryResultCol_String/"name":_[1,2]#02
=== RUN   TestQueryResultCol_String/"name":_[true,false]
=== RUN   TestQueryResultCol_String/"name":_{"a":"b"}
=== RUN   TestQueryResultCol_String/"name":_{"a":1}
=== RUN   TestQueryResultCol_String/"name":_{"a":{"int":1}}
--- PASS: TestQueryResultCol_String (0.01s)
--- PASS: TestQueryResultCol_String/"name":_"test" (0.00s)
--- PASS: TestQueryResultCol_String/"name":_"test_\"GET\"" (0.00s)
--- PASS: TestQueryResultCol_String/"name":_1 (0.00s)
--- PASS: TestQueryResultCol_String/"name":_1#01 (0.00s)
--- PASS: TestQueryResultCol_String/"name":_1#02 (0.00s)
--- PASS: TestQueryResultCol_String/"name":_1#03 (0.00s)
--- PASS: TestQueryResultCol_String/"name":_true (0.00s)
--- PASS: TestQueryResultCol_String/"name":_"0001-01-01_00:00:00_+0000_UTC" (0.00s)
--- PASS: TestQueryResultCol_String/"name":_"" (0.00s)
--- PASS: TestQueryResultCol_String/#00 (0.00s)
--- PASS: TestQueryResultCol_String/"name":_"0001-01-01_00:00:00_+0000_UTC"#01 (0.00s)
--- PASS: TestQueryResultCol_String/#01 (0.00s)
--- PASS: TestQueryResultCol_String/"name":_1#04 (0.00s)
--- PASS: TestQueryResultCol_String/#02 (0.00s)
--- PASS: TestQueryResultCol_String/"name":_1#05 (0.00s)
--- PASS: TestQueryResultCol_String/#03 (0.00s)
--- PASS: TestQueryResultCol_String/"name":_1#06 (0.00s)
--- PASS: TestQueryResultCol_String/#04 (0.00s)
--- PASS: TestQueryResultCol_String/"name":_true#01 (0.00s)
--- PASS: TestQueryResultCol_String/#05 (0.00s)
--- PASS: TestQueryResultCol_String/"name":_["a","b"] (0.00s)
--- PASS: TestQueryResultCol_String/"name":_[1,2] (0.00s)
--- PASS: TestQueryResultCol_String/"name":_[1,2]#01 (0.00s)
--- PASS: TestQueryResultCol_String/"name":_[1,2]#02 (0.00s)
--- PASS: TestQueryResultCol_String/"name":_[true,false] (0.00s)
--- PASS: TestQueryResultCol_String/"name":_{"a":"b"} (0.00s)
--- PASS: TestQueryResultCol_String/"name":_{"a":1} (0.00s)
--- PASS: TestQueryResultCol_String/"name":_{"a":{"int":1}} (0.00s)
=== RUN   TestQueryResultCol_ExtractValue
=== RUN   TestQueryResultCol_ExtractValue/0
=== RUN   TestQueryResultCol_ExtractValue/1
=== RUN   TestQueryResultCol_ExtractValue/2
=== RUN   TestQueryResultCol_ExtractValue/3
=== RUN   TestQueryResultCol_ExtractValue/4
=== RUN   TestQueryResultCol_ExtractValue/5
=== RUN   TestQueryResultCol_ExtractValue/6
=== RUN   TestQueryResultCol_ExtractValue/7
=== RUN   TestQueryResultCol_ExtractValue/8
=== RUN   TestQueryResultCol_ExtractValue/9
=== RUN   TestQueryResultCol_ExtractValue/10
=== RUN   TestQueryResultCol_ExtractValue/11
=== RUN   TestQueryResultCol_ExtractValue/12
=== RUN   TestQueryResultCol_ExtractValue/13
=== RUN   TestQueryResultCol_ExtractValue/14
=== RUN   TestQueryResultCol_ExtractValue/15
=== RUN   TestQueryResultCol_ExtractValue/16
=== RUN   TestQueryResultCol_ExtractValue/17
=== RUN   TestQueryResultCol_ExtractValue/18
--- PASS: TestQueryResultCol_ExtractValue (0.00s)
--- PASS: TestQueryResultCol_ExtractValue/0 (0.00s)
--- PASS: TestQueryResultCol_ExtractValue/1 (0.00s)
--- PASS: TestQueryResultCol_ExtractValue/2 (0.00s)
--- PASS: TestQueryResultCol_ExtractValue/3 (0.00s)
--- PASS: TestQueryResultCol_ExtractValue/4 (0.00s)
--- PASS: TestQueryResultCol_ExtractValue/5 (0.00s)
--- PASS: TestQueryResultCol_ExtractValue/6 (0.00s)
--- PASS: TestQueryResultCol_ExtractValue/7 (0.00s)
--- PASS: TestQueryResultCol_ExtractValue/8 (0.00s)
--- PASS: TestQueryResultCol_ExtractValue/9 (0.00s)
--- PASS: TestQueryResultCol_ExtractValue/10 (0.00s)
--- PASS: TestQueryResultCol_ExtractValue/11 (0.00s)
--- PASS: TestQueryResultCol_ExtractValue/12 (0.00s)
--- PASS: TestQueryResultCol_ExtractValue/13 (0.00s)
--- PASS: TestQueryResultCol_ExtractValue/14 (0.00s)
--- PASS: TestQueryResultCol_ExtractValue/15 (0.00s)
--- PASS: TestQueryResultCol_ExtractValue/16 (0.00s)
--- PASS: TestQueryResultCol_ExtractValue/17 (0.00s)
--- PASS: TestQueryResultCol_ExtractValue/18 (0.00s)
PASS
ok  	github.com/QuesmaOrg/quesma/quesma/model	1.026s
=== RUN   TestTranslateSqlResponseToJson
--- PASS: TestTranslateSqlResponseToJson (0.00s)
PASS
ok  	github.com/QuesmaOrg/quesma/quesma/model/bucket_aggregations	1.013s
=== RUN   Test_processResult
=== RUN   Test_processResult/testing_processResult0
Feb 10 13:50:22.000 WRN unexpected type in percentile array: <nil>, value: <nil>
Feb 10 13:50:22.000 WRN empty percentile values for not-important
=== RUN   Test_processResult/testing_processResult1
Feb 10 13:50:22.000 WRN unexpected type in percentile array: string, value:
Feb 10 13:50:22.000 WRN empty percentile values for not-important
=== RUN   Test_processResult/testing_processResult2
Feb 10 13:50:22.000 WRN unexpected type in percentile array: string, value: 0
Feb 10 13:50:22.000 WRN empty percentile values for not-important
=== RUN   Test_processResult/testing_processResult3
Feb 10 13:50:22.000 WRN unexpected type in percentile array: int, value: 0
Feb 10 13:50:22.000 WRN empty percentile values for not-important
=== RUN   Test_processResult/testing_processResult4
Feb 10 13:50:22.000 WRN unexpected type in percentile array: float64, value: 0
Feb 10 13:50:22.000 WRN empty percentile values for not-important
=== RUN   Test_processResult/testing_processResult5
Feb 10 13:50:22.000 WRN unexpected type in percentile array: []string, value: [1.0]
Feb 10 13:50:22.000 WRN empty percentile values for not-important
=== RUN   Test_processResult/testing_processResult6
Feb 10 13:50:22.000 WRN unexpected type in percentile array: []string, value: [1.0 5]
Feb 10 13:50:22.000 WRN empty percentile values for not-important
=== RUN   Test_processResult/testing_processResult7
Feb 10 13:50:22.000 WRN unexpected type in percentile array: string, array: [1.0 5]
=== RUN   Test_processResult/testing_processResult8
Feb 10 13:50:22.000 WRN unexpected type in percentile array: string, array: [1.0 5]
=== RUN   Test_processResult/testing_processResult9
Feb 10 13:50:22.000 WRN unexpected type in percentile array: []int, value: [1]
Feb 10 13:50:22.000 WRN empty percentile values for not-important
=== RUN   Test_processResult/testing_processResult10
Feb 10 13:50:22.000 WRN unexpected type in percentile array: []int, value: []
Feb 10 13:50:22.000 WRN empty percentile values for not-important
=== RUN   Test_processResult/testing_processResult11
Feb 10 13:50:22.000 WRN empty percentile values for not-important
=== RUN   Test_processResult/testing_processResult12
=== RUN   Test_processResult/testing_processResult13
=== RUN   Test_processResult/testing_processResult14
=== RUN   Test_processResult/testing_processResult15
Feb 10 13:50:22.000 WRN unexpected type in percentile array: int, array: [5 1]
=== RUN   Test_processResult/testing_processResult16
=== RUN   Test_processResult/testing_processResult17
=== RUN   Test_processResult/testing_processResult18
=== RUN   Test_processResult/testing_processResult19
--- PASS: Test_processResult (0.01s)
--- PASS: Test_processResult/testing_processResult0 (0.00s)
--- PASS: Test_processResult/testing_processResult1 (0.00s)
--- PASS: Test_processResult/testing_processResult2 (0.00s)
--- PASS: Test_processResult/testing_processResult3 (0.00s)
--- PASS: Test_processResult/testing_processResult4 (0.00s)
--- PASS: Test_processResult/testing_processResult5 (0.00s)
--- PASS: Test_processResult/testing_processResult6 (0.00s)
--- PASS: Test_processResult/testing_processResult7 (0.00s)
--- PASS: Test_processResult/testing_processResult8 (0.00s)
--- PASS: Test_processResult/testing_processResult9 (0.00s)
--- PASS: Test_processResult/testing_processResult10 (0.00s)
--- PASS: Test_processResult/testing_processResult11 (0.00s)
--- PASS: Test_processResult/testing_processResult12 (0.00s)
--- PASS: Test_processResult/testing_processResult13 (0.00s)
--- PASS: Test_processResult/testing_processResult14 (0.00s)
--- PASS: Test_processResult/testing_processResult15 (0.00s)
--- PASS: Test_processResult/testing_processResult16 (0.00s)
--- PASS: Test_processResult/testing_processResult17 (0.00s)
--- PASS: Test_processResult/testing_processResult18 (0.00s)
--- PASS: Test_processResult/testing_processResult19 (0.00s)
PASS
ok  	github.com/QuesmaOrg/quesma/quesma/model/metrics_aggregations	1.026s
=== RUN   Test_cacheQueries
=== RUN   Test_cacheQueries/select_all
Feb 10 13:50:23.000 DBG Query not eligible for time range optimization: LIMIT 0
=== RUN   Test_cacheQueries/select_a,_count()_from_foo__group_by_1
Feb 10 13:50:23.000 DBG Query not eligible for time range optimization: LIMIT 0
--- PASS: Test_cacheQueries (0.00s)
--- PASS: Test_cacheQueries/select_all (0.00s)
--- PASS: Test_cacheQueries/select_a,_count()_from_foo__group_by_1 (0.00s)
=== RUN   Test_dateTrunc
=== RUN   Test_dateTrunc/select_all
Feb 10 13:50:23.000 DBG Query not eligible for time range optimization: LIMIT 0
=== RUN   Test_dateTrunc/select_all_where_date_
Feb 10 13:50:23.000 DBG Query not eligible for time range optimization: LIMIT 0
=== RUN   Test_dateTrunc/select_all_where_and_between_dates_(>24h)
Feb 10 13:50:23.000 DBG Query not eligible for time range optimization: LIMIT 0
=== RUN   Test_dateTrunc/select_all_where_and_between_dates_(<24h)
Feb 10 13:50:23.000 DBG Query not eligible for time range optimization: LIMIT 0
=== RUN   Test_dateTrunc/select_a,_count()_from_foo__group_by_1
Feb 10 13:50:23.000 DBG Query not eligible for time range optimization: LIMIT 0
=== RUN   Test_dateTrunc/select_all_where_and_between_dates_(>24h),_disabled_index_
Feb 10 13:50:23.000 DBG Query not eligible for time range optimization: LIMIT 0
--- PASS: Test_dateTrunc (0.00s)
--- PASS: Test_dateTrunc/select_all (0.00s)
--- PASS: Test_dateTrunc/select_all_where_date_ (0.00s)
--- PASS: Test_dateTrunc/select_all_where_and_between_dates_(>24h) (0.00s)
--- PASS: Test_dateTrunc/select_all_where_and_between_dates_(<24h) (0.00s)
--- PASS: Test_dateTrunc/select_a,_count()_from_foo__group_by_1 (0.00s)
--- PASS: Test_dateTrunc/select_all_where_and_between_dates_(>24h),_disabled_index_ (0.00s)
=== RUN   Test_materialized_view_replace
=== RUN   Test_materialized_view_replace/select_all_where_date_
Feb 10 13:50:23.000 DBG Query not eligible for time range optimization: LIMIT 0
=== RUN   Test_materialized_view_replace/select_all_with_condition_at_top_level
Feb 10 13:50:23.000 INF materialized_view_replace triggered, input query: SELECT "*" FROM foo WHERE "a">10
Feb 10 13:50:23.000 INF materialized_view_replace triggered, output query: SELECT "*" FROM foo_view WHERE TRUE
Feb 10 13:50:23.000 DBG Query not eligible for time range optimization: LIMIT 0
=== RUN   Test_materialized_view_replace/select_all_with_condition_2
Feb 10 13:50:23.000 INF materialized_view_replace triggered, input query: SELECT "*" FROM foo WHERE "c"<1and"a">10
Feb 10 13:50:23.000 INF materialized_view_replace triggered, output query: SELECT "*" FROM foo_view WHERE "c"<1andTRUE
Feb 10 13:50:23.000 DBG Query not eligible for time range optimization: LIMIT 0
=== RUN   Test_materialized_view_replace/select_all_with_condition_3
Feb 10 13:50:23.000 INF materialized_view_replace triggered, input query: SELECT "*" FROM foo WHERE "a">10and"c"<1and"a">10
Feb 10 13:50:23.000 INF materialized_view_replace triggered, output query: SELECT "*" FROM foo_view WHERE TRUEand"c"<1andTRUE
Feb 10 13:50:23.000 DBG Query not eligible for time range optimization: LIMIT 0
=== RUN   Test_materialized_view_replace/select_all_with_condition_4
Feb 10 13:50:23.000 INF materialized_view_replace triggered, input query: SELECT "*" FROM foo WHERE "a">10and"a">10and"c"<1and"a">10
Feb 10 13:50:23.000 INF materialized_view_replace triggered, output query: SELECT "*" FROM foo_view WHERE TRUEandTRUEand"c"<1andTRUE
Feb 10 13:50:23.000 DBG Query not eligible for time range optimization: LIMIT 0
=== RUN   Test_materialized_view_replace/select_all_without_condition
Feb 10 13:50:23.000 DBG Query not eligible for time range optimization: LIMIT 0
=== RUN   Test_materialized_view_replace/select_all_from_other_table_with_condition_at_top_level
Feb 10 13:50:23.000 DBG Query not eligible for time range optimization: LIMIT 0
=== RUN   Test_materialized_view_replace/select_all_OR
Feb 10 13:50:23.000 DBG Query not eligible for time range optimization: LIMIT 0
=== RUN   Test_materialized_view_replace/select_all_NOT
Feb 10 13:50:23.000 DBG Query not eligible for time range optimization: LIMIT 0
=== RUN   Test_materialized_view_replace/select_all_NOT2
Feb 10 13:50:23.000 DBG Query not eligible for time range optimization: LIMIT 0
--- PASS: Test_materialized_view_replace (0.01s)
--- PASS: Test_materialized_view_replace/select_all_where_date_ (0.00s)
--- PASS: Test_materialized_view_replace/select_all_with_condition_at_top_level (0.00s)
--- PASS: Test_materialized_view_replace/select_all_with_condition_2 (0.00s)
--- PASS: Test_materialized_view_replace/select_all_with_condition_3 (0.00s)
--- PASS: Test_materialized_view_replace/select_all_with_condition_4 (0.00s)
--- PASS: Test_materialized_view_replace/select_all_without_condition (0.00s)
--- PASS: Test_materialized_view_replace/select_all_from_other_table_with_condition_at_top_level (0.00s)
--- PASS: Test_materialized_view_replace/select_all_OR (0.00s)
--- PASS: Test_materialized_view_replace/select_all_NOT (0.00s)
--- PASS: Test_materialized_view_replace/select_all_NOT2 (0.00s)
PASS
ok  	github.com/QuesmaOrg/quesma/quesma/optimize	1.024s
=== RUN   TestExpectDate
--- PASS: TestExpectDate (0.00s)
=== RUN   TestPainless
=== RUN   TestPainless/simple_addition
=== RUN   TestPainless/concat
=== RUN   TestPainless/concat_strings
=== RUN   TestPainless/concat_date_literal_and_string
=== RUN   TestPainless/get_hour_from_date
=== RUN   TestPainless/format_date_with_ISO
=== RUN   TestPainless/url-encode
--- PASS: TestPainless (0.01s)
--- PASS: TestPainless/simple_addition (0.00s)
--- PASS: TestPainless/concat (0.00s)
--- PASS: TestPainless/concat_strings (0.00s)
--- PASS: TestPainless/concat_date_literal_and_string (0.00s)
--- PASS: TestPainless/get_hour_from_date (0.00s)
--- PASS: TestPainless/format_date_with_ISO (0.00s)
--- PASS: TestPainless/url-encode (0.00s)
PASS
ok  	github.com/QuesmaOrg/quesma/quesma/painful	1.019s
=== RUN   TestNewElasticPersistence
--- PASS: TestNewElasticPersistence (0.00s)
PASS
ok  	github.com/QuesmaOrg/quesma/quesma/persistence	1.020s
=== RUN   Test3AggregationParserNewLogic
aggregation_parser_new_logic_test.go:22: Skip for now. Wait for a new implementation.
--- SKIP: Test3AggregationParserNewLogic (0.00s)
=== RUN   Test_quoteArray
--- PASS: Test_quoteArray (0.00s)
=== RUN   Test_parseFieldFromScriptField
Feb 10 13:50:26.000 WRN source not found in script: map[]
Feb 10 13:50:26.000 WRN source is not a string, but <nil>, value: <nil>
Feb 10 13:50:26.000 WRN script is not a JsonMap, but string, value: script
Feb 10 13:50:26.000 WRN source is not a string, but int, value: 1
--- PASS: Test_parseFieldFromScriptField (0.00s)
=== RUN   Test_parsePercentilesAggregationWithDefaultPercents
--- PASS: Test_parsePercentilesAggregationWithDefaultPercents (0.00s)
=== RUN   Test_parsePercentilesAggregationWithUserSpecifiedPercents
--- PASS: Test_parsePercentilesAggregationWithUserSpecifiedPercents (0.00s)
=== RUN   Test_parsePercentilesAggregationKeyed
Feb 10 13:50:26.000 DBG field 'custom_name' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'custom_name' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'custom_name' referenced, but not found in schema, falling back to original name
--- PASS: Test_parsePercentilesAggregationKeyed (0.00s)
=== RUN   TestParseDateMathExpression
=== RUN   TestParseDateMathExpression/now
=== RUN   TestParseDateMathExpression/now-15m
=== RUN   TestParseDateMathExpression/now-15m-25s
=== RUN   TestParseDateMathExpression/now-15m-25s/y
=== RUN   TestParseDateMathExpression/now-15m-25s/y#01
--- PASS: TestParseDateMathExpression (0.00s)
--- PASS: TestParseDateMathExpression/now (0.00s)
--- PASS: TestParseDateMathExpression/now-15m (0.00s)
--- PASS: TestParseDateMathExpression/now-15m-25s (0.00s)
--- PASS: TestParseDateMathExpression/now-15m-25s/y (0.00s)
--- PASS: TestParseDateMathExpression/now-15m-25s/y#01 (0.00s)
=== RUN   Test_parseDateTimeInClickhouseMathLanguage
=== RUN   Test_parseDateTimeInClickhouseMathLanguage/now-15m
=== RUN   Test_parseDateTimeInClickhouseMathLanguage/now-15m+5s
=== RUN   Test_parseDateTimeInClickhouseMathLanguage/now-
=== RUN   Test_parseDateTimeInClickhouseMathLanguage/now-15m+/M
=== RUN   Test_parseDateTimeInClickhouseMathLanguage/now-15m/d
=== RUN   Test_parseDateTimeInClickhouseMathLanguage/now-15m+5s/w
=== RUN   Test_parseDateTimeInClickhouseMathLanguage/now-/Y
=== RUN   Test_parseDateTimeInClickhouseMathLanguage/now
--- PASS: Test_parseDateTimeInClickhouseMathLanguage (0.00s)
--- PASS: Test_parseDateTimeInClickhouseMathLanguage/now-15m (0.00s)
--- PASS: Test_parseDateTimeInClickhouseMathLanguage/now-15m+5s (0.00s)
--- PASS: Test_parseDateTimeInClickhouseMathLanguage/now- (0.00s)
--- PASS: Test_parseDateTimeInClickhouseMathLanguage/now-15m+/M (0.00s)
--- PASS: Test_parseDateTimeInClickhouseMathLanguage/now-15m/d (0.00s)
--- PASS: Test_parseDateTimeInClickhouseMathLanguage/now-15m+5s/w (0.00s)
--- PASS: Test_parseDateTimeInClickhouseMathLanguage/now-/Y (0.00s)
--- PASS: Test_parseDateTimeInClickhouseMathLanguage/now (0.00s)
=== RUN   Test_DateMathExpressionAsLiteral
=== RUN   Test_DateMathExpressionAsLiteral/now
=== RUN   Test_DateMathExpressionAsLiteral/now-15m
=== RUN   Test_DateMathExpressionAsLiteral/now-15m+5s
=== RUN   Test_DateMathExpressionAsLiteral/now-
=== RUN   Test_DateMathExpressionAsLiteral/now-15m+/M
=== RUN   Test_DateMathExpressionAsLiteral/now-15m/d
=== RUN   Test_DateMathExpressionAsLiteral/now-15m+5s/w
=== RUN   Test_DateMathExpressionAsLiteral/now-/Y
=== RUN   Test_DateMathExpressionAsLiteral/now-2M
=== RUN   Test_DateMathExpressionAsLiteral/now-1y
=== RUN   Test_DateMathExpressionAsLiteral/now-1w
=== RUN   Test_DateMathExpressionAsLiteral/now-1s
=== RUN   Test_DateMathExpressionAsLiteral/now-1m
=== RUN   Test_DateMathExpressionAsLiteral/now-1d
--- PASS: Test_DateMathExpressionAsLiteral (0.00s)
--- PASS: Test_DateMathExpressionAsLiteral/now (0.00s)
--- PASS: Test_DateMathExpressionAsLiteral/now-15m (0.00s)
--- PASS: Test_DateMathExpressionAsLiteral/now-15m+5s (0.00s)
--- PASS: Test_DateMathExpressionAsLiteral/now- (0.00s)
--- PASS: Test_DateMathExpressionAsLiteral/now-15m+/M (0.00s)
--- PASS: Test_DateMathExpressionAsLiteral/now-15m/d (0.00s)
--- PASS: Test_DateMathExpressionAsLiteral/now-15m+5s/w (0.00s)
--- PASS: Test_DateMathExpressionAsLiteral/now-/Y (0.00s)
--- PASS: Test_DateMathExpressionAsLiteral/now-2M (0.00s)
--- PASS: Test_DateMathExpressionAsLiteral/now-1y (0.00s)
--- PASS: Test_DateMathExpressionAsLiteral/now-1w (0.00s)
--- PASS: Test_DateMathExpressionAsLiteral/now-1s (0.00s)
--- PASS: Test_DateMathExpressionAsLiteral/now-1m (0.00s)
--- PASS: Test_DateMathExpressionAsLiteral/now-1d (0.00s)
=== RUN   TestDateManager_parseStrictDateOptionalTimeOrEpochMillis
=== RUN   TestDateManager_parseStrictDateOptionalTimeOrEpochMillis/<nil>
=== RUN   TestDateManager_parseStrictDateOptionalTimeOrEpochMillis/2024
=== RUN   TestDateManager_parseStrictDateOptionalTimeOrEpochMillis/123
=== RUN   TestDateManager_parseStrictDateOptionalTimeOrEpochMillis/4234324223
=== RUN   TestDateManager_parseStrictDateOptionalTimeOrEpochMillis/4234
=== RUN   TestDateManager_parseStrictDateOptionalTimeOrEpochMillis/42340
=== RUN   TestDateManager_parseStrictDateOptionalTimeOrEpochMillis/42340.234
=== RUN   TestDateManager_parseStrictDateOptionalTimeOrEpochMillis/2024/02
=== RUN   TestDateManager_parseStrictDateOptionalTimeOrEpochMillis/2024-02
=== RUN   TestDateManager_parseStrictDateOptionalTimeOrEpochMillis/2024-2
=== RUN   TestDateManager_parseStrictDateOptionalTimeOrEpochMillis/2024-02-02
=== RUN   TestDateManager_parseStrictDateOptionalTimeOrEpochMillis/2024-02-3
=== RUN   TestDateManager_parseStrictDateOptionalTimeOrEpochMillis/2024-02-30
=== RUN   TestDateManager_parseStrictDateOptionalTimeOrEpochMillis/2024-02-25T1
=== RUN   TestDateManager_parseStrictDateOptionalTimeOrEpochMillis/2024-02-25T13:00:00
=== RUN   TestDateManager_parseStrictDateOptionalTimeOrEpochMillis/2024-02-25_13:00:00
=== RUN   TestDateManager_parseStrictDateOptionalTimeOrEpochMillis/2024-02-25T13:11
=== RUN   TestDateManager_parseStrictDateOptionalTimeOrEpochMillis/2024-02-25T25:00:00
=== RUN   TestDateManager_parseStrictDateOptionalTimeOrEpochMillis/2024-02-25T13:00:00+05
=== RUN   TestDateManager_parseStrictDateOptionalTimeOrEpochMillis/2024-02-25T13:00:00+05:00
--- PASS: TestDateManager_parseStrictDateOptionalTimeOrEpochMillis (0.00s)
--- PASS: TestDateManager_parseStrictDateOptionalTimeOrEpochMillis/<nil> (0.00s)
--- PASS: TestDateManager_parseStrictDateOptionalTimeOrEpochMillis/2024 (0.00s)
--- PASS: TestDateManager_parseStrictDateOptionalTimeOrEpochMillis/123 (0.00s)
--- PASS: TestDateManager_parseStrictDateOptionalTimeOrEpochMillis/4234324223 (0.00s)
--- PASS: TestDateManager_parseStrictDateOptionalTimeOrEpochMillis/4234 (0.00s)
--- PASS: TestDateManager_parseStrictDateOptionalTimeOrEpochMillis/42340 (0.00s)
--- PASS: TestDateManager_parseStrictDateOptionalTimeOrEpochMillis/42340.234 (0.00s)
--- PASS: TestDateManager_parseStrictDateOptionalTimeOrEpochMillis/2024/02 (0.00s)
--- PASS: TestDateManager_parseStrictDateOptionalTimeOrEpochMillis/2024-02 (0.00s)
--- PASS: TestDateManager_parseStrictDateOptionalTimeOrEpochMillis/2024-2 (0.00s)
--- PASS: TestDateManager_parseStrictDateOptionalTimeOrEpochMillis/2024-02-02 (0.00s)
--- PASS: TestDateManager_parseStrictDateOptionalTimeOrEpochMillis/2024-02-3 (0.00s)
--- PASS: TestDateManager_parseStrictDateOptionalTimeOrEpochMillis/2024-02-30 (0.00s)
--- PASS: TestDateManager_parseStrictDateOptionalTimeOrEpochMillis/2024-02-25T1 (0.00s)
--- PASS: TestDateManager_parseStrictDateOptionalTimeOrEpochMillis/2024-02-25T13:00:00 (0.00s)
--- PASS: TestDateManager_parseStrictDateOptionalTimeOrEpochMillis/2024-02-25_13:00:00 (0.00s)
--- PASS: TestDateManager_parseStrictDateOptionalTimeOrEpochMillis/2024-02-25T13:11 (0.00s)
--- PASS: TestDateManager_parseStrictDateOptionalTimeOrEpochMillis/2024-02-25T25:00:00 (0.00s)
--- PASS: TestDateManager_parseStrictDateOptionalTimeOrEpochMillis/2024-02-25T13:00:00+05 (0.00s)
--- PASS: TestDateManager_parseStrictDateOptionalTimeOrEpochMillis/2024-02-25T13:00:00+05:00 (0.00s)
=== RUN   TestPancakeQueryGeneration
=== RUN   TestPancakeQueryGeneration/simple_max/min_aggregation_as_2_siblings(file:agg_req,nr:0)(0)
i: 0 test: simple max/min aggregation as 2 siblings(file:agg_req,nr:0)
Feb 10 13:50:26.000 DBG field 'timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'AvgTicketPrice' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'AvgTicketPrice' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'AvgTicketPrice' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'AvgTicketPrice' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/2_sibling_count_aggregations(file:agg_req,nr:1)(1)
i: 1 test: 2 sibling count aggregations(file:agg_req,nr:1)
Feb 10 13:50:26.000 DBG field 'timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'OriginCityName' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'FlightDelay' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'Cancelled' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/date_histogram_+_size_as_string(file:agg_req,nr:2)(2)
i: 2 test: date_histogram + size as string(file:agg_req,nr:2)
Feb 10 13:50:26.000 DBG field 'timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 WRN we didn't expect shard_size in Terms params map[field:FlightDelayType order:map[_count:desc] shard_size:25 size:10]
Feb 10 13:50:26.000 DBG field 'FlightDelayType' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'timestamp' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/Sum(file:agg_req,nr:3)(3)
i: 3 test: Sum(file:agg_req,nr:3)
Feb 10 13:50:26.000 DBG field 'order_date' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'order_date' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'taxful_total_price' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'taxful_total_price' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/cardinality(file:agg_req,nr:4)(4)
i: 4 test: cardinality(file:agg_req,nr:4)
Feb 10 13:50:26.000 DBG field 'timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'OriginCityName' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'OriginCityName' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'OriginCityName' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/simple_filter/count(file:agg_req,nr:5)(5)
i: 5 test: simple filter/count(file:agg_req,nr:5)
Feb 10 13:50:26.000 DBG field 'timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'FlightDelay' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/filters(file:agg_req,nr:6)(6)
i: 6 test: filters(file:agg_req,nr:6)
Feb 10 13:50:26.000 DBG field 'FlightDelay' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'timestamp' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/top_hits,_quite_complex(file:agg_req,nr:7)(7)
i: 7 test: top hits, quite complex(file:agg_req,nr:7)
Feb 10 13:50:26.000 DBG field 'OriginAirportID' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'DestAirportID' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 ERR failed to merge aggregations: 1 error occurred:
* mergeAny: i1 isn't neither JsonMap nor []JsonMap, i1 type: int64, i2 type: int64, i1: 25, i2: 15


=== RUN   TestPancakeQueryGeneration/histogram,_different_field_than_timestamp(file:agg_req,nr:8)(8)
i: 8 test: histogram, different field than timestamp(file:agg_req,nr:8)
Feb 10 13:50:26.000 DBG field 'timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'FlightDelayMin' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'FlightDelayMin' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/double_aggregation_with_histogram_+_harder_query(file:agg_req,nr:9)(9)
i: 9 test: double aggregation with histogram + harder query(file:agg_req,nr:9)
Feb 10 13:50:26.000 DBG field 'host.name' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field '@timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field '@timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'severity' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field '@timestamp' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/very_long:_multiple_top_metrics_+_histogram(file:agg_req,nr:10)(10)
i: 10 test: very long: multiple top_metrics + histogram(file:agg_req,nr:10)
Feb 10 13:50:26.000 DBG field 'order_date' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'order_date' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'order_date' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/value_count_+_top_values:_regression_test(file:agg_req,nr:11)(11)
i: 11 test: value_count + top_values: regression test(file:agg_req,nr:11)
Feb 10 13:50:26.000 DBG field '@timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field '@timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'host.name' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'host.name' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'host.name' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/date_histogram:_regression_test(file:agg_req,nr:12)(12)
i: 12 test: date_histogram: regression test(file:agg_req,nr:12)
Feb 10 13:50:26.000 DBG field '@timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field '@timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field '@timestamp' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/terms_with_date_histogram_as_subaggregation:_regression_test(file:agg_req,nr:13)(13)
i: 13 test: terms with date_histogram as subaggregation: regression test(file:agg_req,nr:13)
Feb 10 13:50:26.000 DBG field '@timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field '@timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'event.dataset' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field '@timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 WRN min_doc_count is not 0 or 1, but 12. Not really supported
=== RUN   TestPancakeQueryGeneration/earliest/latest_timestamp:_regression_test(file:agg_req,nr:14)(14)
i: 14 test: earliest/latest timestamp: regression test(file:agg_req,nr:14)
Feb 10 13:50:26.000 DBG field 'message' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'host.name' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field '@timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field '@timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field '@timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field '@timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field '@timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field '@timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 WRN could not parse date <nil>
Feb 10 13:50:26.000 WRN could not parse date <nil>
Feb 10 13:50:26.000 WRN could not parse date <nil>
=== RUN   TestPancakeQueryGeneration/date_histogram:_regression_test(file:agg_req,nr:15)(15)
i: 15 test: date_histogram: regression test(file:agg_req,nr:15)
Feb 10 13:50:26.000 DBG field 'order_date' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'order_date' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'order_date' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'taxful_total_price' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'taxful_total_price' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/simple_terms,_seen_at_client's(file:agg_req,nr:16)(16)
i: 16 test: simple terms, seen at client's(file:agg_req,nr:16)
Feb 10 13:50:26.000 DBG field 'timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'message' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/triple_nested_aggs(file:agg_req,nr:17)(17)
i: 17 test: triple nested aggs(file:agg_req,nr:17)
Feb 10 13:50:26.000 DBG field 'order_date' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'order_date' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'order_date' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'taxful_total_price' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'taxful_total_price' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 WRN no rows returned for filter aggregation
Feb 10 13:50:26.000 WRN no rows returned for filter aggregation
=== RUN   TestPancakeQueryGeneration/complex_filters(file:agg_req,nr:18)(18)
i: 18 test: complex filters(file:agg_req,nr:18)
Feb 10 13:50:26.000 DBG field 'order_date' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'order_date' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'order_date' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'order_date' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'order_date' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'order_date' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'order_date' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'order_date' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'order_date' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'taxful_total_price' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'taxful_total_price' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'taxful_total_price' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'taxful_total_price' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/random_sampler,_from_Explorer_>_Field_statistics(file:agg_req,nr:19)(19)
i: 19 test: random sampler, from Explorer > Field statistics(file:agg_req,nr:19)
Feb 10 13:50:26.000 DBG field '@timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field '@timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 WRN seed is not an float64, but string, value: 1225474982. Using default: 0
Feb 10 13:50:26.000 DBG field '@timestamp' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/Field_statistics_>_summary_for_numeric_fields(file:agg_req,nr:20)(20)
i: 20 test: Field statistics > summary for numeric fields(file:agg_req,nr:20)
Feb 10 13:50:26.000 DBG field 'timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'bytes_gauge' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'bytes_gauge' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'bytes_gauge' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'bytes_gauge' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'bytes_gauge' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'bytes_gauge' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'bytes_gauge' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/range_bucket_aggregation,_both_keyed_and_not(file:agg_req,nr:21)(21)
i: 21 test: range bucket aggregation, both keyed and not(file:agg_req,nr:21)
Feb 10 13:50:26.000 DBG field 'timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'bytes_gauge' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'bytes_gauge' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/date_range_aggregation(file:agg_req,nr:22)(22)
i: 22 test: date_range aggregation(file:agg_req,nr:22)
Feb 10 13:50:26.000 DBG field 'timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'timestamp' referenced, but not found in schema, falling back to original name
pancake_sql_query_generation_test.go:115: We don't have result yet
=== RUN   TestPancakeQueryGeneration/significant_terms_aggregation:_same_as_terms_for_now(file:agg_req,nr:23)(23)
i: 23 test: significant terms aggregation: same as terms for now(file:agg_req,nr:23)
Feb 10 13:50:26.000 DBG field 'message' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/meta_field_in_aggregation(file:agg_req,nr:24)(24)
i: 24 test: meta field in aggregation(file:agg_req,nr:24)
Feb 10 13:50:26.000 DBG field '@timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'host.name' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'host.name' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/simple_histogram,_but_min_doc_count:_0(file:agg_req,nr:25)(25)
i: 25 test: simple histogram, but min_doc_count: 0(file:agg_req,nr:25)
Feb 10 13:50:26.000 DBG field 'timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'bytes' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/simple_date_histogram,_but_min_doc_count:_0(file:agg_req,nr:26)(26)
i: 26 test: simple date_histogram, but min_doc_count: 0(file:agg_req,nr:26)
Feb 10 13:50:26.000 DBG field 'timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'timestamp' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/simple_date_histogram,_but_min_doc_count:_0(file:agg_req,nr:27)(27)
i: 27 test: simple date_histogram, but min_doc_count: 0(file:agg_req,nr:27)
Feb 10 13:50:26.000 DBG field 'rspContentLen' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'message' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/Terms,_completely_different_tree_results_from_2_queries_-_merging_them_didn't_work_before(file:agg_req,nr:28)(28)
i: 28 test: Terms, completely different tree results from 2 queries - merging them didn't work before(file:agg_req,nr:28)
Feb 10 13:50:26.000 DBG field 'OriginCityName' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'Cancelled' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'FlightDelay' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/Terms,_completely_different_tree_results_from_2_queries_-_merging_them_didn't_work_before_(logs)_TODO_add_results(file:agg_req,nr:29)(29)
i: 29 test: Terms, completely different tree results from 2 queries - merging them didn't work before (logs) TODO add results(file:agg_req,nr:29)
Feb 10 13:50:26.000 DBG field 'geo.src' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'memory' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'memory' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'machine.os' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'memory' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'memory' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/Terms,_completely_different_tree_results_from_2_queries_-_merging_them_didn't_work_before_(logs)._what_when_cardinality_=_0?(file:agg_req,nr:30)(30)
i: 30 test: Terms, completely different tree results from 2 queries - merging them didn't work before (logs). what when cardinality = 0?(file:agg_req,nr:30)
Feb 10 13:50:26.000 DBG field 'timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'machine.os' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'clientip' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'clientip' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/Kibana_Visualize_->_Last_Value._Used_to_panic(file:agg_req,nr:31)(31)
i: 31 test: Kibana Visualize -> Last Value. Used to panic(file:agg_req,nr:31)
Feb 10 13:50:26.000 DBG field '@timestamp' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/Standard_deviation(file:agg_req,nr:32)(32)
i: 32 test: Standard deviation(file:agg_req,nr:32)
Feb 10 13:50:26.000 DBG field 'timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'bytes' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'bytes' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/0_result_rows_in_2x_terms(file:agg_req,nr:33)(33)
i: 33 test: 0 result rows in 2x terms(file:agg_req,nr:33)
Feb 10 13:50:26.000 DBG field 'message' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'host.name' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'message' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/0_result_rows_in_3x_terms(file:agg_req,nr:34)(34)
i: 34 test: 0 result rows in 3x terms(file:agg_req,nr:34)
Feb 10 13:50:26.000 DBG field 'message' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'host.name' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'message' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'message' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/0_result_rows_in_terms+histogram(file:agg_req,nr:35)(35)
i: 35 test: 0 result rows in terms+histogram(file:agg_req,nr:35)
Feb 10 13:50:26.000 DBG field 'message' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'host.name' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'FlightDelayMin' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/0_result_rows_in_terms+histogram_+_meta_field(file:agg_req,nr:36)(36)
i: 36 test: 0 result rows in terms+histogram + meta field(file:agg_req,nr:36)
Feb 10 13:50:26.000 DBG field 'message' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'host.name' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'FlightDelayMin' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/0_result_rows_in_terms+histogram_+_meta_field,_meta_in_subaggregation(file:agg_req,nr:37)(37)
i: 37 test: 0 result rows in terms+histogram + meta field, meta in subaggregation(file:agg_req,nr:37)
Feb 10 13:50:26.000 DBG field 'message' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'host.name' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'FlightDelayMin' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/simplest_top_metrics,_no_sort(file:agg_req,nr:38)(38)
i: 38 test: simplest top_metrics, no sort(file:agg_req,nr:38)
Feb 10 13:50:26.000 WRN no sort field found in top_metrics query
Feb 10 13:50:26.000 WRN invalid sort order: , defaulting to desc
Feb 10 13:50:26.000 WRN no sort field found in top_metrics query
Feb 10 13:50:26.000 WRN invalid sort order: , defaulting to desc
Feb 10 13:50:26.000 WRN no columns returned for top_metrics aggregation, len(rows[0].Cols): 1, len(rows): 1
=== RUN   TestPancakeQueryGeneration/simplest_top_metrics,_with_sort(file:agg_req,nr:39)(39)
i: 39 test: simplest top_metrics, with sort(file:agg_req,nr:39)
=== RUN   TestPancakeQueryGeneration/terms_ordered_by_subaggregation(file:agg_req,nr:40)(40)
i: 40 test: terms ordered by subaggregation(file:agg_req,nr:40)
Feb 10 13:50:26.000 DBG field 'abc' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'abc' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 WRN datetime field 'abc' not found in table '__quesma_table_name'
Feb 10 13:50:26.000 DBG field 'type' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'name' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'total' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'total' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/0_result_rows_in_2x_terms(file:agg_req,nr:41)(41)
i: 41 test: 0 result rows in 2x terms(file:agg_req,nr:41)
Feb 10 13:50:26.000 DBG field 'OriginAirportID' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'DestAirportID' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/histogram_with_all_possible_calendar_intervals(file:agg_req_2,nr:0)(42)
i: 42 test: histogram with all possible calendar_intervals(file:agg_req_2,nr:0)
Feb 10 13:50:26.000 DBG field '@timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field '@timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field '@timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field '@timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field '@timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field '@timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field '@timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field '@timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field '@timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field '@timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field '@timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field '@timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field '@timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field '@timestamp' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/Percentiles_with_another_metric_aggregation._It_might_get_buggy_after_introducing_pancakes.(file:agg_req_2,nr:1)(43)
i: 43 test: Percentiles with another metric aggregation. It might get buggy after introducing pancakes.(file:agg_req_2,nr:1)
Feb 10 13:50:26.000 DBG field 'timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'response' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'count' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'count' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/2x_terms_with_nulls_1/4,_nulls_in_second_aggregation,_with_missing_parameter(file:agg_req_2,nr:2)(44)
i: 44 test: 2x terms with nulls 1/4, nulls in second aggregation, with missing parameter(file:agg_req_2,nr:2)
Feb 10 13:50:26.000 DBG field 'surname' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'limbName' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/2x_terms_with_nulls_2/4,_nulls_in_the_second_aggregation,_but_no_missing_parameter(file:agg_req_2,nr:3)(45)
i: 45 test: 2x terms with nulls 2/4, nulls in the second aggregation, but no missing parameter(file:agg_req_2,nr:3)
Feb 10 13:50:26.000 DBG field 'surname' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'limbName' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/2x_terms_with_nulls_3/4,_nulls_in_the_first_aggregation,_with_missing_parameter(file:agg_req_2,nr:4)(46)
i: 46 test: 2x terms with nulls 3/4, nulls in the first aggregation, with missing parameter(file:agg_req_2,nr:4)
Feb 10 13:50:26.000 DBG field 'surname' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'limbName' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/2x_terms_with_nulls_4/4,_nulls_in_the_first_aggregation,_without_missing_parameter(file:agg_req_2,nr:5)(47)
i: 47 test: 2x terms with nulls 4/4, nulls in the first aggregation, without missing parameter(file:agg_req_2,nr:5)
Feb 10 13:50:26.000 DBG field 'surname' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'limbName' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/2x_date_histogram(file:agg_req_2,nr:6)(48)
i: 48 test: 2x date_histogram(file:agg_req_2,nr:6)
Feb 10 13:50:26.000 DBG field '@timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field '@timestamp' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/2x_histogram(file:agg_req_2,nr:7)(49)
i: 49 test: 2x histogram(file:agg_req_2,nr:7)
Feb 10 13:50:26.000 DBG field 'timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'bytes' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'bytes2' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/2x_histogram_with_min_doc_count_0(file:agg_req_2,nr:8)(50)
i: 50 test: 2x histogram with min_doc_count 0(file:agg_req_2,nr:8)
Feb 10 13:50:26.000 DBG field 'timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'bytes' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'bytes2' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/2x_terms_with_sampler_in_the_middle(file:agg_req_2,nr:9)(51)
i: 51 test: 2x terms with sampler in the middle(file:agg_req_2,nr:9)
Feb 10 13:50:26.000 DBG field 'surname' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'limbName' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/2x_terms_with_random_sampler_in_the_middle(file:agg_req_2,nr:10)(52)
i: 52 test: 2x terms with random_sampler in the middle(file:agg_req_2,nr:10)
Feb 10 13:50:26.000 DBG field 'surname' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 WRN seed is not an float64, but string, value: 1225474982. Using default: 0
Feb 10 13:50:26.000 DBG field 'limbName' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/terms_order_by_quantile,_simplest_-_only_one_percentile(file:agg_req_2,nr:11)(53)
i: 53 test: terms order by quantile, simplest - only one percentile(file:agg_req_2,nr:11)
Feb 10 13:50:26.000 DBG field 'data_stream.dataset' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field '@timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field '@timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field '@timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'container.name' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'docker.cpu.total.pct' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'docker.cpu.total.pct' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/terms_order_by_quantile_-_more_percentiles(file:agg_req_2,nr:12)(54)
i: 54 test: terms order by quantile - more percentiles(file:agg_req_2,nr:12)
Feb 10 13:50:26.000 DBG field '@timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'container.name' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'docker.cpu.total.pct' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'docker.cpu.total.pct' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/terms_order_by_percentile_ranks(file:agg_req_2,nr:13)(55)
i: 55 test: terms order by percentile_ranks(file:agg_req_2,nr:13)
Feb 10 13:50:26.000 DBG field 'Cancelled' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'DistanceKilometers' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/simple_histogram_with_null_values,_no_missing_parameter(file:agg_req_2,nr:14)(56)
i: 56 test: simple histogram with null values, no missing parameter(file:agg_req_2,nr:14)
Feb 10 13:50:26.000 DBG field 'taxful_total_price' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/histogram_with_null_values,_no_missing_parameter,_and_some_subaggregation(file:agg_req_2,nr:15)(57)
i: 57 test: histogram with null values, no missing parameter, and some subaggregation(file:agg_req_2,nr:15)
Feb 10 13:50:26.000 DBG field 'taxful_total_price' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'type' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/simple_histogram_with_null_values_and_missing_parameter(file:agg_req_2,nr:16)(58)
i: 58 test: simple histogram with null values and missing parameter(file:agg_req_2,nr:16)
Feb 10 13:50:26.000 DBG field 'taxful_total_price' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/histogram_with_null_values,_missing_parameter,_and_some_subaggregation(file:agg_req_2,nr:17)(59)
i: 59 test: histogram with null values, missing parameter, and some subaggregation(file:agg_req_2,nr:17)
Feb 10 13:50:26.000 DBG field 'taxful_total_price' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'type' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/simple_date_histogram_with_null_values,_no_missing_parameter_(DateTime)(file:agg_req_2,nr:18)(60)
i: 60 test: simple date_histogram with null values, no missing parameter (DateTime)(file:agg_req_2,nr:18)
Feb 10 13:50:26.000 DBG field 'customer_birth_date' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 WRN extractInterval: no interval found, returning default: 30s (fixed_interval)
=== RUN   TestPancakeQueryGeneration/date_histogram_with_null_values,_no_missing_parameter,_and_some_subaggregation(file:agg_req_2,nr:19)(61)
i: 61 test: date_histogram with null values, no missing parameter, and some subaggregation(file:agg_req_2,nr:19)
Feb 10 13:50:26.000 DBG field 'customer_birth_date_datetime64' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 WRN extractInterval: no interval found, returning default: 30s (fixed_interval)
Feb 10 13:50:26.000 DBG field 'type' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/date_histogram_with_null_values,_missing_parameter_(DateTime,_not_DateTime64),_and_some_subaggregation(file:agg_req_2,nr:20)(62)
i: 62 test: date_histogram with null values, missing parameter (DateTime, not DateTime64), and some subaggregation(file:agg_req_2,nr:20)
Feb 10 13:50:26.000 DBG field 'customer_birth_date' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 WRN extractInterval: no interval found, returning default: 30s (fixed_interval)
Feb 10 13:50:26.000 DBG field 'type' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/date_histogram_with_missing,_different_formats,_and_types_(DateTime/DateTime64)(file:agg_req_2,nr:21)(63)
i: 63 test: date_histogram with missing, different formats, and types (DateTime/DateTime64)(file:agg_req_2,nr:21)
Feb 10 13:50:26.000 DBG field 'customer_birth_date' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'customer_birth_date' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'customer_birth_date_datetime64' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'customer_birth_date_datetime64' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'customer_birth_date' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/histogram,_min_doc_count=0,_int_keys_when_interval=1(file:agg_req_2,nr:22)(64)
i: 64 test: histogram, min_doc_count=0, int keys when interval=1(file:agg_req_2,nr:22)
Feb 10 13:50:26.000 DBG field 'total_quantity' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'total_quantity' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'total_quantity' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'total_quantity' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/simplest_composite:_1_terms(file:agg_req_2,nr:23)(65)
i: 65 test: simplest composite: 1 terms(file:agg_req_2,nr:23)
Feb 10 13:50:26.000 DBG field 'product' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/simplest_composite:_1_histogram_(with_size)(file:agg_req_2,nr:24)(66)
i: 66 test: simplest composite: 1 histogram (with size)(file:agg_req_2,nr:24)
Feb 10 13:50:26.000 DBG field 'price' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/simplest_composite:_1_date_histogram(file:agg_req_2,nr:25)(67)
i: 67 test: simplest composite: 1 date_histogram(file:agg_req_2,nr:25)
Feb 10 13:50:26.000 DBG field 'timestamp' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/simplest_composite:_1_geotile_grid(file:agg_req_2,nr:26)(68)
i: 68 test: simplest composite: 1 geotile_grid(file:agg_req_2,nr:26)
Feb 10 13:50:26.000 DBG field 'OriginLocation' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/composite:_2_sources_+_1_subaggregation(file:agg_req_2,nr:27)(69)
i: 69 test: composite: 2 sources + 1 subaggregation(file:agg_req_2,nr:27)
Feb 10 13:50:26.000 DBG field 'timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'product' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'price' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'price' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/simplest_terms_with_exclude_(array_of_values)(file:agg_req_2,nr:28)(70)
i: 70 test: simplest terms with exclude (array of values)(file:agg_req_2,nr:28)
Feb 10 13:50:26.000 DBG field 'chess_goat' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/simplest_terms_with_exclude_(single_value,_no_regex)(file:agg_req_2,nr:29)(71)
i: 71 test: simplest terms with exclude (single value, no regex)(file:agg_req_2,nr:29)
Feb 10 13:50:26.000 DBG field 'agi_birth_year' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/simplest_terms_with_exclude_(empty_array)(file:agg_req_2,nr:30)(72)
i: 72 test: simplest terms with exclude (empty array)(file:agg_req_2,nr:30)
Feb 10 13:50:26.000 DBG field 'agi_birth_year' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/simplest_terms_with_exclude_(of_strings),_regression_test(file:agg_req_2,nr:31)(73)
i: 73 test: simplest terms with exclude (of strings), regression test(file:agg_req_2,nr:31)
Feb 10 13:50:26.000 DBG field 'chess_goat' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/terms_with_exclude_(more_complex,_string_field_with_exclude_regex)(file:agg_req_2,nr:32)(74)
i: 74 test: terms with exclude (more complex, string field with exclude regex)(file:agg_req_2,nr:32)
Feb 10 13:50:26.000 DBG field 'chess_goat' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/complex_terms_with_exclude:_nested_terms_+_2_metrics(file:agg_req_2,nr:33)(75)
i: 75 test: complex terms with exclude: nested terms + 2 metrics(file:agg_req_2,nr:33)
Feb 10 13:50:26.000 DBG field 'Carrier' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'DistanceMiles' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'DistanceMiles' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'DestCityName' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'AvgTicketPrice' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'AvgTicketPrice' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/terms_with_exclude,_but_with_branched_off_aggregation_tree(file:agg_req_2,nr:34)(76)
i: 76 test: terms with exclude, but with branched off aggregation tree(file:agg_req_2,nr:34)
Feb 10 13:50:26.000 DBG field 'Carrier' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'DistanceMiles' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'DistanceMiles' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'Carrier' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'DistanceMiles' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'DistanceMiles' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/terms_with_bool_field(file:agg_req_2,nr:35)(77)
i: 77 test: terms with bool field(file:agg_req_2,nr:35)
Feb 10 13:50:26.000 DBG field 'Cancelled' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/Escaping_of_',_\,_\n,_and_\t_in_some_example_aggregations._No_tests_for_other_escape_characters,_e.g._\r_or_'b._Add_if_needed.(file:agg_req_2,nr:36)(78)
i: 78 test: Escaping of ', \, \n, and \t in some example aggregations. No tests for other escape characters, e.g. \r or 'b. Add if needed.(file:agg_req_2,nr:36)
Feb 10 13:50:26.000 DBG field '@timestamp's\' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field '@timestamp's\' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'agent' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/simple_max/min_aggregation_as_2_siblings(file:dates,nr:0)(79)
i: 79 test: simple max/min aggregation as 2 siblings(file:dates,nr:0)
Feb 10 13:50:26.000 WRN seed is not an float64, but string, value: 1292529172. Using default: 0
Feb 10 13:50:26.000 DBG field 'order_date' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/extended_bounds_pre_keys_(timezone_calculations_most_tricky_to_get_right)(file:dates,nr:1)(80)
i: 80 test: extended_bounds pre keys (timezone calculations most tricky to get right)(file:dates,nr:1)
Feb 10 13:50:26.000 DBG field '@timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field '@timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field '@timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 WRN unexpected result in bucket_script: bucket_script(isCount: true, parent: _count, pathToParent: [], parentBucketAggregation: date_histogram(field: { @timestamp}, interval: 10s, min_doc_count: 0, timezone: Europe/Warsaw, script: ), len(rows): 0. Returning default.
Feb 10 13:50:26.000 WRN unexpected result in bucket_script: bucket_script(isCount: true, parent: _count, pathToParent: [], parentBucketAggregation: date_histogram(field: { @timestamp}, interval: 10s, min_doc_count: 0, timezone: Europe/Warsaw, script: ), len(rows): 0. Returning default.
Feb 10 13:50:26.000 WRN unexpected result in bucket_script: bucket_script(isCount: true, parent: _count, pathToParent: [], parentBucketAggregation: date_histogram(field: { @timestamp}, interval: 10s, min_doc_count: 0, timezone: Europe/Warsaw, script: ), len(rows): 0. Returning default.
Feb 10 13:50:26.000 WRN unexpected result in bucket_script: bucket_script(isCount: true, parent: _count, pathToParent: [], parentBucketAggregation: date_histogram(field: { @timestamp}, interval: 10s, min_doc_count: 0, timezone: Europe/Warsaw, script: ), len(rows): 0. Returning default.
Feb 10 13:50:26.000 WRN unexpected result in bucket_script: bucket_script(isCount: true, parent: _count, pathToParent: [], parentBucketAggregation: date_histogram(field: { @timestamp}, interval: 10s, min_doc_count: 0, timezone: Europe/Warsaw, script: ), len(rows): 0. Returning default.
Feb 10 13:50:26.000 WRN unexpected result in bucket_script: bucket_script(isCount: true, parent: _count, pathToParent: [], parentBucketAggregation: date_histogram(field: { @timestamp}, interval: 10s, min_doc_count: 0, timezone: Europe/Warsaw, script: ), len(rows): 0. Returning default.
Feb 10 13:50:26.000 WRN unexpected result in bucket_script: bucket_script(isCount: true, parent: _count, pathToParent: [], parentBucketAggregation: date_histogram(field: { @timestamp}, interval: 10s, min_doc_count: 0, timezone: Europe/Warsaw, script: ), len(rows): 0. Returning default.
Feb 10 13:50:26.000 WRN unexpected result in bucket_script: bucket_script(isCount: true, parent: _count, pathToParent: [], parentBucketAggregation: date_histogram(field: { @timestamp}, interval: 10s, min_doc_count: 0, timezone: Europe/Warsaw, script: ), len(rows): 0. Returning default.
=== RUN   TestPancakeQueryGeneration/extended_bounds_post_keys_(timezone_calculations_most_tricky_to_get_right)(file:dates,nr:2)(81)
i: 81 test: extended_bounds post keys (timezone calculations most tricky to get right)(file:dates,nr:2)
Feb 10 13:50:26.000 DBG field '@timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field '@timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field '@timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 WRN unexpected result in bucket_script: bucket_script(isCount: true, parent: _count, pathToParent: [], parentBucketAggregation: date_histogram(field: { @timestamp}, interval: 10s, min_doc_count: 0, timezone: Europe/Warsaw, script: ), len(rows): 0. Returning default.
Feb 10 13:50:26.000 WRN unexpected result in bucket_script: bucket_script(isCount: true, parent: _count, pathToParent: [], parentBucketAggregation: date_histogram(field: { @timestamp}, interval: 10s, min_doc_count: 0, timezone: Europe/Warsaw, script: ), len(rows): 0. Returning default.
Feb 10 13:50:26.000 WRN unexpected result in bucket_script: bucket_script(isCount: true, parent: _count, pathToParent: [], parentBucketAggregation: date_histogram(field: { @timestamp}, interval: 10s, min_doc_count: 0, timezone: Europe/Warsaw, script: ), len(rows): 0. Returning default.
Feb 10 13:50:26.000 WRN unexpected result in bucket_script: bucket_script(isCount: true, parent: _count, pathToParent: [], parentBucketAggregation: date_histogram(field: { @timestamp}, interval: 10s, min_doc_count: 0, timezone: Europe/Warsaw, script: ), len(rows): 0. Returning default.
Feb 10 13:50:26.000 WRN unexpected result in bucket_script: bucket_script(isCount: true, parent: _count, pathToParent: [], parentBucketAggregation: date_histogram(field: { @timestamp}, interval: 10s, min_doc_count: 0, timezone: Europe/Warsaw, script: ), len(rows): 0. Returning default.
=== RUN   TestPancakeQueryGeneration/empty_results,_we_still_should_add_empty_buckets,_because_of_the_extended_bounds_and_min_doc_count_defaulting_to_0(file:dates,nr:3)(82)
i: 82 test: empty results, we still should add empty buckets, because of the extended_bounds and min_doc_count defaulting to 0(file:dates,nr:3)
Feb 10 13:50:26.000 DBG field '@timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field '@timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field '@timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'body_bytes_sent' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'body_bytes_sent' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/date_histogram_add_in-between_rows,_calendar_interval:_>=_month_(regression_test)(file:dates,nr:4)(83)
i: 83 test: date_histogram add in-between rows, calendar_interval: >= month (regression test)(file:dates,nr:4)
Feb 10 13:50:26.000 DBG field 'date' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 WRN datetime field 'date' not found in table '__quesma_table_name'
Feb 10 13:50:26.000 WRN invalid date time type for field { date}
=== RUN   TestPancakeQueryGeneration/date_histogram_add_in-between_rows,_calendar_interval:_>=_month_(regression_test)(file:dates,nr:5)(84)
i: 84 test: date_histogram add in-between rows, calendar_interval: >= month (regression test)(file:dates,nr:5)
Feb 10 13:50:26.000 DBG field 'date' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 WRN datetime field 'date' not found in table '__quesma_table_name'
Feb 10 13:50:26.000 WRN invalid date time type for field { date}
=== RUN   TestPancakeQueryGeneration/date_histogram_add_in-between_rows,_calendar_interval:_>=_month_(regression_test)(file:dates,nr:6)(85)
i: 85 test: date_histogram add in-between rows, calendar_interval: >= month (regression test)(file:dates,nr:6)
Feb 10 13:50:26.000 DBG field 'date' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 WRN datetime field 'date' not found in table '__quesma_table_name'
Feb 10 13:50:26.000 WRN invalid date time type for field { date}
=== RUN   TestPancakeQueryGeneration/turing_1_-_painless_script_in_terms(file:dates,nr:7)(86)
i: 86 test: turing 1 - painless script in terms(file:dates,nr:7)
Feb 10 13:50:26.000 DBG field '@timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 WRN we didn't expect value_type in Terms params map[order:map[_count:desc] script:map[lang:painless source:if (doc['request_id.value'].value == doc['origin_request_id.value'].value) {
return 1;
} else {
return 0;
}] shard_size:25 size:5 value_type:boolean]
=== RUN   TestPancakeQueryGeneration/Range_with_subaggregations._Reproduce:_Visualize_->_Pie_chart_->_Aggregation:_Unique_Count,_Buckets:_Aggregation:_Range(file:opensearch-visualize/agg_req,nr:0)(87)
i: 87 test: Range with subaggregations. Reproduce: Visualize -> Pie chart -> Aggregation: Unique Count, Buckets: Aggregation: Range(file:opensearch-visualize/agg_req,nr:0)
Feb 10 13:50:26.000 DBG field 'epoch_time' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'epoch_time' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 WRN datetime field 'epoch_time' not found in table '__quesma_table_name'
Feb 10 13:50:26.000 DBG field 'ftd_session_time' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'ftd_session_time' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'ftd_session_time' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/Range_with_subaggregations._Reproduce:_Visualize_->_Pie_chart_->_Aggregation:_Top_Hit,_Buckets:_Aggregation:_Range(file:opensearch-visualize/agg_req,nr:1)(88)
i: 88 test: Range with subaggregations. Reproduce: Visualize -> Pie chart -> Aggregation: Top Hit, Buckets: Aggregation: Range(file:opensearch-visualize/agg_req,nr:1)
Feb 10 13:50:26.000 DBG field 'epoch_time' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'epoch_time' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 WRN datetime field 'epoch_time' not found in table '__quesma_table_name'
Feb 10 13:50:26.000 DBG field 'properties.entry_time' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 WRN no columns returned for top_hits aggregation, len(rows[0].Cols): 0, len(rows): 1
Feb 10 13:50:26.000 WRN no columns returned for top_hits aggregation, skipping
=== RUN   TestPancakeQueryGeneration/Range_with_subaggregations._Reproduce:_Visualize_->_Pie_chart_->_Aggregation:_Sum,_Buckets:_Aggregation:_Range(file:opensearch-visualize/agg_req,nr:2)(89)
i: 89 test: Range with subaggregations. Reproduce: Visualize -> Pie chart -> Aggregation: Sum, Buckets: Aggregation: Range(file:opensearch-visualize/agg_req,nr:2)
Feb 10 13:50:26.000 DBG field 'epoch_time' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'epoch_time' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 WRN datetime field 'epoch_time' not found in table '__quesma_table_name'
Feb 10 13:50:26.000 DBG field 'epoch_time_original' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'properties.entry_time' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'properties.entry_time' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/Range_with_subaggregations._Reproduce:_Visualize_->_Heat_Map_->_Metrics:_Median,_Buckets:_X-Asis_Range(file:opensearch-visualize/agg_req,nr:3)(90)
i: 90 test: Range with subaggregations. Reproduce: Visualize -> Heat Map -> Metrics: Median, Buckets: X-Asis Range(file:opensearch-visualize/agg_req,nr:3)
Feb 10 13:50:26.000 DBG field 'epoch_time' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'epoch_time' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 WRN datetime field 'epoch_time' not found in table '__quesma_table_name'
Feb 10 13:50:26.000 DBG field 'properties::exoestimation_connection_speedinkbps' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'properties::entry_time' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'properties::entry_time' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/Max_on_DateTime_field._Reproduce:_Visualize_->_Line:_Metrics_->_Max_@timestamp,_Buckets:_Add_X-Asis,_Aggregation:_Significant_Terms(file:opensearch-visualize/agg_req,nr:4)(91)
i: 91 test: Max on DateTime field. Reproduce: Visualize -> Line: Metrics -> Max @timestamp, Buckets: Add X-Asis, Aggregation: Significant Terms(file:opensearch-visualize/agg_req,nr:4)
Feb 10 13:50:26.000 DBG field 'timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'response' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'timestamp' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/Min_on_DateTime_field._Reproduce:_Visualize_->_Line:_Metrics_->_Min_@timestamp,_Buckets:_Add_X-Asis,_Aggregation:_Significant_Terms(file:opensearch-visualize/agg_req,nr:5)(92)
i: 92 test: Min on DateTime field. Reproduce: Visualize -> Line: Metrics -> Min @timestamp, Buckets: Add X-Asis, Aggregation: Significant Terms(file:opensearch-visualize/agg_req,nr:5)
Feb 10 13:50:26.000 DBG field 'timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'response' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'timestamp' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/Percentiles_on_DateTime_field._Reproduce:_Visualize_->_Line:_Metrics_->_Percentiles_(or_Median,_it's_the_same_aggregation)_@timestamp,_Buckets:_Add_X-Asis,_Aggregation:_Significant_Terms(file:opensearch-visualize/agg_req,nr:6)(93)
i: 93 test: Percentiles on DateTime field. Reproduce: Visualize -> Line: Metrics -> Percentiles (or Median, it's the same aggregation) @timestamp, Buckets: Add X-Asis, Aggregation: Significant Terms(file:opensearch-visualize/agg_req,nr:6)
Feb 10 13:50:26.000 DBG field 'timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'response' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'timestamp' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/Percentile_ranks_keyed=false._Reproduce:_Visualize_->_Line_->_Metrics:_Percentile_Ranks,_Buckets:_X-Asis_Date_Histogram(file:opensearch-visualize/agg_req,nr:7)(94)
i: 94 test: Percentile_ranks keyed=false. Reproduce: Visualize -> Line -> Metrics: Percentile Ranks, Buckets: X-Asis Date Histogram(file:opensearch-visualize/agg_req,nr:7)
Feb 10 13:50:26.000 DBG field 'timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'AvgTicketPrice' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/Min/max_with_simple_script._Reproduce:_Visualize_->_Line_->_Metrics:_Count,_Buckets:_X-Asis_Histogram(file:opensearch-visualize/agg_req,nr:8)(95)
i: 95 test: Min/max with simple script. Reproduce: Visualize -> Line -> Metrics: Count, Buckets: X-Asis Histogram(file:opensearch-visualize/agg_req,nr:8)
=== RUN   TestPancakeQueryGeneration/Histogram_with_simple_script._Reproduce:_Visualize_->_Line_->_Metrics:_Count,_Buckets:_X-Asis_Histogram(file:opensearch-visualize/agg_req,nr:9)(96)
i: 96 test: Histogram with simple script. Reproduce: Visualize -> Line -> Metrics: Count, Buckets: X-Asis Histogram(file:opensearch-visualize/agg_req,nr:9)
=== RUN   TestPancakeQueryGeneration/dashboard-1:_latency_by_region(file:dashboard-1/agg_req,nr:0)(97)
i: 97 test: dashboard-1: latency by region(file:dashboard-1/agg_req,nr:0)
Feb 10 13:50:26.000 DBG field 'reqTimeSec' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'reqTimeSec' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 WRN datetime field 'reqTimeSec' not found in table '__quesma_table_name'
Feb 10 13:50:26.000 DBG field 'rspContentLen' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'rspContentLen' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'rspContentLen' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'rspContentLen' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/dashboard-1:_bug,_used_to_be_infinite_loop(file:dashboard-1/agg_req,nr:1)(98)
i: 98 test: dashboard-1: bug, used to be infinite loop(file:dashboard-1/agg_req,nr:1)
Feb 10 13:50:26.000 DBG field 'reqTimeSec' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'reqTimeSec' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 WRN datetime field 'reqTimeSec' not found in table '__quesma_table_name'
Feb 10 13:50:26.000 DBG field 'reqTimeSec' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 WRN datetime field 'reqTimeSec' not found in table '__quesma_table_name'
Feb 10 13:50:26.000 WRN invalid date time type for field { reqTimeSec}
Feb 10 13:50:26.000 ERR invalid date type for DateHistogram date_histogram(field: { reqTimeSec}, interval: 30s, min_doc_count: 0, timezone: Europe/Warsaw. Using DateTime64 as default.
Feb 10 13:50:26.000 DBG field 'billingRegion' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'latency' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'latency' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/Simplest_cumulative_sum_(count)._Reproduce:_Visualize_->_Vertical_Bar:_Metrics:_Cumulative_Sum_(Aggregation:_Count),_Buckets:_Histogram(file:opensearch-visualize/pipeline_agg_req,nr:0)(99)
i: 99 test: Simplest cumulative_sum (count). Reproduce: Visualize -> Vertical Bar: Metrics: Cumulative Sum (Aggregation: Count), Buckets: Histogram(file:opensearch-visualize/pipeline_agg_req,nr:0)
Feb 10 13:50:26.000 DBG field 'order_date' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'order_date' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'day_of_week_i' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/Cumulative_sum_with_other_aggregation._Reproduce:_Visualize_->_Vertical_Bar:_Metrics:_Cumulative_Sum_(Aggregation:_Average),_Buckets:_Histogram(file:opensearch-visualize/pipeline_agg_req,nr:1)(100)
i: 100 test: Cumulative sum with other aggregation. Reproduce: Visualize -> Vertical Bar: Metrics: Cumulative Sum (Aggregation: Average), Buckets: Histogram(file:opensearch-visualize/pipeline_agg_req,nr:1)
Feb 10 13:50:26.000 DBG field 'day_of_week_i' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'day_of_week_i' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'day_of_week_i' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/Cumulative_sum_to_other_cumulative_sum._Reproduce:_Visualize_->_Vertical_Bar:_Metrics:_Cumulative_Sum_(Aggregation:_Cumulative_Sum_(Aggregation:_Count)),_Buckets:_Histogram(file:opensearch-visualize/pipeline_agg_req,nr:2)(101)
i: 101 test: Cumulative sum to other cumulative sum. Reproduce: Visualize -> Vertical Bar: Metrics: Cumulative Sum (Aggregation: Cumulative Sum (Aggregation: Count)), Buckets: Histogram(file:opensearch-visualize/pipeline_agg_req,nr:2)
Feb 10 13:50:26.000 DBG field 'day_of_week_i' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 WRN could not find parent column metric__2__1-metric_col_0
Feb 10 13:50:26.000 ERR pipeline 1 already exists in resultsPerPipeline
=== RUN   TestPancakeQueryGeneration/Cumulative_sum_-_quite_complex,_a_graph_of_pipelines._Reproduce:_Visualize_->_Vertical_Bar:_Metrics:_Cumulative_Sum_(Aggregation:_Cumulative_Sum_(Aggregation:_Max)),_Buckets:_Histogram(file:opensearch-visualize/pipeline_agg_req,nr:3)(102)
i: 102 test: Cumulative sum - quite complex, a graph of pipelines. Reproduce: Visualize -> Vertical Bar: Metrics: Cumulative Sum (Aggregation: Cumulative Sum (Aggregation: Max)), Buckets: Histogram(file:opensearch-visualize/pipeline_agg_req,nr:3)
Feb 10 13:50:26.000 DBG field 'day_of_week_i' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'products.base_price' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'products.base_price' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 WRN could not find parent column metric__2__1-metric_col_0
Feb 10 13:50:26.000 ERR pipeline 1 already exists in resultsPerPipeline
=== RUN   TestPancakeQueryGeneration/Simplest_Derivative_(count)._Reproduce:_Visualize_->_Vertical_Bar:_Metrics:_Derivative_(Aggregation:_Count),_Buckets:_Histogram(file:opensearch-visualize/pipeline_agg_req,nr:4)(103)
i: 103 test: Simplest Derivative (count). Reproduce: Visualize -> Vertical Bar: Metrics: Derivative (Aggregation: Count), Buckets: Histogram(file:opensearch-visualize/pipeline_agg_req,nr:4)
Feb 10 13:50:26.000 DBG field 'bytes' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/Derivative_with_other_aggregation._Reproduce:_Visualize_->_Vertical_Bar:_Metrics:_Derivative_(Aggregation:_Sum),_Buckets:_Date_Histogram(file:opensearch-visualize/pipeline_agg_req,nr:5)(104)
i: 104 test: Derivative with other aggregation. Reproduce: Visualize -> Vertical Bar: Metrics: Derivative (Aggregation: Sum), Buckets: Date Histogram(file:opensearch-visualize/pipeline_agg_req,nr:5)
Feb 10 13:50:26.000 DBG field 'timestamp' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/Derivative_to_cumulative_sum._Reproduce:_Visualize_->_Vertical_Bar:_Metrics:_Derivative_(Aggregation:_Cumulative_Sum_(Aggregation:_Count)),_Buckets:_Date_Histogram(file:opensearch-visualize/pipeline_agg_req,nr:6)(105)
i: 105 test: Derivative to cumulative sum. Reproduce: Visualize -> Vertical Bar: Metrics: Derivative (Aggregation: Cumulative Sum (Aggregation: Count)), Buckets: Date Histogram(file:opensearch-visualize/pipeline_agg_req,nr:6)
Feb 10 13:50:26.000 DBG field 'timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 WRN could not find parent column metric__2__1-metric_col_0
Feb 10 13:50:26.000 ERR pipeline 1 already exists in resultsPerPipeline
=== RUN   TestPancakeQueryGeneration/Simplest_Serial_Diff_(count),_lag=default_(1)._Reproduce:_Visualize_->_Vertical_Bar:_Metrics:_Serial_Diff_(Aggregation:_Count),_Buckets:_Histogram(file:opensearch-visualize/pipeline_agg_req,nr:7)(106)
i: 106 test: Simplest Serial Diff (count), lag=default (1). Reproduce: Visualize -> Vertical Bar: Metrics: Serial Diff (Aggregation: Count), Buckets: Histogram(file:opensearch-visualize/pipeline_agg_req,nr:7)
Feb 10 13:50:26.000 DBG field 'bytes' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/Simplest_Serial_Diff_(count),_lag=2._Don't_know_how_to_reproduce_in_OpenSearch,_but_you_can_click_out:Reproduce:_Visualize_->_Vertical_Bar:_Metrics:_Serial_Diff_(Aggregation:_Count),_Buckets:_HistogramAnd_then_change_the_request_manually(file:opensearch-visualize/pipeline_agg_req,nr:8)(107)
i: 107 test: Simplest Serial Diff (count), lag=2. Don't know how to reproduce in OpenSearch, but you can click out:Reproduce: Visualize -> Vertical Bar: Metrics: Serial Diff (Aggregation: Count), Buckets: HistogramAnd then change the request manually(file:opensearch-visualize/pipeline_agg_req,nr:8)
Feb 10 13:50:26.000 DBG field 'bytes' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/Serial_diff_with_other_aggregation._Reproduce:_Visualize_->_Vertical_Bar:_Metrics:_Serial_Diff_(Aggregation:_Sum),_Buckets:_Date_Histogram(file:opensearch-visualize/pipeline_agg_req,nr:9)(108)
i: 108 test: Serial diff with other aggregation. Reproduce: Visualize -> Vertical Bar: Metrics: Serial Diff (Aggregation: Sum), Buckets: Date Histogram(file:opensearch-visualize/pipeline_agg_req,nr:9)
Feb 10 13:50:26.000 DBG field 'timestamp' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/Serial_Diff_to_cumulative_sum._Reproduce:_Visualize_->_Vertical_Bar:_Metrics:_Serial_Diff_(Aggregation:_Cumulative_Sum_(Aggregation:_Count)),_Buckets:_Date_Histogram(file:opensearch-visualize/pipeline_agg_req,nr:10)(109)
i: 109 test: Serial Diff to cumulative sum. Reproduce: Visualize -> Vertical Bar: Metrics: Serial Diff (Aggregation: Cumulative Sum (Aggregation: Count)), Buckets: Date Histogram(file:opensearch-visualize/pipeline_agg_req,nr:10)
Feb 10 13:50:26.000 DBG field 'timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 WRN could not find parent column metric__2__1-metric_col_0
Feb 10 13:50:26.000 ERR pipeline 1 already exists in resultsPerPipeline
=== RUN   TestPancakeQueryGeneration/Simplest_avg_bucket._Reproduce:_Visualize_->_Vertical_Bar:_Metrics:_Average_Bucket_(Bucket:_Date_Histogram,_Metric:_Count)(file:opensearch-visualize/pipeline_agg_req,nr:11)(110)
i: 110 test: Simplest avg_bucket. Reproduce: Visualize -> Vertical Bar: Metrics: Average Bucket (Bucket: Date Histogram, Metric: Count)(file:opensearch-visualize/pipeline_agg_req,nr:11)
Feb 10 13:50:26.000 DBG field 'timestamp' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/avg_bucket._Reproduce:_Visualize_->_Vertical_Bar:_Metrics:_Average_Bucket_(Bucket:_Date_Histogram,_Metric:_Max)(file:opensearch-visualize/pipeline_agg_req,nr:12)(111)
i: 111 test: avg_bucket. Reproduce: Visualize -> Vertical Bar: Metrics: Average Bucket (Bucket: Date Histogram, Metric: Max)(file:opensearch-visualize/pipeline_agg_req,nr:12)
Feb 10 13:50:26.000 DBG field 'timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'bytes' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'bytes' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/avg_bucket._Reproduce:_Visualize_->_Horizontal_Bar:_Metrics:_Average_Bucket_(Bucket:_Histogram,_Metric:_Count),_Buckets:_X-Asis:_Date_Histogram(file:opensearch-visualize/pipeline_agg_req,nr:13)(112)
i: 112 test: avg_bucket. Reproduce: Visualize -> Horizontal Bar: Metrics: Average Bucket (Bucket: Histogram, Metric: Count), Buckets: X-Asis: Date Histogram(file:opensearch-visualize/pipeline_agg_req,nr:13)
Feb 10 13:50:26.000 DBG field 'timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'bytes' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/Simplest_min_bucket._Reproduce:_Visualize_->_Vertical_Bar:_Metrics:_Min_Bucket_(Bucket:_Terms,_Metric:_Count)(file:opensearch-visualize/pipeline_agg_req,nr:14)(113)
i: 113 test: Simplest min_bucket. Reproduce: Visualize -> Vertical Bar: Metrics: Min Bucket (Bucket: Terms, Metric: Count)(file:opensearch-visualize/pipeline_agg_req,nr:14)
Feb 10 13:50:26.000 DBG field 'timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'clientip' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/min_bucket._Reproduce:_Visualize_->_Vertical_Bar:_Metrics:_Min_Bucket_(Bucket:_Terms,_Metric:_Unique_Count)(file:opensearch-visualize/pipeline_agg_req,nr:15)(114)
i: 114 test: min_bucket. Reproduce: Visualize -> Vertical Bar: Metrics: Min Bucket (Bucket: Terms, Metric: Unique Count)(file:opensearch-visualize/pipeline_agg_req,nr:15)
Feb 10 13:50:26.000 DBG field 'clientip' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'geo.coordinates' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'geo.coordinates' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/complex_min_bucket._Reproduce:_Visualize_->_Vertical_Bar:_Metrics:_Min_Bucket_(Bucket:_Terms,_Metric:_Sum),_Buckets:_Split_Series:_Histogram(file:opensearch-visualize/pipeline_agg_req,nr:16)(115)
i: 115 test: complex min_bucket. Reproduce: Visualize -> Vertical Bar: Metrics: Min Bucket (Bucket: Terms, Metric: Sum), Buckets: Split Series: Histogram(file:opensearch-visualize/pipeline_agg_req,nr:16)
Feb 10 13:50:26.000 DBG field 'bytes' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'clientip' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'bytes' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'bytes' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/Simplest_max_bucket._Reproduce:_Visualize_->_Line:_Metrics:_Max_Bucket_(Bucket:_Terms,_Metric:_Count)(file:opensearch-visualize/pipeline_agg_req,nr:17)(116)
i: 116 test: Simplest max_bucket. Reproduce: Visualize -> Line: Metrics: Max Bucket (Bucket: Terms, Metric: Count)(file:opensearch-visualize/pipeline_agg_req,nr:17)
Feb 10 13:50:26.000 DBG field 'timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'Cancelled' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/Max/Sum_bucket_with_some_null_buckets._Reproduce:_Visualize_->_Vertical_Bar:_Metrics:_Max_(Sum)_Bucket_(Aggregation:_Date_Histogram,_Metric:_Min)(file:opensearch-visualize/pipeline_agg_req,nr:18)(117)
i: 117 test: Max/Sum bucket with some null buckets. Reproduce: Visualize -> Vertical Bar: Metrics: Max (Sum) Bucket (Aggregation: Date Histogram, Metric: Min)(file:opensearch-visualize/pipeline_agg_req,nr:18)
Feb 10 13:50:26.000 DBG field 'timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'memory' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'memory' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 WRN could not convert value to float: <nil>, type: <nil>. Skipping
Feb 10 13:50:26.000 WRN could not convert value to float: <nil>, type: <nil>. Skipping
=== RUN   TestPancakeQueryGeneration/Different_pipeline_aggrs_with_some_null_buckets._Reproduce:_Visualize_->_Vertical_Bar:_Metrics:_Max/Sum_Bucket/etc._(Aggregation:_Histogram,_Metric:_Max)(file:opensearch-visualize/pipeline_agg_req,nr:19)(118)
i: 118 test: Different pipeline aggrs with some null buckets. Reproduce: Visualize -> Vertical Bar: Metrics: Max/Sum Bucket/etc. (Aggregation: Histogram, Metric: Max)(file:opensearch-visualize/pipeline_agg_req,nr:19)
Feb 10 13:50:26.000 DBG field 'bytes' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'memory' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'memory' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/max_bucket._Reproduce:_Visualize_->_Line:_Metrics:_Max_Bucket_(Bucket:_Filters,_Metric:_Sum)(file:opensearch-visualize/pipeline_agg_req,nr:20)(119)
pancake_sql_query_generation_test.go:65: Was skipped before. Wrong key in max_bucket, should be an easy fix
=== RUN   TestPancakeQueryGeneration/complex_max_bucket._Reproduce:_Visualize_->_Line:_Metrics:_Max_Bucket_(Bucket:_Filters,_Metric:_Sum),_Buckets:_Split_chart:_Rows_->_Range(file:opensearch-visualize/pipeline_agg_req,nr:21)(120)
pancake_sql_query_generation_test.go:65: Was skipped before. Wrong key in max_bucket, should be an easy fix
=== RUN   TestPancakeQueryGeneration/Simplest_sum_bucket._Reproduce:_Visualize_->_Horizontal_Bar:_Metrics:_Sum_Bucket_(B_ucket:_Terms,_Metric:_Count)(file:opensearch-visualize/pipeline_agg_req,nr:22)(121)
i: 121 test: Simplest sum_bucket. Reproduce: Visualize -> Horizontal Bar: Metrics: Sum Bucket (B ucket: Terms, Metric: Count)(file:opensearch-visualize/pipeline_agg_req,nr:22)
Feb 10 13:50:26.000 DBG field 'timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'extension' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/sum_bucket._Reproduce:_Visualize_->_Horizontal_Bar:_Metrics:_Sum_Bucket_(Bucket:_Significant_Terms,_Metric:_Average)(file:opensearch-visualize/pipeline_agg_req,nr:23)(122)
i: 122 test: sum_bucket. Reproduce: Visualize -> Horizontal Bar: Metrics: Sum Bucket (Bucket: Significant Terms, Metric: Average)(file:opensearch-visualize/pipeline_agg_req,nr:23)
Feb 10 13:50:26.000 DBG field 'extension' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'machine.ram' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'machine.ram' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/complex_sum_bucket._Reproduce:_Visualize_->_Vertical_Bar:_Metrics:_Sum_Bucket_(Bucket:_Date_Histogram,_Metric:_Average),_Buckets:_X-Asis:_Histogram(file:opensearch-visualize/pipeline_agg_req,nr:24)(123)
i: 123 test: complex sum_bucket. Reproduce: Visualize -> Vertical Bar: Metrics: Sum Bucket (Bucket: Date Histogram, Metric: Average), Buckets: X-Asis: Histogram(file:opensearch-visualize/pipeline_agg_req,nr:24)
Feb 10 13:50:26.000 DBG field 'bytes' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'bytes' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'memory' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'memory' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 WRN could not convert value to float: <nil>, type: <nil>. Skipping
Feb 10 13:50:26.000 WRN could not convert value to float: <nil>, type: <nil>. Skipping
=== RUN   TestPancakeQueryGeneration/Multi_terms_without_subaggregations._Visualize:_Bar_Vertical:_Horizontal_Axis:_Date_Histogram,_Vertical_Axis:_Count_of_records,_Breakdown:_Top_values_(2_values)(file:kibana-visualize/agg_req,nr:0)(124)
i: 124 test: Multi_terms without subaggregations. Visualize: Bar Vertical: Horizontal Axis: Date Histogram, Vertical Axis: Count of records, Breakdown: Top values (2 values)(file:kibana-visualize/agg_req,nr:0)
Feb 10 13:50:26.000 DBG field '@timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field '@timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field '@timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'severity' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'source' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/Multi_terms_with_simple_count._Visualize:_Bar_Vertical:_Horizontal_Axis:_Top_values_(2_values),_Vertical:_Count_of_records,_Breakdown:_@timestamp(file:kibana-visualize/agg_req,nr:1)(125)
i: 125 test: Multi_terms with simple count. Visualize: Bar Vertical: Horizontal Axis: Top values (2 values), Vertical: Count of records, Breakdown: @timestamp(file:kibana-visualize/agg_req,nr:1)
Feb 10 13:50:26.000 DBG field 'message' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'host.name' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field '@timestamp' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/Multi_terms_with_double-nested_subaggregations._Visualize:_Bar_Vertical:_Horizontal_Axis:_Top_values_(2_values),_Vertical:_Unique_count,_Breakdown:_@timestamp(file:kibana-visualize/agg_req,nr:2)(126)
i: 126 test: Multi_terms with double-nested subaggregations. Visualize: Bar Vertical: Horizontal Axis: Top values (2 values), Vertical: Unique count, Breakdown: @timestamp(file:kibana-visualize/agg_req,nr:2)
Feb 10 13:50:26.000 DBG field 'severity' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'source' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'severity' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'severity' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field '@timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'severity' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'severity' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/Quite_simple_multi_terms,_but_with_non-string_keys._Visualize:_Bar_Vertical:_Horizontal_Axis:_Date_Histogram,_Vertical_Axis:_Count_of_records,_Breakdown:_Top_values_(2_values)(file:kibana-visualize/agg_req,nr:3)(127)
i: 127 test: Quite simple multi_terms, but with non-string keys. Visualize: Bar Vertical: Horizontal Axis: Date Histogram, Vertical Axis: Count of records, Breakdown: Top values (2 values)(file:kibana-visualize/agg_req,nr:3)
Feb 10 13:50:26.000 DBG field 'Cancelled' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'AvgTicketPrice' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field '@timestamp' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/percentile_with_subaggregation_(so,_combinator)._Visualize,_Pie,_Slice_by:_top5_of_Cancelled,_DistanceKilometers,_Metric:_95th_Percentile(file:kibana-visualize/agg_req,nr:4)(128)
i: 128 test: percentile with subaggregation (so, combinator). Visualize, Pie, Slice by: top5 of Cancelled, DistanceKilometers, Metric: 95th Percentile(file:kibana-visualize/agg_req,nr:4)
Feb 10 13:50:26.000 DBG field 'Cancelled' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'DistanceKilometers' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'DistanceKilometers' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'DistanceKilometers' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'DistanceKilometers' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'DistanceKilometers' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 WRN unknown type for terms doc_count: int, value: 2974
=== RUN   TestPancakeQueryGeneration/terms_with_order_by_agg1>agg2_(multiple_aggregations)(file:kibana-visualize/agg_req,nr:5)(129)
i: 129 test: terms with order by agg1>agg2 (multiple aggregations)(file:kibana-visualize/agg_req,nr:5)
Feb 10 13:50:26.000 DBG field 'AvgTicketPrice' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'DistanceKilometers' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'DistanceKilometers' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/terms_with_order_by_stats,_easily_reproducible_in_Kibana_Visualize(file:kibana-visualize/agg_req,nr:6)(130)
i: 130 test: terms with order by stats, easily reproducible in Kibana Visualize(file:kibana-visualize/agg_req,nr:6)
Feb 10 13:50:26.000 DBG field 'timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'Carrier' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'FlightDelayMin' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'FlightDelayMin' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/terms_with_order_by_extended_stats_(easily_reproducible_in_Kibana_Visualize)(file:kibana-visualize/agg_req,nr:7)(131)
i: 131 test: terms with order by extended_stats (easily reproducible in Kibana Visualize)(file:kibana-visualize/agg_req,nr:7)
Feb 10 13:50:26.000 DBG field 'timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'Carrier' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'FlightDelayMin' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/Terms_with_order_by_top_metrics(file:kibana-visualize/agg_req,nr:8)(132)
pancake_sql_query_generation_test.go:60: Need to implement order by top metrics (talk with Jacek, he has an idea)
=== RUN   TestPancakeQueryGeneration/Line,_Y-axis:_Min,_Buckets:_Date_Range,_X-Axis:_Terms,_Split_Chart:_Date_Histogram(file:kibana-visualize/agg_req,nr:9)(133)
pancake_sql_query_generation_test.go:56: Date range is broken, fix in progress (PR #971)
=== RUN   TestPancakeQueryGeneration/simplest_IP_Prefix_(Kibana_8.13+),_ipv4_field,_prefix_length=0(file:kibana-visualize/agg_req,nr:10)(134)
i: 134 test: simplest IP Prefix (Kibana 8.13+), ipv4 field, prefix_length=0(file:kibana-visualize/agg_req,nr:10)
Feb 10 13:50:26.000 DBG field 'clientip' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/simplest_IP_Prefix_(Kibana_8.13+),_ipv4_field,_prefix_length=1(file:kibana-visualize/agg_req,nr:11)(135)
i: 135 test: simplest IP Prefix (Kibana 8.13+), ipv4 field, prefix_length=1(file:kibana-visualize/agg_req,nr:11)
Feb 10 13:50:26.000 DBG field 'clientip' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/simplest_IP_Prefix_(Kibana_8.13+),_ipv4_field,_prefix_length=10(file:kibana-visualize/agg_req,nr:12)(136)
i: 136 test: simplest IP Prefix (Kibana 8.13+), ipv4 field, prefix_length=10(file:kibana-visualize/agg_req,nr:12)
Feb 10 13:50:26.000 DBG field 'clientip' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/simplest_IP_Prefix_(Kibana_8.13+),_ipv4_field,_prefix_length=32(file:kibana-visualize/agg_req,nr:13)(137)
i: 137 test: simplest IP Prefix (Kibana 8.13+), ipv4 field, prefix_length=32(file:kibana-visualize/agg_req,nr:13)
Feb 10 13:50:26.000 DBG field 'clientip' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/simplest_IP_Prefix_(Kibana_8.13+),_ipv4_field,_keyed=true(file:kibana-visualize/agg_req,nr:14)(138)
i: 138 test: simplest IP Prefix (Kibana 8.13+), ipv4 field, keyed=true(file:kibana-visualize/agg_req,nr:14)
Feb 10 13:50:26.000 DBG field 'clientip' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/simplest_IP_Prefix_(Kibana_8.13+),_ipv4_field,_append_prefix_length=true(file:kibana-visualize/agg_req,nr:15)(139)
i: 139 test: simplest IP Prefix (Kibana 8.13+), ipv4 field, append_prefix_length=true(file:kibana-visualize/agg_req,nr:15)
Feb 10 13:50:26.000 DBG field 'clientip' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/simplest_IP_Prefix_(Kibana_8.13+),_ipv4_field,_keyed=true,_append_prefix_length=true(file:kibana-visualize/agg_req,nr:16)(140)
i: 140 test: simplest IP Prefix (Kibana 8.13+), ipv4 field, keyed=true, append_prefix_length=true(file:kibana-visualize/agg_req,nr:16)
Feb 10 13:50:26.000 DBG field 'clientip' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/IP_Prefix_with_other_aggregations(file:kibana-visualize/agg_req,nr:17)(141)
i: 141 test: IP Prefix with other aggregations(file:kibana-visualize/agg_req,nr:17)
Feb 10 13:50:26.000 DBG field 'bytes' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'clientip' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'bytes' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'bytes' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 WRN unknown type for terms doc_count: int, value: 441
=== RUN   TestPancakeQueryGeneration/simplest_IP_Prefix_(Kibana_8.13+),_ipv6_field,_prefix_length=0(file:kibana-visualize/agg_req,nr:18)(142)
i: 142 test: simplest IP Prefix (Kibana 8.13+), ipv6 field, prefix_length=0(file:kibana-visualize/agg_req,nr:18)
Feb 10 13:50:26.000 DBG field 'clientip' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/simplest_IP_Prefix_(Kibana_8.13+),_ipv6_field,_prefix_length=128(file:kibana-visualize/agg_req,nr:19)(143)
i: 143 test: simplest IP Prefix (Kibana 8.13+), ipv6 field, prefix_length=128(file:kibana-visualize/agg_req,nr:19)
Feb 10 13:50:26.000 DBG field 'clientip' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/IP_Prefix_(Kibana_8.13+),_ipv6_field,_keyed=true_and_overflow_of_1<<(128-prefix_length)(file:kibana-visualize/agg_req,nr:20)(144)
i: 144 test: IP Prefix (Kibana 8.13+), ipv6 field, keyed=true and overflow of 1<<(128-prefix_length)(file:kibana-visualize/agg_req,nr:20)
Feb 10 13:50:26.000 DBG field 'clientip' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/simple_IP_Prefix_(Kibana_8.13+),_ipv6_field,_non-zero&_and_non-ipv4_key(file:kibana-visualize/agg_req,nr:21)(145)
i: 145 test: simple IP Prefix (Kibana 8.13+), ipv6 field, non-zero& and non-ipv4 key(file:kibana-visualize/agg_req,nr:21)
Feb 10 13:50:26.000 DBG field 'clientip' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/IP_Prefix_(Kibana_8.13+),_ipv6_field,_multiple_keys_and_append_prefix_length=true(file:kibana-visualize/agg_req,nr:22)(146)
i: 146 test: IP Prefix (Kibana 8.13+), ipv6 field, multiple keys and append_prefix_length=true(file:kibana-visualize/agg_req,nr:22)
Feb 10 13:50:26.000 DBG field 'clientip' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/IP_Prefix_(Kibana_8.13+),_ipv6_field,_multiple_keys_and_append_prefix_length=true(file:kibana-visualize/agg_req,nr:23)(147)
i: 147 test: IP Prefix (Kibana 8.13+), ipv6 field, multiple keys and append_prefix_length=true(file:kibana-visualize/agg_req,nr:23)
Feb 10 13:50:26.000 DBG field 'clientip' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/Simplest_IP_range._In_Kibana:_Add_panel_>_Aggregation_Based_>_Area._Buckets:_X-asis:_IP_Range(file:kibana-visualize/agg_req,nr:24)(148)
i: 148 test: Simplest IP range. In Kibana: Add panel > Aggregation Based > Area. Buckets: X-asis: IP Range(file:kibana-visualize/agg_req,nr:24)
Feb 10 13:50:26.000 DBG field 'clientip' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/IP_range,_with_ranges_as_CIDR_masks._In_Kibana:_Add_panel_>_Aggregation_Based_>_Area._Buckets:_X-asis:_IP_Range(file:kibana-visualize/agg_req,nr:25)(149)
i: 149 test: IP range, with ranges as CIDR masks. In Kibana: Add panel > Aggregation Based > Area. Buckets: X-asis: IP Range(file:kibana-visualize/agg_req,nr:25)
Feb 10 13:50:26.000 DBG field 'clientip' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/IP_range,_with_ranges_as_CIDR_masks,_keyed=true._In_Kibana:_Add_panel_>_Aggregation_Based_>_Area._Buckets:_X-asis:_IP_Range(file:kibana-visualize/agg_req,nr:26)(150)
i: 150 test: IP range, with ranges as CIDR masks, keyed=true. In Kibana: Add panel > Aggregation Based > Area. Buckets: X-asis: IP Range(file:kibana-visualize/agg_req,nr:26)
Feb 10 13:50:26.000 DBG field 'clientip' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/IP_range_ipv6(file:kibana-visualize/agg_req,nr:27)(151)
i: 151 test: IP range ipv6(file:kibana-visualize/agg_req,nr:27)
Feb 10 13:50:26.000 DBG field 'clientip' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/IP_range_ipv6_with_mask(file:kibana-visualize/agg_req,nr:28)(152)
i: 152 test: IP range ipv6 with mask(file:kibana-visualize/agg_req,nr:28)
Feb 10 13:50:26.000 DBG field 'clientip' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/Sum_bucket_for_dates(file:kibana-visualize/pipeline_agg_req,nr:0)(153)
i: 153 test: Sum bucket for dates(file:kibana-visualize/pipeline_agg_req,nr:0)
Feb 10 13:50:26.000 DBG field 'timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'timestamp' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/Reproduce:_Visualize_->_Vertical_Bar:_Metrics:_Cumulative_Sum_(Aggregation:_Avg),_Buckets:_Date_Histogram(file:kibana-visualize/pipeline_agg_req,nr:1)(154)
i: 154 test: Reproduce: Visualize -> Vertical Bar: Metrics: Cumulative Sum (Aggregation: Avg), Buckets: Date Histogram(file:kibana-visualize/pipeline_agg_req,nr:1)
Feb 10 13:50:26.000 DBG field 'timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'dayOfWeek' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'dayOfWeek' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 WRN could not convert value to float: 0, type: int. Skipping
Feb 10 13:50:26.000 WRN could not convert value to float: 0, type: int. Skipping
=== RUN   TestPancakeQueryGeneration/Reproduce:_Visualize_->_Vertical_Bar:_Metrics:_Cumulative_Sum_(Aggregation:_Cumulative_Sum_(Aggregation:_Count)),_Buckets:_Date_Histogram(file:kibana-visualize/pipeline_agg_req,nr:2)(155)
i: 155 test: Reproduce: Visualize -> Vertical Bar: Metrics: Cumulative Sum (Aggregation: Cumulative Sum (Aggregation: Count)), Buckets: Date Histogram(file:kibana-visualize/pipeline_agg_req,nr:2)
Feb 10 13:50:26.000 DBG field 'timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 WRN could not find parent column metric__2__1-metric_col_0
Feb 10 13:50:26.000 ERR pipeline 1 already exists in resultsPerPipeline
=== RUN   TestPancakeQueryGeneration/Reproduce:_Visualize_->_Vertical_Bar:_Metrics:_Cumulative_Sum_(Aggregation:_Count),_Buckets:_Histogram(need_add_empty_rows,_even_though_there's_no_min_doc_count=0)(file:kibana-visualize/pipeline_agg_req,nr:3)(156)
i: 156 test: Reproduce: Visualize -> Vertical Bar: Metrics: Cumulative Sum (Aggregation: Count), Buckets: Histogram(need add empty rows, even though there's no min_doc_count=0)(file:kibana-visualize/pipeline_agg_req,nr:3)
Feb 10 13:50:26.000 DBG field 'DistanceMiles' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/clients/kunkka/test_0,_used_to_be_broken_before_aggregations_merge_fixOutput_more_or_less_works,_but_is_different_and_worse_than_what_Elastic_returns.If_it_starts_failing,_maybe_that's_a_good_thing(file:clients/kunkka,nr:0)(157)
i: 157 test: clients/kunkka/test_0, used to be broken before aggregations merge fixOutput more or less works, but is different and worse than what Elastic returns.If it starts failing, maybe that's a good thing(file:clients/kunkka,nr:0)
Feb 10 13:50:26.000 DBG field '@timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'multiplier' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'multiplier' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'spent' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'spent' referenced, but not found in schema, falling back to original name
pancake_sql_query_generation_test.go:115: We don't have result yet
=== RUN   TestPancakeQueryGeneration/it's_the_same_input_as_in_previous_test,_but_with_the_original_output_from_Elastic.Skipped_for_now,_as_our_response_is_different_in_2_things:_key_as_string_date_(probably_not_important)_+_we_don't_return_0's_(e.g._doc_count:_0).If_we_need_clients/kunkka/test_0,_used_to_be_broken_before_aggregations_merge_fix(file:clients/kunkka,nr:1)(158)
i: 158 test: it's the same input as in previous test, but with the original output from Elastic.Skipped for now, as our response is different in 2 things: key_as_string date (probably not important) + we don't return 0's (e.g. doc_count: 0).If we need clients/kunkka/test_0, used to be broken before aggregations merge fix(file:clients/kunkka,nr:1)
Feb 10 13:50:26.000 DBG field '@timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'spent' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'spent' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'multiplier' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'multiplier' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/clients/kunkka/test_1,_used_to_be_broken_before_aggregations_merge_fix(file:clients/kunkka,nr:2)(159)
pancake_sql_query_generation_test.go:52: Fix filters
=== RUN   TestPancakeQueryGeneration/Ophelia_Test_1:_triple_terms_+_default_order(file:clients/ophelia,nr:0)(160)
i: 160 test: Ophelia Test 1: triple terms + default order(file:clients/ophelia,nr:0)
Feb 10 13:50:26.000 DBG field 'surname' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'limbName' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'organName' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/Ophelia_Test_2:_triple_terms_+_other_aggregations_+_default_order(file:clients/ophelia,nr:1)(161)
i: 161 test: Ophelia Test 2: triple terms + other aggregations + default order(file:clients/ophelia,nr:1)
Feb 10 13:50:26.000 DBG field 'surname' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'total' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'total' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'limbName' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'total' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'total' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'organName' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'total' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'total' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'some' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'some' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/Ophelia_Test_3:_5x_terms_+_a_lot_of_other_aggregations(file:clients/ophelia,nr:2)(162)
i: 162 test: Ophelia Test 3: 5x terms + a lot of other aggregations(file:clients/ophelia,nr:2)
Feb 10 13:50:26.000 DBG field 'surname' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'total' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'total' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'limbName' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'organName' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'total' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'total' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'doctorName' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'height' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'total' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'total' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'some' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'some' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'cost' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'cost' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'total' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'total' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'total' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'total' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/Ophelia_Test_4:_triple_terms_+_order_by_another_aggregations(file:clients/ophelia,nr:3)(163)
i: 163 test: Ophelia Test 4: triple terms + order by another aggregations(file:clients/ophelia,nr:3)
Feb 10 13:50:26.000 DBG field 'surname' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'limbName' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'organName' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'total' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'total' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'total' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'total' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/Ophelia_Test_5:_4x_terms_+_order_by_another_aggregations(file:clients/ophelia,nr:4)(164)
i: 164 test: Ophelia Test 5: 4x terms + order by another aggregations(file:clients/ophelia,nr:4)
Feb 10 13:50:26.000 DBG field 'surname' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'limbName' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'organName' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'organName' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'total' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'total' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/Ophelia_Test_6:_triple_terms_+_other_aggregations_+_order_by_another_aggregations(file:clients/ophelia,nr:5)(165)
i: 165 test: Ophelia Test 6: triple terms + other aggregations + order by another aggregations(file:clients/ophelia,nr:5)
Feb 10 13:50:26.000 DBG field 'surname' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'total' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'total' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'limbName' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'organName' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'some' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'some' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'total' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'total' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'total' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'total' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/Ophelia_Test_7:_5x_terms_+_a_lot_of_other_aggregations_+_different_order_bys(file:clients/ophelia,nr:6)(166)
i: 166 test: Ophelia Test 7: 5x terms + a lot of other aggregations + different order bys(file:clients/ophelia,nr:6)
Feb 10 13:50:26.000 DBG field 'surname' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'total' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'total' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'limbName' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'organName' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'total' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'total' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'doctorName' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'total' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'total' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'height' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'total' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'total' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'some' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'some' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'cost' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'cost' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'total' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'total' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/todo(file:clients/clover,nr:0)(167)
i: 167 test: todo(file:clients/clover,nr:0)
Feb 10 13:50:26.000 DBG field 'nobel_laureate' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field '@timestamp' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/multiple_buckets_path(file:clients/clover,nr:1)(168)
i: 168 test: multiple buckets_path(file:clients/clover,nr:1)
Feb 10 13:50:26.000 DBG field '@timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field '@timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field '@timestamp' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/simplest_auto_date_histogram(file:clients/clover,nr:2)(169)
i: 169 test: simplest auto_date_histogram(file:clients/clover,nr:2)
Feb 10 13:50:26.000 DBG field 'timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'timestamp' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/bucket_script_with_multiple_buckets_path(file:clients/clover,nr:3)(170)
i: 170 test: bucket_script with multiple buckets_path(file:clients/clover,nr:3)
Feb 10 13:50:26.000 DBG field '@timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field '@timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field '@timestamp' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/todo(file:clients/clover,nr:4)(171)
i: 171 test: todo(file:clients/clover,nr:4)
Feb 10 13:50:26.000 DBG field 'a' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'c' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'field' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/todo(file:clients/clover,nr:5)(172)
i: 172 test: todo(file:clients/clover,nr:5)
Feb 10 13:50:26.000 DBG field '@timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field '@timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field '@timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 WRN unexpected result in bucket_script: bucket_script(isCount: true, parent: _count, pathToParent: [], parentBucketAggregation: date_histogram(field: { @timestamp}, interval: 7d, min_doc_count: 0, timezone: , script: ), len(rows): 0. Returning default.
Feb 10 13:50:26.000 WRN unexpected result in bucket_script: bucket_script(isCount: true, parent: _count, pathToParent: [], parentBucketAggregation: date_histogram(field: { @timestamp}, interval: 7d, min_doc_count: 0, timezone: , script: ), len(rows): 0. Returning default.
Feb 10 13:50:26.000 WRN unexpected result in bucket_script: bucket_script(isCount: true, parent: _count, pathToParent: [], parentBucketAggregation: date_histogram(field: { @timestamp}, interval: 7d, min_doc_count: 0, timezone: , script: ), len(rows): 0. Returning default.
=== RUN   TestPancakeQueryGeneration/Clover(file:clients/clover,nr:6)(173)
i: 173 test: Clover(file:clients/clover,nr:6)
Feb 10 13:50:26.000 DBG field '@timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'count' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'count' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/Weird_aggregation_and_filter_names(file:clients/clover,nr:7)(174)
i: 174 test: Weird aggregation and filter names(file:clients/clover,nr:7)
Feb 10 13:50:26.000 DBG field '@timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'a.b' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'a.b' referenced, but not found in schema, falling back to original name
=== RUN   TestPancakeQueryGeneration/empty_results(file:clients/turing,nr:0)(175)
i: 175 test: empty results(file:clients/turing,nr:0)
Feb 10 13:50:26.000 DBG field '@timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'score' referenced, but not found in schema, falling back to original name
--- PASS: TestPancakeQueryGeneration (0.31s)
--- PASS: TestPancakeQueryGeneration/simple_max/min_aggregation_as_2_siblings(file:agg_req,nr:0)(0) (0.00s)
--- PASS: TestPancakeQueryGeneration/2_sibling_count_aggregations(file:agg_req,nr:1)(1) (0.00s)
--- PASS: TestPancakeQueryGeneration/date_histogram_+_size_as_string(file:agg_req,nr:2)(2) (0.01s)
--- PASS: TestPancakeQueryGeneration/Sum(file:agg_req,nr:3)(3) (0.00s)
--- PASS: TestPancakeQueryGeneration/cardinality(file:agg_req,nr:4)(4) (0.00s)
--- PASS: TestPancakeQueryGeneration/simple_filter/count(file:agg_req,nr:5)(5) (0.00s)
--- PASS: TestPancakeQueryGeneration/filters(file:agg_req,nr:6)(6) (0.00s)
--- PASS: TestPancakeQueryGeneration/top_hits,_quite_complex(file:agg_req,nr:7)(7) (0.01s)
--- PASS: TestPancakeQueryGeneration/histogram,_different_field_than_timestamp(file:agg_req,nr:8)(8) (0.00s)
--- PASS: TestPancakeQueryGeneration/double_aggregation_with_histogram_+_harder_query(file:agg_req,nr:9)(9) (0.00s)
--- PASS: TestPancakeQueryGeneration/very_long:_multiple_top_metrics_+_histogram(file:agg_req,nr:10)(10) (0.00s)
--- PASS: TestPancakeQueryGeneration/value_count_+_top_values:_regression_test(file:agg_req,nr:11)(11) (0.00s)
--- PASS: TestPancakeQueryGeneration/date_histogram:_regression_test(file:agg_req,nr:12)(12) (0.00s)
--- PASS: TestPancakeQueryGeneration/terms_with_date_histogram_as_subaggregation:_regression_test(file:agg_req,nr:13)(13) (0.00s)
--- PASS: TestPancakeQueryGeneration/earliest/latest_timestamp:_regression_test(file:agg_req,nr:14)(14) (0.00s)
--- PASS: TestPancakeQueryGeneration/date_histogram:_regression_test(file:agg_req,nr:15)(15) (0.00s)
--- PASS: TestPancakeQueryGeneration/simple_terms,_seen_at_client's(file:agg_req,nr:16)(16) (0.00s)
--- PASS: TestPancakeQueryGeneration/triple_nested_aggs(file:agg_req,nr:17)(17) (0.00s)
--- PASS: TestPancakeQueryGeneration/complex_filters(file:agg_req,nr:18)(18) (0.00s)
--- PASS: TestPancakeQueryGeneration/random_sampler,_from_Explorer_>_Field_statistics(file:agg_req,nr:19)(19) (0.00s)
--- PASS: TestPancakeQueryGeneration/Field_statistics_>_summary_for_numeric_fields(file:agg_req,nr:20)(20) (0.00s)
--- PASS: TestPancakeQueryGeneration/range_bucket_aggregation,_both_keyed_and_not(file:agg_req,nr:21)(21) (0.00s)
--- SKIP: TestPancakeQueryGeneration/date_range_aggregation(file:agg_req,nr:22)(22) (0.00s)
--- PASS: TestPancakeQueryGeneration/significant_terms_aggregation:_same_as_terms_for_now(file:agg_req,nr:23)(23) (0.00s)
--- PASS: TestPancakeQueryGeneration/meta_field_in_aggregation(file:agg_req,nr:24)(24) (0.00s)
--- PASS: TestPancakeQueryGeneration/simple_histogram,_but_min_doc_count:_0(file:agg_req,nr:25)(25) (0.00s)
--- PASS: TestPancakeQueryGeneration/simple_date_histogram,_but_min_doc_count:_0(file:agg_req,nr:26)(26) (0.00s)
--- PASS: TestPancakeQueryGeneration/simple_date_histogram,_but_min_doc_count:_0(file:agg_req,nr:27)(27) (0.00s)
--- PASS: TestPancakeQueryGeneration/Terms,_completely_different_tree_results_from_2_queries_-_merging_them_didn't_work_before(file:agg_req,nr:28)(28) (0.00s)
--- PASS: TestPancakeQueryGeneration/Terms,_completely_different_tree_results_from_2_queries_-_merging_them_didn't_work_before_(logs)_TODO_add_results(file:agg_req,nr:29)(29) (0.00s)
--- PASS: TestPancakeQueryGeneration/Terms,_completely_different_tree_results_from_2_queries_-_merging_them_didn't_work_before_(logs)._what_when_cardinality_=_0?(file:agg_req,nr:30)(30) (0.00s)
--- PASS: TestPancakeQueryGeneration/Kibana_Visualize_->_Last_Value._Used_to_panic(file:agg_req,nr:31)(31) (0.00s)
--- PASS: TestPancakeQueryGeneration/Standard_deviation(file:agg_req,nr:32)(32) (0.00s)
--- PASS: TestPancakeQueryGeneration/0_result_rows_in_2x_terms(file:agg_req,nr:33)(33) (0.00s)
--- PASS: TestPancakeQueryGeneration/0_result_rows_in_3x_terms(file:agg_req,nr:34)(34) (0.00s)
--- PASS: TestPancakeQueryGeneration/0_result_rows_in_terms+histogram(file:agg_req,nr:35)(35) (0.00s)
--- PASS: TestPancakeQueryGeneration/0_result_rows_in_terms+histogram_+_meta_field(file:agg_req,nr:36)(36) (0.00s)
--- PASS: TestPancakeQueryGeneration/0_result_rows_in_terms+histogram_+_meta_field,_meta_in_subaggregation(file:agg_req,nr:37)(37) (0.00s)
--- PASS: TestPancakeQueryGeneration/simplest_top_metrics,_no_sort(file:agg_req,nr:38)(38) (0.00s)
--- PASS: TestPancakeQueryGeneration/simplest_top_metrics,_with_sort(file:agg_req,nr:39)(39) (0.00s)
--- PASS: TestPancakeQueryGeneration/terms_ordered_by_subaggregation(file:agg_req,nr:40)(40) (0.00s)
--- PASS: TestPancakeQueryGeneration/0_result_rows_in_2x_terms(file:agg_req,nr:41)(41) (0.00s)
--- PASS: TestPancakeQueryGeneration/histogram_with_all_possible_calendar_intervals(file:agg_req_2,nr:0)(42) (0.01s)
--- PASS: TestPancakeQueryGeneration/Percentiles_with_another_metric_aggregation._It_might_get_buggy_after_introducing_pancakes.(file:agg_req_2,nr:1)(43) (0.00s)
--- PASS: TestPancakeQueryGeneration/2x_terms_with_nulls_1/4,_nulls_in_second_aggregation,_with_missing_parameter(file:agg_req_2,nr:2)(44) (0.00s)
--- PASS: TestPancakeQueryGeneration/2x_terms_with_nulls_2/4,_nulls_in_the_second_aggregation,_but_no_missing_parameter(file:agg_req_2,nr:3)(45) (0.00s)
--- PASS: TestPancakeQueryGeneration/2x_terms_with_nulls_3/4,_nulls_in_the_first_aggregation,_with_missing_parameter(file:agg_req_2,nr:4)(46) (0.00s)
--- PASS: TestPancakeQueryGeneration/2x_terms_with_nulls_4/4,_nulls_in_the_first_aggregation,_without_missing_parameter(file:agg_req_2,nr:5)(47) (0.00s)
--- PASS: TestPancakeQueryGeneration/2x_date_histogram(file:agg_req_2,nr:6)(48) (0.00s)
--- PASS: TestPancakeQueryGeneration/2x_histogram(file:agg_req_2,nr:7)(49) (0.00s)
--- PASS: TestPancakeQueryGeneration/2x_histogram_with_min_doc_count_0(file:agg_req_2,nr:8)(50) (0.00s)
--- PASS: TestPancakeQueryGeneration/2x_terms_with_sampler_in_the_middle(file:agg_req_2,nr:9)(51) (0.00s)
--- PASS: TestPancakeQueryGeneration/2x_terms_with_random_sampler_in_the_middle(file:agg_req_2,nr:10)(52) (0.00s)
--- PASS: TestPancakeQueryGeneration/terms_order_by_quantile,_simplest_-_only_one_percentile(file:agg_req_2,nr:11)(53) (0.00s)
--- PASS: TestPancakeQueryGeneration/terms_order_by_quantile_-_more_percentiles(file:agg_req_2,nr:12)(54) (0.00s)
--- PASS: TestPancakeQueryGeneration/terms_order_by_percentile_ranks(file:agg_req_2,nr:13)(55) (0.00s)
--- PASS: TestPancakeQueryGeneration/simple_histogram_with_null_values,_no_missing_parameter(file:agg_req_2,nr:14)(56) (0.00s)
--- PASS: TestPancakeQueryGeneration/histogram_with_null_values,_no_missing_parameter,_and_some_subaggregation(file:agg_req_2,nr:15)(57) (0.00s)
--- PASS: TestPancakeQueryGeneration/simple_histogram_with_null_values_and_missing_parameter(file:agg_req_2,nr:16)(58) (0.00s)
--- PASS: TestPancakeQueryGeneration/histogram_with_null_values,_missing_parameter,_and_some_subaggregation(file:agg_req_2,nr:17)(59) (0.00s)
--- PASS: TestPancakeQueryGeneration/simple_date_histogram_with_null_values,_no_missing_parameter_(DateTime)(file:agg_req_2,nr:18)(60) (0.00s)
--- PASS: TestPancakeQueryGeneration/date_histogram_with_null_values,_no_missing_parameter,_and_some_subaggregation(file:agg_req_2,nr:19)(61) (0.00s)
--- PASS: TestPancakeQueryGeneration/date_histogram_with_null_values,_missing_parameter_(DateTime,_not_DateTime64),_and_some_subaggregation(file:agg_req_2,nr:20)(62) (0.00s)
--- PASS: TestPancakeQueryGeneration/date_histogram_with_missing,_different_formats,_and_types_(DateTime/DateTime64)(file:agg_req_2,nr:21)(63) (0.00s)
--- PASS: TestPancakeQueryGeneration/histogram,_min_doc_count=0,_int_keys_when_interval=1(file:agg_req_2,nr:22)(64) (0.00s)
--- PASS: TestPancakeQueryGeneration/simplest_composite:_1_terms(file:agg_req_2,nr:23)(65) (0.00s)
--- PASS: TestPancakeQueryGeneration/simplest_composite:_1_histogram_(with_size)(file:agg_req_2,nr:24)(66) (0.00s)
--- PASS: TestPancakeQueryGeneration/simplest_composite:_1_date_histogram(file:agg_req_2,nr:25)(67) (0.00s)
--- PASS: TestPancakeQueryGeneration/simplest_composite:_1_geotile_grid(file:agg_req_2,nr:26)(68) (0.00s)
--- PASS: TestPancakeQueryGeneration/composite:_2_sources_+_1_subaggregation(file:agg_req_2,nr:27)(69) (0.00s)
--- PASS: TestPancakeQueryGeneration/simplest_terms_with_exclude_(array_of_values)(file:agg_req_2,nr:28)(70) (0.00s)
--- PASS: TestPancakeQueryGeneration/simplest_terms_with_exclude_(single_value,_no_regex)(file:agg_req_2,nr:29)(71) (0.00s)
--- PASS: TestPancakeQueryGeneration/simplest_terms_with_exclude_(empty_array)(file:agg_req_2,nr:30)(72) (0.00s)
--- PASS: TestPancakeQueryGeneration/simplest_terms_with_exclude_(of_strings),_regression_test(file:agg_req_2,nr:31)(73) (0.00s)
--- PASS: TestPancakeQueryGeneration/terms_with_exclude_(more_complex,_string_field_with_exclude_regex)(file:agg_req_2,nr:32)(74) (0.00s)
--- PASS: TestPancakeQueryGeneration/complex_terms_with_exclude:_nested_terms_+_2_metrics(file:agg_req_2,nr:33)(75) (0.00s)
--- PASS: TestPancakeQueryGeneration/terms_with_exclude,_but_with_branched_off_aggregation_tree(file:agg_req_2,nr:34)(76) (0.00s)
--- PASS: TestPancakeQueryGeneration/terms_with_bool_field(file:agg_req_2,nr:35)(77) (0.00s)
--- PASS: TestPancakeQueryGeneration/Escaping_of_',_\,_\n,_and_\t_in_some_example_aggregations._No_tests_for_other_escape_characters,_e.g._\r_or_'b._Add_if_needed.(file:agg_req_2,nr:36)(78) (0.00s)
--- PASS: TestPancakeQueryGeneration/simple_max/min_aggregation_as_2_siblings(file:dates,nr:0)(79) (0.00s)
--- PASS: TestPancakeQueryGeneration/extended_bounds_pre_keys_(timezone_calculations_most_tricky_to_get_right)(file:dates,nr:1)(80) (0.00s)
--- PASS: TestPancakeQueryGeneration/extended_bounds_post_keys_(timezone_calculations_most_tricky_to_get_right)(file:dates,nr:2)(81) (0.00s)
--- PASS: TestPancakeQueryGeneration/empty_results,_we_still_should_add_empty_buckets,_because_of_the_extended_bounds_and_min_doc_count_defaulting_to_0(file:dates,nr:3)(82) (0.00s)
--- PASS: TestPancakeQueryGeneration/date_histogram_add_in-between_rows,_calendar_interval:_>=_month_(regression_test)(file:dates,nr:4)(83) (0.00s)
--- PASS: TestPancakeQueryGeneration/date_histogram_add_in-between_rows,_calendar_interval:_>=_month_(regression_test)(file:dates,nr:5)(84) (0.00s)
--- PASS: TestPancakeQueryGeneration/date_histogram_add_in-between_rows,_calendar_interval:_>=_month_(regression_test)(file:dates,nr:6)(85) (0.00s)
--- PASS: TestPancakeQueryGeneration/turing_1_-_painless_script_in_terms(file:dates,nr:7)(86) (0.00s)
--- PASS: TestPancakeQueryGeneration/Range_with_subaggregations._Reproduce:_Visualize_->_Pie_chart_->_Aggregation:_Unique_Count,_Buckets:_Aggregation:_Range(file:opensearch-visualize/agg_req,nr:0)(87) (0.00s)
--- PASS: TestPancakeQueryGeneration/Range_with_subaggregations._Reproduce:_Visualize_->_Pie_chart_->_Aggregation:_Top_Hit,_Buckets:_Aggregation:_Range(file:opensearch-visualize/agg_req,nr:1)(88) (0.00s)
--- PASS: TestPancakeQueryGeneration/Range_with_subaggregations._Reproduce:_Visualize_->_Pie_chart_->_Aggregation:_Sum,_Buckets:_Aggregation:_Range(file:opensearch-visualize/agg_req,nr:2)(89) (0.00s)
--- PASS: TestPancakeQueryGeneration/Range_with_subaggregations._Reproduce:_Visualize_->_Heat_Map_->_Metrics:_Median,_Buckets:_X-Asis_Range(file:opensearch-visualize/agg_req,nr:3)(90) (0.00s)
--- PASS: TestPancakeQueryGeneration/Max_on_DateTime_field._Reproduce:_Visualize_->_Line:_Metrics_->_Max_@timestamp,_Buckets:_Add_X-Asis,_Aggregation:_Significant_Terms(file:opensearch-visualize/agg_req,nr:4)(91) (0.00s)
--- PASS: TestPancakeQueryGeneration/Min_on_DateTime_field._Reproduce:_Visualize_->_Line:_Metrics_->_Min_@timestamp,_Buckets:_Add_X-Asis,_Aggregation:_Significant_Terms(file:opensearch-visualize/agg_req,nr:5)(92) (0.00s)
--- PASS: TestPancakeQueryGeneration/Percentiles_on_DateTime_field._Reproduce:_Visualize_->_Line:_Metrics_->_Percentiles_(or_Median,_it's_the_same_aggregation)_@timestamp,_Buckets:_Add_X-Asis,_Aggregation:_Significant_Terms(file:opensearch-visualize/agg_req,nr:6)(93) (0.00s)
--- PASS: TestPancakeQueryGeneration/Percentile_ranks_keyed=false._Reproduce:_Visualize_->_Line_->_Metrics:_Percentile_Ranks,_Buckets:_X-Asis_Date_Histogram(file:opensearch-visualize/agg_req,nr:7)(94) (0.00s)
--- PASS: TestPancakeQueryGeneration/Min/max_with_simple_script._Reproduce:_Visualize_->_Line_->_Metrics:_Count,_Buckets:_X-Asis_Histogram(file:opensearch-visualize/agg_req,nr:8)(95) (0.00s)
--- PASS: TestPancakeQueryGeneration/Histogram_with_simple_script._Reproduce:_Visualize_->_Line_->_Metrics:_Count,_Buckets:_X-Asis_Histogram(file:opensearch-visualize/agg_req,nr:9)(96) (0.00s)
--- PASS: TestPancakeQueryGeneration/dashboard-1:_latency_by_region(file:dashboard-1/agg_req,nr:0)(97) (0.00s)
--- PASS: TestPancakeQueryGeneration/dashboard-1:_bug,_used_to_be_infinite_loop(file:dashboard-1/agg_req,nr:1)(98) (0.00s)
--- PASS: TestPancakeQueryGeneration/Simplest_cumulative_sum_(count)._Reproduce:_Visualize_->_Vertical_Bar:_Metrics:_Cumulative_Sum_(Aggregation:_Count),_Buckets:_Histogram(file:opensearch-visualize/pipeline_agg_req,nr:0)(99) (0.00s)
--- PASS: TestPancakeQueryGeneration/Cumulative_sum_with_other_aggregation._Reproduce:_Visualize_->_Vertical_Bar:_Metrics:_Cumulative_Sum_(Aggregation:_Average),_Buckets:_Histogram(file:opensearch-visualize/pipeline_agg_req,nr:1)(100) (0.00s)
--- PASS: TestPancakeQueryGeneration/Cumulative_sum_to_other_cumulative_sum._Reproduce:_Visualize_->_Vertical_Bar:_Metrics:_Cumulative_Sum_(Aggregation:_Cumulative_Sum_(Aggregation:_Count)),_Buckets:_Histogram(file:opensearch-visualize/pipeline_agg_req,nr:2)(101) (0.00s)
--- PASS: TestPancakeQueryGeneration/Cumulative_sum_-_quite_complex,_a_graph_of_pipelines._Reproduce:_Visualize_->_Vertical_Bar:_Metrics:_Cumulative_Sum_(Aggregation:_Cumulative_Sum_(Aggregation:_Max)),_Buckets:_Histogram(file:opensearch-visualize/pipeline_agg_req,nr:3)(102) (0.00s)
--- PASS: TestPancakeQueryGeneration/Simplest_Derivative_(count)._Reproduce:_Visualize_->_Vertical_Bar:_Metrics:_Derivative_(Aggregation:_Count),_Buckets:_Histogram(file:opensearch-visualize/pipeline_agg_req,nr:4)(103) (0.00s)
--- PASS: TestPancakeQueryGeneration/Derivative_with_other_aggregation._Reproduce:_Visualize_->_Vertical_Bar:_Metrics:_Derivative_(Aggregation:_Sum),_Buckets:_Date_Histogram(file:opensearch-visualize/pipeline_agg_req,nr:5)(104) (0.00s)
--- PASS: TestPancakeQueryGeneration/Derivative_to_cumulative_sum._Reproduce:_Visualize_->_Vertical_Bar:_Metrics:_Derivative_(Aggregation:_Cumulative_Sum_(Aggregation:_Count)),_Buckets:_Date_Histogram(file:opensearch-visualize/pipeline_agg_req,nr:6)(105) (0.00s)
--- PASS: TestPancakeQueryGeneration/Simplest_Serial_Diff_(count),_lag=default_(1)._Reproduce:_Visualize_->_Vertical_Bar:_Metrics:_Serial_Diff_(Aggregation:_Count),_Buckets:_Histogram(file:opensearch-visualize/pipeline_agg_req,nr:7)(106) (0.00s)
--- PASS: TestPancakeQueryGeneration/Simplest_Serial_Diff_(count),_lag=2._Don't_know_how_to_reproduce_in_OpenSearch,_but_you_can_click_out:Reproduce:_Visualize_->_Vertical_Bar:_Metrics:_Serial_Diff_(Aggregation:_Count),_Buckets:_HistogramAnd_then_change_the_request_manually(file:opensearch-visualize/pipeline_agg_req,nr:8)(107) (0.00s)
--- PASS: TestPancakeQueryGeneration/Serial_diff_with_other_aggregation._Reproduce:_Visualize_->_Vertical_Bar:_Metrics:_Serial_Diff_(Aggregation:_Sum),_Buckets:_Date_Histogram(file:opensearch-visualize/pipeline_agg_req,nr:9)(108) (0.00s)
--- PASS: TestPancakeQueryGeneration/Serial_Diff_to_cumulative_sum._Reproduce:_Visualize_->_Vertical_Bar:_Metrics:_Serial_Diff_(Aggregation:_Cumulative_Sum_(Aggregation:_Count)),_Buckets:_Date_Histogram(file:opensearch-visualize/pipeline_agg_req,nr:10)(109) (0.00s)
--- PASS: TestPancakeQueryGeneration/Simplest_avg_bucket._Reproduce:_Visualize_->_Vertical_Bar:_Metrics:_Average_Bucket_(Bucket:_Date_Histogram,_Metric:_Count)(file:opensearch-visualize/pipeline_agg_req,nr:11)(110) (0.00s)
--- PASS: TestPancakeQueryGeneration/avg_bucket._Reproduce:_Visualize_->_Vertical_Bar:_Metrics:_Average_Bucket_(Bucket:_Date_Histogram,_Metric:_Max)(file:opensearch-visualize/pipeline_agg_req,nr:12)(111) (0.00s)
--- PASS: TestPancakeQueryGeneration/avg_bucket._Reproduce:_Visualize_->_Horizontal_Bar:_Metrics:_Average_Bucket_(Bucket:_Histogram,_Metric:_Count),_Buckets:_X-Asis:_Date_Histogram(file:opensearch-visualize/pipeline_agg_req,nr:13)(112) (0.00s)
--- PASS: TestPancakeQueryGeneration/Simplest_min_bucket._Reproduce:_Visualize_->_Vertical_Bar:_Metrics:_Min_Bucket_(Bucket:_Terms,_Metric:_Count)(file:opensearch-visualize/pipeline_agg_req,nr:14)(113) (0.00s)
--- PASS: TestPancakeQueryGeneration/min_bucket._Reproduce:_Visualize_->_Vertical_Bar:_Metrics:_Min_Bucket_(Bucket:_Terms,_Metric:_Unique_Count)(file:opensearch-visualize/pipeline_agg_req,nr:15)(114) (0.00s)
--- PASS: TestPancakeQueryGeneration/complex_min_bucket._Reproduce:_Visualize_->_Vertical_Bar:_Metrics:_Min_Bucket_(Bucket:_Terms,_Metric:_Sum),_Buckets:_Split_Series:_Histogram(file:opensearch-visualize/pipeline_agg_req,nr:16)(115) (0.00s)
--- PASS: TestPancakeQueryGeneration/Simplest_max_bucket._Reproduce:_Visualize_->_Line:_Metrics:_Max_Bucket_(Bucket:_Terms,_Metric:_Count)(file:opensearch-visualize/pipeline_agg_req,nr:17)(116) (0.00s)
--- PASS: TestPancakeQueryGeneration/Max/Sum_bucket_with_some_null_buckets._Reproduce:_Visualize_->_Vertical_Bar:_Metrics:_Max_(Sum)_Bucket_(Aggregation:_Date_Histogram,_Metric:_Min)(file:opensearch-visualize/pipeline_agg_req,nr:18)(117) (0.00s)
--- PASS: TestPancakeQueryGeneration/Different_pipeline_aggrs_with_some_null_buckets._Reproduce:_Visualize_->_Vertical_Bar:_Metrics:_Max/Sum_Bucket/etc._(Aggregation:_Histogram,_Metric:_Max)(file:opensearch-visualize/pipeline_agg_req,nr:19)(118) (0.00s)
--- SKIP: TestPancakeQueryGeneration/max_bucket._Reproduce:_Visualize_->_Line:_Metrics:_Max_Bucket_(Bucket:_Filters,_Metric:_Sum)(file:opensearch-visualize/pipeline_agg_req,nr:20)(119) (0.00s)
--- SKIP: TestPancakeQueryGeneration/complex_max_bucket._Reproduce:_Visualize_->_Line:_Metrics:_Max_Bucket_(Bucket:_Filters,_Metric:_Sum),_Buckets:_Split_chart:_Rows_->_Range(file:opensearch-visualize/pipeline_agg_req,nr:21)(120) (0.00s)
--- PASS: TestPancakeQueryGeneration/Simplest_sum_bucket._Reproduce:_Visualize_->_Horizontal_Bar:_Metrics:_Sum_Bucket_(B_ucket:_Terms,_Metric:_Count)(file:opensearch-visualize/pipeline_agg_req,nr:22)(121) (0.00s)
--- PASS: TestPancakeQueryGeneration/sum_bucket._Reproduce:_Visualize_->_Horizontal_Bar:_Metrics:_Sum_Bucket_(Bucket:_Significant_Terms,_Metric:_Average)(file:opensearch-visualize/pipeline_agg_req,nr:23)(122) (0.00s)
--- PASS: TestPancakeQueryGeneration/complex_sum_bucket._Reproduce:_Visualize_->_Vertical_Bar:_Metrics:_Sum_Bucket_(Bucket:_Date_Histogram,_Metric:_Average),_Buckets:_X-Asis:_Histogram(file:opensearch-visualize/pipeline_agg_req,nr:24)(123) (0.01s)
--- PASS: TestPancakeQueryGeneration/Multi_terms_without_subaggregations._Visualize:_Bar_Vertical:_Horizontal_Axis:_Date_Histogram,_Vertical_Axis:_Count_of_records,_Breakdown:_Top_values_(2_values)(file:kibana-visualize/agg_req,nr:0)(124) (0.00s)
--- PASS: TestPancakeQueryGeneration/Multi_terms_with_simple_count._Visualize:_Bar_Vertical:_Horizontal_Axis:_Top_values_(2_values),_Vertical:_Count_of_records,_Breakdown:_@timestamp(file:kibana-visualize/agg_req,nr:1)(125) (0.00s)
--- PASS: TestPancakeQueryGeneration/Multi_terms_with_double-nested_subaggregations._Visualize:_Bar_Vertical:_Horizontal_Axis:_Top_values_(2_values),_Vertical:_Unique_count,_Breakdown:_@timestamp(file:kibana-visualize/agg_req,nr:2)(126) (0.00s)
--- PASS: TestPancakeQueryGeneration/Quite_simple_multi_terms,_but_with_non-string_keys._Visualize:_Bar_Vertical:_Horizontal_Axis:_Date_Histogram,_Vertical_Axis:_Count_of_records,_Breakdown:_Top_values_(2_values)(file:kibana-visualize/agg_req,nr:3)(127) (0.00s)
--- PASS: TestPancakeQueryGeneration/percentile_with_subaggregation_(so,_combinator)._Visualize,_Pie,_Slice_by:_top5_of_Cancelled,_DistanceKilometers,_Metric:_95th_Percentile(file:kibana-visualize/agg_req,nr:4)(128) (0.00s)
--- PASS: TestPancakeQueryGeneration/terms_with_order_by_agg1>agg2_(multiple_aggregations)(file:kibana-visualize/agg_req,nr:5)(129) (0.00s)
--- PASS: TestPancakeQueryGeneration/terms_with_order_by_stats,_easily_reproducible_in_Kibana_Visualize(file:kibana-visualize/agg_req,nr:6)(130) (0.00s)
--- PASS: TestPancakeQueryGeneration/terms_with_order_by_extended_stats_(easily_reproducible_in_Kibana_Visualize)(file:kibana-visualize/agg_req,nr:7)(131) (0.00s)
--- SKIP: TestPancakeQueryGeneration/Terms_with_order_by_top_metrics(file:kibana-visualize/agg_req,nr:8)(132) (0.00s)
--- SKIP: TestPancakeQueryGeneration/Line,_Y-axis:_Min,_Buckets:_Date_Range,_X-Axis:_Terms,_Split_Chart:_Date_Histogram(file:kibana-visualize/agg_req,nr:9)(133) (0.00s)
--- PASS: TestPancakeQueryGeneration/simplest_IP_Prefix_(Kibana_8.13+),_ipv4_field,_prefix_length=0(file:kibana-visualize/agg_req,nr:10)(134) (0.00s)
--- PASS: TestPancakeQueryGeneration/simplest_IP_Prefix_(Kibana_8.13+),_ipv4_field,_prefix_length=1(file:kibana-visualize/agg_req,nr:11)(135) (0.00s)
--- PASS: TestPancakeQueryGeneration/simplest_IP_Prefix_(Kibana_8.13+),_ipv4_field,_prefix_length=10(file:kibana-visualize/agg_req,nr:12)(136) (0.00s)
--- PASS: TestPancakeQueryGeneration/simplest_IP_Prefix_(Kibana_8.13+),_ipv4_field,_prefix_length=32(file:kibana-visualize/agg_req,nr:13)(137) (0.00s)
--- PASS: TestPancakeQueryGeneration/simplest_IP_Prefix_(Kibana_8.13+),_ipv4_field,_keyed=true(file:kibana-visualize/agg_req,nr:14)(138) (0.00s)
--- PASS: TestPancakeQueryGeneration/simplest_IP_Prefix_(Kibana_8.13+),_ipv4_field,_append_prefix_length=true(file:kibana-visualize/agg_req,nr:15)(139) (0.00s)
--- PASS: TestPancakeQueryGeneration/simplest_IP_Prefix_(Kibana_8.13+),_ipv4_field,_keyed=true,_append_prefix_length=true(file:kibana-visualize/agg_req,nr:16)(140) (0.00s)
--- PASS: TestPancakeQueryGeneration/IP_Prefix_with_other_aggregations(file:kibana-visualize/agg_req,nr:17)(141) (0.00s)
--- PASS: TestPancakeQueryGeneration/simplest_IP_Prefix_(Kibana_8.13+),_ipv6_field,_prefix_length=0(file:kibana-visualize/agg_req,nr:18)(142) (0.00s)
--- PASS: TestPancakeQueryGeneration/simplest_IP_Prefix_(Kibana_8.13+),_ipv6_field,_prefix_length=128(file:kibana-visualize/agg_req,nr:19)(143) (0.00s)
--- PASS: TestPancakeQueryGeneration/IP_Prefix_(Kibana_8.13+),_ipv6_field,_keyed=true_and_overflow_of_1<<(128-prefix_length)(file:kibana-visualize/agg_req,nr:20)(144) (0.00s)
--- PASS: TestPancakeQueryGeneration/simple_IP_Prefix_(Kibana_8.13+),_ipv6_field,_non-zero&_and_non-ipv4_key(file:kibana-visualize/agg_req,nr:21)(145) (0.00s)
--- PASS: TestPancakeQueryGeneration/IP_Prefix_(Kibana_8.13+),_ipv6_field,_multiple_keys_and_append_prefix_length=true(file:kibana-visualize/agg_req,nr:22)(146) (0.00s)
--- PASS: TestPancakeQueryGeneration/IP_Prefix_(Kibana_8.13+),_ipv6_field,_multiple_keys_and_append_prefix_length=true(file:kibana-visualize/agg_req,nr:23)(147) (0.00s)
--- PASS: TestPancakeQueryGeneration/Simplest_IP_range._In_Kibana:_Add_panel_>_Aggregation_Based_>_Area._Buckets:_X-asis:_IP_Range(file:kibana-visualize/agg_req,nr:24)(148) (0.00s)
--- PASS: TestPancakeQueryGeneration/IP_range,_with_ranges_as_CIDR_masks._In_Kibana:_Add_panel_>_Aggregation_Based_>_Area._Buckets:_X-asis:_IP_Range(file:kibana-visualize/agg_req,nr:25)(149) (0.00s)
--- PASS: TestPancakeQueryGeneration/IP_range,_with_ranges_as_CIDR_masks,_keyed=true._In_Kibana:_Add_panel_>_Aggregation_Based_>_Area._Buckets:_X-asis:_IP_Range(file:kibana-visualize/agg_req,nr:26)(150) (0.00s)
--- PASS: TestPancakeQueryGeneration/IP_range_ipv6(file:kibana-visualize/agg_req,nr:27)(151) (0.00s)
--- PASS: TestPancakeQueryGeneration/IP_range_ipv6_with_mask(file:kibana-visualize/agg_req,nr:28)(152) (0.00s)
--- PASS: TestPancakeQueryGeneration/Sum_bucket_for_dates(file:kibana-visualize/pipeline_agg_req,nr:0)(153) (0.00s)
--- PASS: TestPancakeQueryGeneration/Reproduce:_Visualize_->_Vertical_Bar:_Metrics:_Cumulative_Sum_(Aggregation:_Avg),_Buckets:_Date_Histogram(file:kibana-visualize/pipeline_agg_req,nr:1)(154) (0.00s)
--- PASS: TestPancakeQueryGeneration/Reproduce:_Visualize_->_Vertical_Bar:_Metrics:_Cumulative_Sum_(Aggregation:_Cumulative_Sum_(Aggregation:_Count)),_Buckets:_Date_Histogram(file:kibana-visualize/pipeline_agg_req,nr:2)(155) (0.00s)
--- PASS: TestPancakeQueryGeneration/Reproduce:_Visualize_->_Vertical_Bar:_Metrics:_Cumulative_Sum_(Aggregation:_Count),_Buckets:_Histogram(need_add_empty_rows,_even_though_there's_no_min_doc_count=0)(file:kibana-visualize/pipeline_agg_req,nr:3)(156) (0.00s)
--- SKIP: TestPancakeQueryGeneration/clients/kunkka/test_0,_used_to_be_broken_before_aggregations_merge_fixOutput_more_or_less_works,_but_is_different_and_worse_than_what_Elastic_returns.If_it_starts_failing,_maybe_that's_a_good_thing(file:clients/kunkka,nr:0)(157) (0.00s)
--- PASS: TestPancakeQueryGeneration/it's_the_same_input_as_in_previous_test,_but_with_the_original_output_from_Elastic.Skipped_for_now,_as_our_response_is_different_in_2_things:_key_as_string_date_(probably_not_important)_+_we_don't_return_0's_(e.g._doc_count:_0).If_we_need_clients/kunkka/test_0,_used_to_be_broken_before_aggregations_merge_fix(file:clients/kunkka,nr:1)(158) (0.00s)
--- SKIP: TestPancakeQueryGeneration/clients/kunkka/test_1,_used_to_be_broken_before_aggregations_merge_fix(file:clients/kunkka,nr:2)(159) (0.00s)
--- PASS: TestPancakeQueryGeneration/Ophelia_Test_1:_triple_terms_+_default_order(file:clients/ophelia,nr:0)(160) (0.00s)
--- PASS: TestPancakeQueryGeneration/Ophelia_Test_2:_triple_terms_+_other_aggregations_+_default_order(file:clients/ophelia,nr:1)(161) (0.00s)
--- PASS: TestPancakeQueryGeneration/Ophelia_Test_3:_5x_terms_+_a_lot_of_other_aggregations(file:clients/ophelia,nr:2)(162) (0.01s)
--- PASS: TestPancakeQueryGeneration/Ophelia_Test_4:_triple_terms_+_order_by_another_aggregations(file:clients/ophelia,nr:3)(163) (0.00s)
--- PASS: TestPancakeQueryGeneration/Ophelia_Test_5:_4x_terms_+_order_by_another_aggregations(file:clients/ophelia,nr:4)(164) (0.00s)
--- PASS: TestPancakeQueryGeneration/Ophelia_Test_6:_triple_terms_+_other_aggregations_+_order_by_another_aggregations(file:clients/ophelia,nr:5)(165) (0.00s)
--- PASS: TestPancakeQueryGeneration/Ophelia_Test_7:_5x_terms_+_a_lot_of_other_aggregations_+_different_order_bys(file:clients/ophelia,nr:6)(166) (0.01s)
--- PASS: TestPancakeQueryGeneration/todo(file:clients/clover,nr:0)(167) (0.00s)
--- PASS: TestPancakeQueryGeneration/multiple_buckets_path(file:clients/clover,nr:1)(168) (0.00s)
--- PASS: TestPancakeQueryGeneration/simplest_auto_date_histogram(file:clients/clover,nr:2)(169) (0.00s)
--- PASS: TestPancakeQueryGeneration/bucket_script_with_multiple_buckets_path(file:clients/clover,nr:3)(170) (0.00s)
--- PASS: TestPancakeQueryGeneration/todo(file:clients/clover,nr:4)(171) (0.00s)
--- PASS: TestPancakeQueryGeneration/todo(file:clients/clover,nr:5)(172) (0.00s)
--- PASS: TestPancakeQueryGeneration/Clover(file:clients/clover,nr:6)(173) (0.00s)
--- PASS: TestPancakeQueryGeneration/Weird_aggregation_and_filter_names(file:clients/clover,nr:7)(174) (0.00s)
--- PASS: TestPancakeQueryGeneration/empty_results(file:clients/turing,nr:0)(175) (0.00s)
=== RUN   TestPancakeQueryGeneration_halfpancake
=== RUN   TestPancakeQueryGeneration_halfpancake/test1
=== RUN   TestPancakeQueryGeneration_halfpancake/test2
--- PASS: TestPancakeQueryGeneration_halfpancake (0.01s)
--- PASS: TestPancakeQueryGeneration_halfpancake/test1 (0.01s)
--- PASS: TestPancakeQueryGeneration_halfpancake/test2 (0.00s)
=== RUN   Test_pancakeTranslateFromAggregationToLayered
=== RUN   Test_pancakeTranslateFromAggregationToLayered/one_bucket_aggregation
=== RUN   Test_pancakeTranslateFromAggregationToLayered/bucket_in_bucket__..._
=== RUN   Test_pancakeTranslateFromAggregationToLayered/one_bucket_aggregation_with_metrics_aggregations_
=== RUN   Test_pancakeTranslateFromAggregationToLayered/one_bucket_aggregation_with_metrics_aggregations_and_bucket_aggregations
=== RUN   Test_pancakeTranslateFromAggregationToLayered/one_bucket_aggregation_with_metrics_aggregations_and_bucket_aggregations#01
--- PASS: Test_pancakeTranslateFromAggregationToLayered (0.00s)
--- PASS: Test_pancakeTranslateFromAggregationToLayered/one_bucket_aggregation (0.00s)
--- PASS: Test_pancakeTranslateFromAggregationToLayered/bucket_in_bucket__..._ (0.00s)
--- PASS: Test_pancakeTranslateFromAggregationToLayered/one_bucket_aggregation_with_metrics_aggregations_ (0.00s)
--- PASS: Test_pancakeTranslateFromAggregationToLayered/one_bucket_aggregation_with_metrics_aggregations_and_bucket_aggregations (0.00s)
--- PASS: Test_pancakeTranslateFromAggregationToLayered/one_bucket_aggregation_with_metrics_aggregations_and_bucket_aggregations#01 (0.00s)
=== RUN   Test_pancakeNameCollision
--- PASS: Test_pancakeNameCollision (0.00s)
=== RUN   Test_pancakeNameCollisionHard
--- PASS: Test_pancakeNameCollisionHard (0.00s)
=== RUN   Test_parseRange
=== RUN   Test_parseRange/DateTime64
Feb 10 13:50:26.000 DBG field 'timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'timestamp' referenced, but not found in schema, falling back to original name
=== RUN   Test_parseRange/parseDateTimeBestEffort
Feb 10 13:50:26.000 DBG field 'timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'timestamp' referenced, but not found in schema, falling back to original name
=== RUN   Test_parseRange/numeric_range
Feb 10 13:50:26.000 DBG field 'time_taken' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'time_taken' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 WRN datetime field 'time_taken' not found in table '__quesma_table_name'
=== RUN   Test_parseRange/DateTime64#01
Feb 10 13:50:26.000 DBG field 'timestamp' referenced, but not found in schema, falling back to original name
Feb 10 13:50:26.000 DBG field 'timestamp' referenced, but not found in schema, falling back to original name
--- PASS: Test_parseRange (0.00s)
--- PASS: Test_parseRange/DateTime64 (0.00s)
--- PASS: Test_parseRange/parseDateTimeBestEffort (0.00s)
--- PASS: Test_parseRange/numeric_range (0.00s)
--- PASS: Test_parseRange/DateTime64#01 (0.00s)
=== RUN   TestQueryParserStringAttrConfig
=== RUN   TestQueryParserStringAttrConfig/Match_all(0)
=== RUN   TestQueryParserStringAttrConfig/Term_as_dictionary(1)
=== RUN   TestQueryParserStringAttrConfig/Term_as_array(2)
=== RUN   TestQueryParserStringAttrConfig/Sample_log_query(3)
=== RUN   TestQueryParserStringAttrConfig/Multiple_bool_query(4)
Feb 10 13:50:26.000 WRN datetime field 'age' not found in table 'logs-generic-default'
Feb 10 13:50:26.000 WRN datetime field 'age' not found in table 'logs-generic-default'
=== RUN   TestQueryParserStringAttrConfig/Match_phrase(5)
=== RUN   TestQueryParserStringAttrConfig/Match(6)
=== RUN   TestQueryParserStringAttrConfig/Terms(7)
=== RUN   TestQueryParserStringAttrConfig/Exists(8)
=== RUN   TestQueryParserStringAttrConfig/Simple_query_string(9)
=== RUN   TestQueryParserStringAttrConfig/Simple_query_string_wildcard(10)
=== RUN   TestQueryParserStringAttrConfig/Simple_wildcard(11)
=== RUN   TestQueryParserStringAttrConfig/Simple_prefix_ver1(12)
=== RUN   TestQueryParserStringAttrConfig/Simple_prefix_ver2(13)
=== RUN   TestQueryParserStringAttrConfig/Query_string,_wildcards_don't_work_properly(14)
=== RUN   TestQueryParserStringAttrConfig/Empty_bool(15)
=== RUN   TestQueryParserStringAttrConfig/Simplest_'match_phrase'(16)
=== RUN   TestQueryParserStringAttrConfig/More_nested_'match_phrase'(17)
=== RUN   TestQueryParserStringAttrConfig/Simple_nested(18)
=== RUN   TestQueryParserStringAttrConfig/random_simple_test(19)
=== RUN   TestQueryParserStringAttrConfig/termWithCompoundValue(20)
=== RUN   TestQueryParserStringAttrConfig/count(*)_as_/_search_query._With_filter(21)
=== RUN   TestQueryParserStringAttrConfig/count(*)_as_/_search_or_/logs-*-/_search_query._Without_filter(22)
=== RUN   TestQueryParserStringAttrConfig/count(*)_as_/_search_query._With_filter(23)
=== RUN   TestQueryParserStringAttrConfig/count(*)_as_/_search_or_/logs-*-/_search_query._Without_filter(24)
=== RUN   TestQueryParserStringAttrConfig/_search,_only_one_so_far_with_fields,_we're_not_sure_if_SELECT_*_is_correct,_or_should_be_SELECT_@timestamp(25)
=== RUN   TestQueryParserStringAttrConfig/Empty_must(26)
=== RUN   TestQueryParserStringAttrConfig/Empty_must_not(27)
=== RUN   TestQueryParserStringAttrConfig/Empty_should(28)
=== RUN   TestQueryParserStringAttrConfig/Empty_all_bools(29)
=== RUN   TestQueryParserStringAttrConfig/Some_bools_empty,_some_not(30)
=== RUN   TestQueryParserStringAttrConfig/Match_all_(empty_query)(31)
=== RUN   TestQueryParserStringAttrConfig/Constant_score_query(32)
=== RUN   TestQueryParserStringAttrConfig/Match_phrase_using__id_field(33)
=== RUN   TestQueryParserStringAttrConfig/Comments_in_filter(34)
=== RUN   TestQueryParserStringAttrConfig/Terms_with_range(35)
=== RUN   TestQueryParserStringAttrConfig/Simple_regexp_(can_be_simply_transformed_to_one_LIKE)(36)
=== RUN   TestQueryParserStringAttrConfig/Simple_regexp_(can_be_simply_transformed_to_one_LIKE),_with__,_which_needs_to_be_escaped(37)
=== RUN   TestQueryParserStringAttrConfig/Complex_regexp_1_(can't_be_transformed_to_LIKE)(38)
=== RUN   TestQueryParserStringAttrConfig/Complex_regexp_2_(can't_be_transformed_to_LIKE)(39)
=== RUN   TestQueryParserStringAttrConfig/Escaping_of_',_\,_\t_and_\n(40)
=== RUN   TestQueryParserStringAttrConfig/ids,_0_values(41)
=== RUN   TestQueryParserStringAttrConfig/ids,_1_value(42)
=== RUN   TestQueryParserStringAttrConfig/ids,_2+_values(43)
=== RUN   TestQueryParserStringAttrConfig/ids_with_DateTime64(9)_(trailing_zeroes)(44)
=== RUN   TestQueryParserStringAttrConfig/ids_with_DateTime64(9)_(no_trailing_zeroes)(45)
=== RUN   TestQueryParserStringAttrConfig/ids_with_DateTime64(0)(46)
=== RUN   TestQueryParserStringAttrConfig/ids_with_DateTime64(1)(47)
--- PASS: TestQueryParserStringAttrConfig (0.02s)
--- PASS: TestQueryParserStringAttrConfig/Match_all(0) (0.00s)
--- PASS: TestQueryParserStringAttrConfig/Term_as_dictionary(1) (0.00s)
--- PASS: TestQueryParserStringAttrConfig/Term_as_array(2) (0.00s)
--- PASS: TestQueryParserStringAttrConfig/Sample_log_query(3) (0.00s)
--- PASS: TestQueryParserStringAttrConfig/Multiple_bool_query(4) (0.00s)
--- PASS: TestQueryParserStringAttrConfig/Match_phrase(5) (0.00s)
--- PASS: TestQueryParserStringAttrConfig/Match(6) (0.00s)
--- PASS: TestQueryParserStringAttrConfig/Terms(7) (0.00s)
--- PASS: TestQueryParserStringAttrConfig/Exists(8) (0.00s)
--- PASS: TestQueryParserStringAttrConfig/Simple_query_string(9) (0.00s)
--- PASS: TestQueryParserStringAttrConfig/Simple_query_string_wildcard(10) (0.00s)
--- PASS: TestQueryParserStringAttrConfig/Simple_wildcard(11) (0.00s)
--- PASS: TestQueryParserStringAttrConfig/Simple_prefix_ver1(12) (0.00s)
--- PASS: TestQueryParserStringAttrConfig/Simple_prefix_ver2(13) (0.00s)
--- PASS: TestQueryParserStringAttrConfig/Query_string,_wildcards_don't_work_properly(14) (0.00s)
--- PASS: TestQueryParserStringAttrConfig/Empty_bool(15) (0.00s)
--- PASS: TestQueryParserStringAttrConfig/Simplest_'match_phrase'(16) (0.00s)
--- PASS: TestQueryParserStringAttrConfig/More_nested_'match_phrase'(17) (0.00s)
--- PASS: TestQueryParserStringAttrConfig/Simple_nested(18) (0.00s)
--- PASS: TestQueryParserStringAttrConfig/random_simple_test(19) (0.00s)
--- PASS: TestQueryParserStringAttrConfig/termWithCompoundValue(20) (0.00s)
--- PASS: TestQueryParserStringAttrConfig/count(*)_as_/_search_query._With_filter(21) (0.00s)
--- PASS: TestQueryParserStringAttrConfig/count(*)_as_/_search_or_/logs-*-/_search_query._Without_filter(22) (0.00s)
--- PASS: TestQueryParserStringAttrConfig/count(*)_as_/_search_query._With_filter(23) (0.00s)
--- PASS: TestQueryParserStringAttrConfig/count(*)_as_/_search_or_/logs-*-/_search_query._Without_filter(24) (0.00s)
--- PASS: TestQueryParserStringAttrConfig/_search,_only_one_so_far_with_fields,_we're_not_sure_if_SELECT_*_is_correct,_or_should_be_SELECT_@timestamp(25) (0.00s)
--- PASS: TestQueryParserStringAttrConfig/Empty_must(26) (0.00s)
--- PASS: TestQueryParserStringAttrConfig/Empty_must_not(27) (0.00s)
--- PASS: TestQueryParserStringAttrConfig/Empty_should(28) (0.00s)
--- PASS: TestQueryParserStringAttrConfig/Empty_all_bools(29) (0.00s)
--- PASS: TestQueryParserStringAttrConfig/Some_bools_empty,_some_not(30) (0.00s)
--- PASS: TestQueryParserStringAttrConfig/Match_all_(empty_query)(31) (0.00s)
--- PASS: TestQueryParserStringAttrConfig/Constant_score_query(32) (0.00s)
--- PASS: TestQueryParserStringAttrConfig/Match_phrase_using__id_field(33) (0.00s)
--- PASS: TestQueryParserStringAttrConfig/Comments_in_filter(34) (0.00s)
--- PASS: TestQueryParserStringAttrConfig/Terms_with_range(35) (0.00s)
--- PASS: TestQueryParserStringAttrConfig/Simple_regexp_(can_be_simply_transformed_to_one_LIKE)(36) (0.00s)
--- PASS: TestQueryParserStringAttrConfig/Simple_regexp_(can_be_simply_transformed_to_one_LIKE),_with__,_which_needs_to_be_escaped(37) (0.00s)
--- PASS: TestQueryParserStringAttrConfig/Complex_regexp_1_(can't_be_transformed_to_LIKE)(38) (0.00s)
--- PASS: TestQueryParserStringAttrConfig/Complex_regexp_2_(can't_be_transformed_to_LIKE)(39) (0.00s)
--- PASS: TestQueryParserStringAttrConfig/Escaping_of_',_\,_\t_and_\n(40) (0.00s)
--- PASS: TestQueryParserStringAttrConfig/ids,_0_values(41) (0.00s)
--- PASS: TestQueryParserStringAttrConfig/ids,_1_value(42) (0.00s)
--- PASS: TestQueryParserStringAttrConfig/ids,_2+_values(43) (0.00s)
--- PASS: TestQueryParserStringAttrConfig/ids_with_DateTime64(9)_(trailing_zeroes)(44) (0.00s)
--- PASS: TestQueryParserStringAttrConfig/ids_with_DateTime64(9)_(no_trailing_zeroes)(45) (0.00s)
--- PASS: TestQueryParserStringAttrConfig/ids_with_DateTime64(0)(46) (0.00s)
--- PASS: TestQueryParserStringAttrConfig/ids_with_DateTime64(1)(47) (0.00s)
=== RUN   TestQueryParserNoFullTextFields
=== RUN   TestQueryParserNoFullTextFields/0
Feb 10 13:50:26.000 WRN datetime field 'timestamp' not found in table '__quesma_table_name'
Feb 10 13:50:26.000 WRN datetime field 'timestamp' not found in table '__quesma_table_name'
--- PASS: TestQueryParserNoFullTextFields (0.00s)
--- PASS: TestQueryParserNoFullTextFields/0 (0.00s)
=== RUN   TestQueryParserNoAttrsConfig
=== RUN   TestQueryParserNoAttrsConfig/Test_empty_ANDs,_ORs_and_NOTs..._idk,_this_test_is_very_old_and_weird,_better_write_to_Krzysiek_if_it_fails_for_you
--- PASS: TestQueryParserNoAttrsConfig (0.00s)
--- PASS: TestQueryParserNoAttrsConfig/Test_empty_ANDs,_ORs_and_NOTs..._idk,_this_test_is_very_old_and_weird,_better_write_to_Krzysiek_if_it_fails_for_you (0.00s)
=== RUN   Test_parseSortFields
=== RUN   Test_parseSortFields/compound
=== RUN   Test_parseSortFields/empty
=== RUN   Test_parseSortFields/map[string]string
=== RUN   Test_parseSortFields/map[string]interface{}
=== RUN   Test_parseSortFields/[]map[string]string
--- PASS: Test_parseSortFields (0.00s)
--- PASS: Test_parseSortFields/compound (0.00s)
--- PASS: Test_parseSortFields/empty (0.00s)
--- PASS: Test_parseSortFields/map[string]string (0.00s)
--- PASS: Test_parseSortFields/map[string]interface{} (0.00s)
--- PASS: Test_parseSortFields/[]map[string]string (0.00s)
=== RUN   TestInvalidQueryRequests
query_parser_test.go:282: Test in the making. Need 1-2 more PRs in 'Report errors in queries better' series.
--- SKIP: TestInvalidQueryRequests (0.00s)
=== RUN   TestSearchResponse
--- PASS: TestSearchResponse (0.00s)
=== RUN   TestMakeResponseSearchQuery
=== RUN   TestMakeResponseSearchQuery/ListByField
--- PASS: TestMakeResponseSearchQuery (0.00s)
--- PASS: TestMakeResponseSearchQuery/ListByField (0.00s)
=== RUN   TestMakeResponseAsyncSearchQuery
=== RUN   TestMakeResponseAsyncSearchQuery/0
query_translator_test.go:420:
=== RUN   TestMakeResponseAsyncSearchQuery/1
query_translator_test.go:420:
--- PASS: TestMakeResponseAsyncSearchQuery (0.00s)
--- SKIP: TestMakeResponseAsyncSearchQuery/0 (0.00s)
--- SKIP: TestMakeResponseAsyncSearchQuery/1 (0.00s)
=== RUN   TestMakeResponseSearchQueryIsProperJson
--- PASS: TestMakeResponseSearchQueryIsProperJson (0.00s)
PASS
ok  	github.com/QuesmaOrg/quesma/quesma/queryparser	1.383s
=== RUN   TestTranslatingLuceneQueriesToSQL
=== RUN   TestTranslatingLuceneQueriesToSQL/0
=== RUN   TestTranslatingLuceneQueriesToSQL/1
=== RUN   TestTranslatingLuceneQueriesToSQL/2
=== RUN   TestTranslatingLuceneQueriesToSQL/3
=== RUN   TestTranslatingLuceneQueriesToSQL/4
=== RUN   TestTranslatingLuceneQueriesToSQL/5
=== RUN   TestTranslatingLuceneQueriesToSQL/6
=== RUN   TestTranslatingLuceneQueriesToSQL/7
=== RUN   TestTranslatingLuceneQueriesToSQL/8
=== RUN   TestTranslatingLuceneQueriesToSQL/9
=== RUN   TestTranslatingLuceneQueriesToSQL/10
=== RUN   TestTranslatingLuceneQueriesToSQL/11
=== RUN   TestTranslatingLuceneQueriesToSQL/12
=== RUN   TestTranslatingLuceneQueriesToSQL/13
=== RUN   TestTranslatingLuceneQueriesToSQL/14
=== RUN   TestTranslatingLuceneQueriesToSQL/15
=== RUN   TestTranslatingLuceneQueriesToSQL/16
=== RUN   TestTranslatingLuceneQueriesToSQL/17
=== RUN   TestTranslatingLuceneQueriesToSQL/18
=== RUN   TestTranslatingLuceneQueriesToSQL/19
=== RUN   TestTranslatingLuceneQueriesToSQL/20
=== RUN   TestTranslatingLuceneQueriesToSQL/21
=== RUN   TestTranslatingLuceneQueriesToSQL/22
=== RUN   TestTranslatingLuceneQueriesToSQL/23
=== RUN   TestTranslatingLuceneQueriesToSQL/24
=== RUN   TestTranslatingLuceneQueriesToSQL/25
=== RUN   TestTranslatingLuceneQueriesToSQL/26
=== RUN   TestTranslatingLuceneQueriesToSQL/27
=== RUN   TestTranslatingLuceneQueriesToSQL/28
=== RUN   TestTranslatingLuceneQueriesToSQL/29
=== RUN   TestTranslatingLuceneQueriesToSQL/30
=== RUN   TestTranslatingLuceneQueriesToSQL/31
=== RUN   TestTranslatingLuceneQueriesToSQL/32
=== RUN   TestTranslatingLuceneQueriesToSQL/33
=== RUN   TestTranslatingLuceneQueriesToSQL/34
=== RUN   TestTranslatingLuceneQueriesToSQL/35
=== RUN   TestTranslatingLuceneQueriesToSQL/36
=== RUN   TestTranslatingLuceneQueriesToSQL/37
=== RUN   TestTranslatingLuceneQueriesToSQL/38
=== RUN   TestTranslatingLuceneQueriesToSQL/39
=== RUN   TestTranslatingLuceneQueriesToSQL/40
=== RUN   TestTranslatingLuceneQueriesToSQL/41
=== RUN   TestTranslatingLuceneQueriesToSQL/42
=== RUN   TestTranslatingLuceneQueriesToSQL/43
=== RUN   TestTranslatingLuceneQueriesToSQL/44
=== RUN   TestTranslatingLuceneQueriesToSQL/45
=== RUN   TestTranslatingLuceneQueriesToSQL/46
=== RUN   TestTranslatingLuceneQueriesToSQL/47
=== RUN   TestTranslatingLuceneQueriesToSQL/48
=== RUN   TestTranslatingLuceneQueriesToSQL/49
=== RUN   TestTranslatingLuceneQueriesToSQL/50
=== RUN   TestTranslatingLuceneQueriesToSQL/51
=== RUN   TestTranslatingLuceneQueriesToSQL/52
=== RUN   TestTranslatingLuceneQueriesToSQL/53
=== RUN   TestTranslatingLuceneQueriesToSQL/54
=== RUN   TestTranslatingLuceneQueriesToSQL/55
=== RUN   TestTranslatingLuceneQueriesToSQL/56
Feb 10 13:50:26.000 ERR invalid expression, missing value, tokens: [{}]
=== RUN   TestTranslatingLuceneQueriesToSQL/57
=== RUN   TestTranslatingLuceneQueriesToSQL/58
=== RUN   TestTranslatingLuceneQueriesToSQL/59
=== RUN   TestTranslatingLuceneQueriesToSQL/60
=== RUN   TestTranslatingLuceneQueriesToSQL/61
=== RUN   TestTranslatingLuceneQueriesToSQL/62
=== RUN   TestTranslatingLuceneQueriesToSQL/63
=== RUN   TestTranslatingLuceneQueriesToSQL/64
Feb 10 13:50:26.000 ERR invalid expression, can't have ) with an empty stack, tokens: []
=== RUN   TestTranslatingLuceneQueriesToSQL/65
--- PASS: TestTranslatingLuceneQueriesToSQL (0.01s)
--- PASS: TestTranslatingLuceneQueriesToSQL/0 (0.00s)
--- PASS: TestTranslatingLuceneQueriesToSQL/1 (0.00s)
--- PASS: TestTranslatingLuceneQueriesToSQL/2 (0.00s)
--- PASS: TestTranslatingLuceneQueriesToSQL/3 (0.00s)
--- PASS: TestTranslatingLuceneQueriesToSQL/4 (0.00s)
--- PASS: TestTranslatingLuceneQueriesToSQL/5 (0.00s)
--- PASS: TestTranslatingLuceneQueriesToSQL/6 (0.00s)
--- PASS: TestTranslatingLuceneQueriesToSQL/7 (0.00s)
--- PASS: TestTranslatingLuceneQueriesToSQL/8 (0.00s)
--- PASS: TestTranslatingLuceneQueriesToSQL/9 (0.00s)
--- PASS: TestTranslatingLuceneQueriesToSQL/10 (0.00s)
--- PASS: TestTranslatingLuceneQueriesToSQL/11 (0.00s)
--- PASS: TestTranslatingLuceneQueriesToSQL/12 (0.00s)
--- PASS: TestTranslatingLuceneQueriesToSQL/13 (0.00s)
--- PASS: TestTranslatingLuceneQueriesToSQL/14 (0.00s)
--- PASS: TestTranslatingLuceneQueriesToSQL/15 (0.00s)
--- PASS: TestTranslatingLuceneQueriesToSQL/16 (0.00s)
--- PASS: TestTranslatingLuceneQueriesToSQL/17 (0.00s)
--- PASS: TestTranslatingLuceneQueriesToSQL/18 (0.00s)
--- PASS: TestTranslatingLuceneQueriesToSQL/19 (0.00s)
--- PASS: TestTranslatingLuceneQueriesToSQL/20 (0.00s)
--- PASS: TestTranslatingLuceneQueriesToSQL/21 (0.00s)
--- PASS: TestTranslatingLuceneQueriesToSQL/22 (0.00s)
--- PASS: TestTranslatingLuceneQueriesToSQL/23 (0.00s)
--- PASS: TestTranslatingLuceneQueriesToSQL/24 (0.00s)
--- PASS: TestTranslatingLuceneQueriesToSQL/25 (0.00s)
--- PASS: TestTranslatingLuceneQueriesToSQL/26 (0.00s)
--- PASS: TestTranslatingLuceneQueriesToSQL/27 (0.00s)
--- PASS: TestTranslatingLuceneQueriesToSQL/28 (0.00s)
--- PASS: TestTranslatingLuceneQueriesToSQL/29 (0.00s)
--- PASS: TestTranslatingLuceneQueriesToSQL/30 (0.00s)
--- PASS: TestTranslatingLuceneQueriesToSQL/31 (0.00s)
--- PASS: TestTranslatingLuceneQueriesToSQL/32 (0.00s)
--- PASS: TestTranslatingLuceneQueriesToSQL/33 (0.00s)
--- PASS: TestTranslatingLuceneQueriesToSQL/34 (0.00s)
--- PASS: TestTranslatingLuceneQueriesToSQL/35 (0.00s)
--- PASS: TestTranslatingLuceneQueriesToSQL/36 (0.00s)
--- PASS: TestTranslatingLuceneQueriesToSQL/37 (0.00s)
--- PASS: TestTranslatingLuceneQueriesToSQL/38 (0.00s)
--- PASS: TestTranslatingLuceneQueriesToSQL/39 (0.00s)
--- PASS: TestTranslatingLuceneQueriesToSQL/40 (0.00s)
--- PASS: TestTranslatingLuceneQueriesToSQL/41 (0.00s)
--- PASS: TestTranslatingLuceneQueriesToSQL/42 (0.00s)
--- PASS: TestTranslatingLuceneQueriesToSQL/43 (0.00s)
--- PASS: TestTranslatingLuceneQueriesToSQL/44 (0.00s)
--- PASS: TestTranslatingLuceneQueriesToSQL/45 (0.00s)
--- PASS: TestTranslatingLuceneQueriesToSQL/46 (0.00s)
--- PASS: TestTranslatingLuceneQueriesToSQL/47 (0.00s)
--- PASS: TestTranslatingLuceneQueriesToSQL/48 (0.00s)
--- PASS: TestTranslatingLuceneQueriesToSQL/49 (0.00s)
--- PASS: TestTranslatingLuceneQueriesToSQL/50 (0.00s)
--- PASS: TestTranslatingLuceneQueriesToSQL/51 (0.00s)
--- PASS: TestTranslatingLuceneQueriesToSQL/52 (0.00s)
--- PASS: TestTranslatingLuceneQueriesToSQL/53 (0.00s)
--- PASS: TestTranslatingLuceneQueriesToSQL/54 (0.00s)
--- PASS: TestTranslatingLuceneQueriesToSQL/55 (0.00s)
--- PASS: TestTranslatingLuceneQueriesToSQL/56 (0.00s)
--- PASS: TestTranslatingLuceneQueriesToSQL/57 (0.00s)
--- PASS: TestTranslatingLuceneQueriesToSQL/58 (0.00s)
--- PASS: TestTranslatingLuceneQueriesToSQL/59 (0.00s)
--- PASS: TestTranslatingLuceneQueriesToSQL/60 (0.00s)
--- PASS: TestTranslatingLuceneQueriesToSQL/61 (0.00s)
--- PASS: TestTranslatingLuceneQueriesToSQL/62 (0.00s)
--- PASS: TestTranslatingLuceneQueriesToSQL/63 (0.00s)
--- PASS: TestTranslatingLuceneQueriesToSQL/64 (0.00s)
--- PASS: TestTranslatingLuceneQueriesToSQL/65 (0.00s)
=== RUN   TestResolvePropertyNamesWhenTranslatingToSQL
=== RUN   TestResolvePropertyNamesWhenTranslatingToSQL/0
=== RUN   TestResolvePropertyNamesWhenTranslatingToSQL/1
--- PASS: TestResolvePropertyNamesWhenTranslatingToSQL (0.00s)
--- PASS: TestResolvePropertyNamesWhenTranslatingToSQL/0 (0.00s)
--- PASS: TestResolvePropertyNamesWhenTranslatingToSQL/1 (0.00s)
PASS
ok  	github.com/QuesmaOrg/quesma/quesma/queryparser/lucene	1.024s
?   	github.com/QuesmaOrg/quesma/quesma/quesma/errors	[no test files]
?   	github.com/QuesmaOrg/quesma/quesma/quesma/functionality/doc	[no test files]
=== RUN   TestParseHighLight
--- PASS: TestParseHighLight (0.00s)
=== RUN   TestHighLightResults
=== RUN   TestHighLightResults/highlighted
=== RUN   TestHighLightResults/highlighted_original_case
=== RUN   TestHighLightResults/highlighted_both
=== RUN   TestHighLightResults/not_highlighted
=== RUN   TestHighLightResults/multiple_highlights
=== RUN   TestHighLightResults/multiple_highlights_security_team_#1
=== RUN   TestHighLightResults/multiple_highlights_security_team_#2
=== RUN   TestHighLightResults/merge_highlights
=== RUN   TestHighLightResults/merge_highlights#01
=== RUN   TestHighLightResults/no_highlights
--- PASS: TestHighLightResults (0.00s)
--- PASS: TestHighLightResults/highlighted (0.00s)
--- PASS: TestHighLightResults/highlighted_original_case (0.00s)
--- PASS: TestHighLightResults/highlighted_both (0.00s)
--- PASS: TestHighLightResults/not_highlighted (0.00s)
--- PASS: TestHighLightResults/multiple_highlights (0.00s)
--- PASS: TestHighLightResults/multiple_highlights_security_team_#1 (0.00s)
--- PASS: TestHighLightResults/multiple_highlights_security_team_#2 (0.00s)
--- PASS: TestHighLightResults/merge_highlights (0.00s)
--- PASS: TestHighLightResults/merge_highlights#01 (0.00s)
--- PASS: TestHighLightResults/no_highlights (0.00s)
=== RUN   TestTcpProxy_Ingest
l4_proxy_test.go:28: This test turned out to cause problems on GitHub Actions CI
(goroutines got stuck on IO Wait for 10 minutes and whole job timed out).
This is probably due to some resource problem with GitHub workers.
We've discussed this in https://quesma.slack.com/archives/C06CNHT9944/p1709136102128349
It's also not a critical test, so it's skipped for now.
--- SKIP: TestTcpProxy_Ingest (0.00s)
=== RUN   TestTcpProxy_IngestAndProcess
l4_proxy_test.go:42: This test turned out to cause problems on GitHub Actions CI
(goroutines got stuck on IO Wait for 10 minutes and whole job timed out).
This is probably due to some resource problem with GitHub workers.
We've discussed this in https://quesma.slack.com/archives/C06CNHT9944/p1709136102128349
It's also not a critical test, so it's skipped for now.
--- SKIP: TestTcpProxy_IngestAndProcess (0.00s)
=== RUN   TestParseMappings_KibanaSampleFlights
--- PASS: TestParseMappings_KibanaSampleFlights (0.00s)
=== RUN   TestGenerateMappings_KibanaSampleFlights
--- PASS: TestGenerateMappings_KibanaSampleFlights (0.00s)
=== RUN   TestParseMappings_KibanaSampleEcommerce
--- PASS: TestParseMappings_KibanaSampleEcommerce (0.00s)
=== RUN   TestGenerateMappings_KibanaSampleEcommerce
--- PASS: TestGenerateMappings_KibanaSampleEcommerce (0.00s)
=== RUN   TestMatchAgainstKibanaAlerts
=== RUN   TestMatchAgainstKibanaAlerts/{kibana_alerts
=== RUN   TestMatchAgainstKibanaAlerts/non_kibana_alerts
=== RUN   TestMatchAgainstKibanaAlerts/migration
--- PASS: TestMatchAgainstKibanaAlerts (0.00s)
--- PASS: TestMatchAgainstKibanaAlerts/{kibana_alerts (0.00s)
--- PASS: TestMatchAgainstKibanaAlerts/non_kibana_alerts (0.00s)
--- PASS: TestMatchAgainstKibanaAlerts/migration (0.00s)
=== RUN   TestShouldExposePprof
quesma_test.go:18: FIXME @pivovarit: this test is flaky, it should be fixed
--- SKIP: TestShouldExposePprof (0.00s)
=== RUN   Test_matchedAgainstConfig
router_test.go:490: Skipping test. These will be replaced with table resolver tests.
--- SKIP: Test_matchedAgainstConfig (0.00s)
=== RUN   Test_matchedAgainstPattern
router_test.go:534: Skipping test. These will be replaced with table resolver tests.
--- SKIP: Test_matchedAgainstPattern (0.00s)
=== RUN   TestConfigureRouter
=== RUN   TestConfigureRouter/GET-at-/_cluster/health
=== RUN   TestConfigureRouter/POST-at-/indexName/_refresh
=== RUN   TestConfigureRouter/POST-at-/indexName/_doc
=== RUN   TestConfigureRouter/POST-at-/indexName/_bulk
=== RUN   TestConfigureRouter/PUT-at-/indexName/_bulk
=== RUN   TestConfigureRouter/GET-at-/_resolve/index/indexName
=== RUN   TestConfigureRouter/GET-at-/indexName/_count
=== RUN   TestConfigureRouter/GET-at-/_search
=== RUN   TestConfigureRouter/POST-at-/_search
=== RUN   TestConfigureRouter/PUT-at-/_search
=== RUN   TestConfigureRouter/GET-at-/indexName/_search
=== RUN   TestConfigureRouter/POST-at-/indexName/_search
=== RUN   TestConfigureRouter/POST-at-/indexName/_async_search
=== RUN   TestConfigureRouter/PUT-at-/indexName/_mapping
=== RUN   TestConfigureRouter/GET-at-/indexName/_mapping
=== RUN   TestConfigureRouter/GET-at-/_async_search/status/quesma_async_absurd_test_id
=== RUN   TestConfigureRouter/GET-at-/_async_search/quesma_async_absurd_test_id
=== RUN   TestConfigureRouter/DELETE-at-/_async_search/quesma_async_absurd_test_id
=== RUN   TestConfigureRouter/GET-at-/indexName/_field_caps
=== RUN   TestConfigureRouter/POST-at-/indexName/_field_caps
=== RUN   TestConfigureRouter/POST-at-/indexName/_terms_enum
=== RUN   TestConfigureRouter/GET-at-/indexName/_eql/search
=== RUN   TestConfigureRouter/POST-at-/indexName/_eql/search
=== RUN   TestConfigureRouter/PUT-at-/indexName
=== RUN   TestConfigureRouter/GET-at-/indexName
=== RUN   TestConfigureRouter/GET-at-/indexName/_quesma_table_resolver
=== RUN   TestConfigureRouter/GET-at-/invalid/path
=== RUN   TestConfigureRouter/POST-at-/_cluster/health
=== RUN   TestConfigureRouter/GET-at-/indexName/_refresh
=== RUN   TestConfigureRouter/GET-at-/indexName/_doc
=== RUN   TestConfigureRouter/DELETE-at-/indexName/_bulk
=== RUN   TestConfigureRouter/POST-at-/_resolve/index/indexName
=== RUN   TestConfigureRouter/POST-at-/indexName/_count
=== RUN   TestConfigureRouter/DELETE-at-/indexName/_search
=== RUN   TestConfigureRouter/GET-at-/indexName/_async_search
=== RUN   TestConfigureRouter/POST-at-/indexName/_mapping
=== RUN   TestConfigureRouter/POST-at-/_async_search/status/quesma_async_absurd_test_id
=== RUN   TestConfigureRouter/PUT-at-/_async_search/quesma_async_absurd_test_id
=== RUN   TestConfigureRouter/DELETE-at-/indexName/_field_caps
=== RUN   TestConfigureRouter/GET-at-/indexName/_terms_enum
=== RUN   TestConfigureRouter/DELETE-at-/indexName/_eql/search
=== RUN   TestConfigureRouter/POST-at-/indexName
=== RUN   TestConfigureRouter/POST-at-/indexName/_quesma_table_resolver
=== RUN   TestConfigureRouter/PUT-at-/indexName/_quesma_table_resolver
=== RUN   TestConfigureRouter/DELETE-at-/indexName/_quesma_table_resolver
--- PASS: TestConfigureRouter (0.01s)
--- PASS: TestConfigureRouter/GET-at-/_cluster/health (0.00s)
--- PASS: TestConfigureRouter/POST-at-/indexName/_refresh (0.00s)
--- PASS: TestConfigureRouter/POST-at-/indexName/_doc (0.00s)
--- PASS: TestConfigureRouter/POST-at-/indexName/_bulk (0.00s)
--- PASS: TestConfigureRouter/PUT-at-/indexName/_bulk (0.00s)
--- PASS: TestConfigureRouter/GET-at-/_resolve/index/indexName (0.00s)
--- PASS: TestConfigureRouter/GET-at-/indexName/_count (0.00s)
--- PASS: TestConfigureRouter/GET-at-/_search (0.00s)
--- PASS: TestConfigureRouter/POST-at-/_search (0.00s)
--- PASS: TestConfigureRouter/PUT-at-/_search (0.00s)
--- PASS: TestConfigureRouter/GET-at-/indexName/_search (0.00s)
--- PASS: TestConfigureRouter/POST-at-/indexName/_search (0.00s)
--- PASS: TestConfigureRouter/POST-at-/indexName/_async_search (0.00s)
--- PASS: TestConfigureRouter/PUT-at-/indexName/_mapping (0.00s)
--- PASS: TestConfigureRouter/GET-at-/indexName/_mapping (0.00s)
--- PASS: TestConfigureRouter/GET-at-/_async_search/status/quesma_async_absurd_test_id (0.00s)
--- PASS: TestConfigureRouter/GET-at-/_async_search/quesma_async_absurd_test_id (0.00s)
--- PASS: TestConfigureRouter/DELETE-at-/_async_search/quesma_async_absurd_test_id (0.00s)
--- PASS: TestConfigureRouter/GET-at-/indexName/_field_caps (0.00s)
--- PASS: TestConfigureRouter/POST-at-/indexName/_field_caps (0.00s)
--- PASS: TestConfigureRouter/POST-at-/indexName/_terms_enum (0.00s)
--- PASS: TestConfigureRouter/GET-at-/indexName/_eql/search (0.00s)
--- PASS: TestConfigureRouter/POST-at-/indexName/_eql/search (0.00s)
--- PASS: TestConfigureRouter/PUT-at-/indexName (0.00s)
--- PASS: TestConfigureRouter/GET-at-/indexName (0.00s)
--- PASS: TestConfigureRouter/GET-at-/indexName/_quesma_table_resolver (0.00s)
--- PASS: TestConfigureRouter/GET-at-/invalid/path (0.00s)
--- PASS: TestConfigureRouter/POST-at-/_cluster/health (0.00s)
--- PASS: TestConfigureRouter/GET-at-/indexName/_refresh (0.00s)
--- PASS: TestConfigureRouter/GET-at-/indexName/_doc (0.00s)
--- PASS: TestConfigureRouter/DELETE-at-/indexName/_bulk (0.00s)
--- PASS: TestConfigureRouter/POST-at-/_resolve/index/indexName (0.00s)
--- PASS: TestConfigureRouter/POST-at-/indexName/_count (0.00s)
--- PASS: TestConfigureRouter/DELETE-at-/indexName/_search (0.00s)
--- PASS: TestConfigureRouter/GET-at-/indexName/_async_search (0.00s)
--- PASS: TestConfigureRouter/POST-at-/indexName/_mapping (0.00s)
--- PASS: TestConfigureRouter/POST-at-/_async_search/status/quesma_async_absurd_test_id (0.00s)
--- PASS: TestConfigureRouter/PUT-at-/_async_search/quesma_async_absurd_test_id (0.00s)
--- PASS: TestConfigureRouter/DELETE-at-/indexName/_field_caps (0.00s)
--- PASS: TestConfigureRouter/GET-at-/indexName/_terms_enum (0.00s)
--- PASS: TestConfigureRouter/DELETE-at-/indexName/_eql/search (0.00s)
--- PASS: TestConfigureRouter/POST-at-/indexName (0.00s)
--- PASS: TestConfigureRouter/POST-at-/indexName/_quesma_table_resolver (0.00s)
--- PASS: TestConfigureRouter/PUT-at-/indexName/_quesma_table_resolver (0.00s)
--- PASS: TestConfigureRouter/DELETE-at-/indexName/_quesma_table_resolver (0.00s)
=== RUN   Test_validateAndParse
=== RUN   Test_validateAndParse/<nil>_(testNr:0)
=== RUN   Test_validateAndParse/[]_(testNr:1)
=== RUN   Test_validateAndParse/[1]_(testNr:2)
=== RUN   Test_validateAndParse/[1]_(testNr:3)
=== RUN   Test_validateAndParse/[1.1]_(testNr:4)
=== RUN   Test_validateAndParse/[-1]_(testNr:5)
=== RUN   Test_validateAndParse/[1_abc]_(testNr:6)
=== RUN   Test_validateAndParse/string_is_bad_(testNr:7)
--- PASS: Test_validateAndParse (0.00s)
--- PASS: Test_validateAndParse/<nil>_(testNr:0) (0.00s)
--- PASS: Test_validateAndParse/[]_(testNr:1) (0.00s)
--- PASS: Test_validateAndParse/[1]_(testNr:2) (0.00s)
--- PASS: Test_validateAndParse/[1]_(testNr:3) (0.00s)
--- PASS: Test_validateAndParse/[1.1]_(testNr:4) (0.00s)
--- PASS: Test_validateAndParse/[-1]_(testNr:5) (0.00s)
--- PASS: Test_validateAndParse/[1_abc]_(testNr:6) (0.00s)
--- PASS: Test_validateAndParse/string_is_bad_(testNr:7) (0.00s)
=== RUN   Test_applySearchAfterParameter
=== RUN   Test_applySearchAfterParameter/<nil>_(testNr:0)
=== RUN   Test_applySearchAfterParameter/[]_(testNr:1)
=== RUN   Test_applySearchAfterParameter/[1]_(testNr:2)
=== RUN   Test_applySearchAfterParameter/[1]_(testNr:3)
=== RUN   Test_applySearchAfterParameter/[1.1]_(testNr:4)
=== RUN   Test_applySearchAfterParameter/[5_10]_(testNr:5)
=== RUN   Test_applySearchAfterParameter/[-1]_(testNr:6)
=== RUN   Test_applySearchAfterParameter/string_is_bad_(testNr:7)
=== RUN   Test_applySearchAfterParameter/[1]_(testNr:8)
--- PASS: Test_applySearchAfterParameter (0.00s)
--- PASS: Test_applySearchAfterParameter/<nil>_(testNr:0) (0.00s)
--- PASS: Test_applySearchAfterParameter/[]_(testNr:1) (0.00s)
--- PASS: Test_applySearchAfterParameter/[1]_(testNr:2) (0.00s)
--- PASS: Test_applySearchAfterParameter/[1]_(testNr:3) (0.00s)
--- PASS: Test_applySearchAfterParameter/[1.1]_(testNr:4) (0.00s)
--- PASS: Test_applySearchAfterParameter/[5_10]_(testNr:5) (0.00s)
--- PASS: Test_applySearchAfterParameter/[-1]_(testNr:6) (0.00s)
--- PASS: Test_applySearchAfterParameter/string_is_bad_(testNr:7) (0.00s)
--- PASS: Test_applySearchAfterParameter/[1]_(testNr:8) (0.00s)
=== RUN   Test_ipRangeTransform
=== RUN   Test_ipRangeTransform/0
Feb 10 13:50:29.000 DBG loading schema for table kibana_sample_data_logs_nested
Feb 10 13:50:29.000 DBG type geo_point not supported, falling back to keyword
Feb 10 13:50:29.000 DBG loading schema for table kibana_sample_data_flights
Feb 10 13:50:29.000 DBG type geo_point not supported, falling back to keyword
Feb 10 13:50:29.000 DBG Got field already resolved message
=== RUN   Test_ipRangeTransform/1
Feb 10 13:50:29.000 DBG Got field already resolved message
=== RUN   Test_ipRangeTransform/2
Feb 10 13:50:29.000 DBG Got field already resolved message
Feb 10 13:50:29.000 DBG Got field already resolved clientip
=== RUN   Test_ipRangeTransform/3
Feb 10 13:50:29.000 DBG Got field already resolved message
Feb 10 13:50:29.000 WRN ip transformation omitted, operator is not = or iLIKE: <, lhs: clientip, rhs: '111.42.223.209/16'
=== RUN   Test_ipRangeTransform/4
Feb 10 13:50:29.000 DBG Got field already resolved message
=== RUN   Test_ipRangeTransform/5
Feb 10 13:50:29.000 DBG Got field already resolved message
=== RUN   Test_ipRangeTransform/6
--- PASS: Test_ipRangeTransform (0.00s)
--- PASS: Test_ipRangeTransform/0 (0.00s)
--- PASS: Test_ipRangeTransform/1 (0.00s)
--- PASS: Test_ipRangeTransform/2 (0.00s)
--- PASS: Test_ipRangeTransform/3 (0.00s)
--- PASS: Test_ipRangeTransform/4 (0.00s)
--- PASS: Test_ipRangeTransform/5 (0.00s)
--- PASS: Test_ipRangeTransform/6 (0.00s)
=== RUN   Test_arrayType
=== RUN   Test_arrayType/simple_array
Feb 10 13:50:29.000 DBG Got field already resolved @timestamp
Feb 10 13:50:29.000 DBG Got field already resolved order_date
Feb 10 13:50:29.000 ERR Unhandled array column ref products_name (Array(String))
Feb 10 13:50:29.000 ERR Unhandled array column ref products_quantity (Array(Int64))
Feb 10 13:50:29.000 ERR Unhandled array column ref products_sku (Array(String))
=== RUN   Test_arrayType/arrayReduce
Feb 10 13:50:29.000 DBG Got field already resolved order_date
Feb 10 13:50:29.000 DBG Got field already resolved order_date
=== RUN   Test_arrayType/arrayReducePancake
Feb 10 13:50:29.000 DBG Got field already resolved order_date
Feb 10 13:50:29.000 DBG Got field already resolved order_date
=== RUN   Test_arrayType/ilike_array
Feb 10 13:50:29.000 DBG Got field already resolved order_date
Feb 10 13:50:29.000 DBG Got field already resolved order_date
=== RUN   Test_arrayType/equals_array
Feb 10 13:50:29.000 DBG Got field already resolved order_date
Feb 10 13:50:29.000 DBG Got field already resolved order_date
--- PASS: Test_arrayType (0.00s)
--- PASS: Test_arrayType/simple_array (0.00s)
--- PASS: Test_arrayType/arrayReduce (0.00s)
--- PASS: Test_arrayType/arrayReducePancake (0.00s)
--- PASS: Test_arrayType/ilike_array (0.00s)
--- PASS: Test_arrayType/equals_array (0.00s)
=== RUN   TestApplyWildCard
=== RUN   TestApplyWildCard/test1
=== RUN   TestApplyWildCard/test2
=== RUN   TestApplyWildCard/test3
=== RUN   TestApplyWildCard/test4
--- PASS: TestApplyWildCard (0.00s)
--- PASS: TestApplyWildCard/test1 (0.00s)
--- PASS: TestApplyWildCard/test2 (0.00s)
--- PASS: TestApplyWildCard/test3 (0.00s)
--- PASS: TestApplyWildCard/test4 (0.00s)
=== RUN   TestApplyPhysicalFromExpression
Feb 10 13:50:29.000 DBG loading schema for table test
=== RUN   TestApplyPhysicalFromExpression/single_table
=== RUN   TestApplyPhysicalFromExpression/single_table_with_common_table
=== RUN   TestApplyPhysicalFromExpression/two_tables__with_common_table
=== RUN   TestApplyPhysicalFromExpression/two_daily_tables_tables__with_common_table_(group_common_table_indexes_optimizer)
=== RUN   TestApplyPhysicalFromExpression/cte_with_fixed_table_name
=== RUN   TestApplyPhysicalFromExpression/cte_with__table_name
--- PASS: TestApplyPhysicalFromExpression (0.00s)
--- PASS: TestApplyPhysicalFromExpression/single_table (0.00s)
--- PASS: TestApplyPhysicalFromExpression/single_table_with_common_table (0.00s)
--- PASS: TestApplyPhysicalFromExpression/two_tables__with_common_table (0.00s)
--- PASS: TestApplyPhysicalFromExpression/two_daily_tables_tables__with_common_table_(group_common_table_indexes_optimizer) (0.00s)
--- PASS: TestApplyPhysicalFromExpression/cte_with_fixed_table_name (0.00s)
--- PASS: TestApplyPhysicalFromExpression/cte_with__table_name (0.00s)
=== RUN   TestFullTextFields
=== RUN   TestFullTextFields/no_full_text_field_column
Feb 10 13:50:29.000 DBG loading schema for table test
=== RUN   TestFullTextFields/single_column
Feb 10 13:50:29.000 DBG loading schema for table test
=== RUN   TestFullTextFields/two_columns
Feb 10 13:50:29.000 DBG loading schema for table test
--- PASS: TestFullTextFields (0.00s)
--- PASS: TestFullTextFields/no_full_text_field_column (0.00s)
--- PASS: TestFullTextFields/single_column (0.00s)
--- PASS: TestFullTextFields/two_columns (0.00s)
=== RUN   Test_applyMatchOperator
=== RUN   Test_applyMatchOperator/match_operator_transformation_for_String_(ILIKE)
Feb 10 13:50:29.000 DBG loading schema for table test
=== RUN   Test_applyMatchOperator/match_operator_transformation_for_Int64_(=)
Feb 10 13:50:29.000 DBG loading schema for table test
--- PASS: Test_applyMatchOperator (0.00s)
--- PASS: Test_applyMatchOperator/match_operator_transformation_for_String_(ILIKE) (0.00s)
--- PASS: Test_applyMatchOperator/match_operator_transformation_for_Int64_(=) (0.00s)
=== RUN   Test_checkAggOverUnsupportedType
=== RUN   Test_checkAggOverUnsupportedType/String
Feb 10 13:50:29.000 DBG loading schema for table test
Feb 10 13:50:29.000 WRN Aggregation 'sum' over unsupported type 'String' in column 'message'
=== RUN   Test_checkAggOverUnsupportedType/do_not_int_field
Feb 10 13:50:29.000 DBG loading schema for table test
=== RUN   Test_checkAggOverUnsupportedType/DateTime
Feb 10 13:50:29.000 DBG loading schema for table test
Feb 10 13:50:29.000 WRN Aggregation 'sum' over unsupported type 'DateTime' in column '@timestamp'
--- PASS: Test_checkAggOverUnsupportedType (0.00s)
--- PASS: Test_checkAggOverUnsupportedType/String (0.00s)
--- PASS: Test_checkAggOverUnsupportedType/do_not_int_field (0.00s)
--- PASS: Test_checkAggOverUnsupportedType/DateTime (0.00s)
=== RUN   TestSearchCommonTable
=== RUN   TestSearchCommonTable/query_non_virtual_table(0)
Feb 10 13:50:29.000 DBG Got field already resolved @timestamp
Feb 10 13:50:29.000 DBG Got field already resolved message
=== RUN   TestSearchCommonTable/query_virtual_table(1)
Feb 10 13:50:29.000 DBG Got field already resolved @timestamp
Feb 10 13:50:29.000 DBG Got field already resolved message
=== RUN   TestSearchCommonTable/query_virtual_tables(2)
Feb 10 13:50:29.000 DBG Got field already resolved @timestamp
Feb 10 13:50:29.000 DBG Got field already resolved message
=== RUN   TestSearchCommonTable/query_virtual_tables_(the_one_is_not_existing)(3)
Feb 10 13:50:29.000 DBG Got field already resolved @timestamp
Feb 10 13:50:29.000 DBG Got field already resolved message
=== RUN   TestSearchCommonTable/query_virtual_tables_-_daily_indexes_with_optimization_enabled(4)
Feb 10 13:50:29.000 DBG Got field already resolved @timestamp
Feb 10 13:50:29.000 DBG Got field already resolved message
=== RUN   TestSearchCommonTable/query_all_logs_-_we_query_only_virtual_tables(5)
Feb 10 13:50:29.000 DBG Got field already resolved @timestamp
Feb 10 13:50:29.000 DBG Got field already resolved message
=== RUN   TestSearchCommonTable/query_all_-_we_query_only_virtual_tables(6)
Feb 10 13:50:29.000 DBG Got field already resolved @timestamp
Feb 10 13:50:29.000 DBG Got field already resolved message
=== RUN   TestSearchCommonTable/aggregation_query(7)
Feb 10 13:50:29.000 DBG field 'timestamp' referenced, but not found in schema, falling back to original name request_id=0194f020-3141-7c08-8b1a-611face75e4e
Feb 10 13:50:29.000 DBG Got field already resolved @timestamp
Feb 10 13:50:29.000 DBG Got field already resolved @timestamp
Feb 10 13:50:29.000 DBG Got field already resolved @timestamp
Feb 10 13:50:29.000 DBG Got field already resolved @timestamp
Feb 10 13:50:29.000 DBG Got field already resolved @timestamp
Feb 10 13:50:29.000 DBG Got field already resolved message
Feb 10 13:50:29.000 WRN failed to convert timestamp field [2024-04-14] to time.Time request_id=0194f020-3141-7c08-8b1a-611face75e4e
--- PASS: TestSearchCommonTable (0.02s)
--- PASS: TestSearchCommonTable/query_non_virtual_table(0) (0.00s)
--- PASS: TestSearchCommonTable/query_virtual_table(1) (0.00s)
--- PASS: TestSearchCommonTable/query_virtual_tables(2) (0.00s)
--- PASS: TestSearchCommonTable/query_virtual_tables_(the_one_is_not_existing)(3) (0.00s)
--- PASS: TestSearchCommonTable/query_virtual_tables_-_daily_indexes_with_optimization_enabled(4) (0.00s)
--- PASS: TestSearchCommonTable/query_all_logs_-_we_query_only_virtual_tables(5) (0.00s)
--- PASS: TestSearchCommonTable/query_all_-_we_query_only_virtual_tables(6) (0.00s)
--- PASS: TestSearchCommonTable/aggregation_query(7) (0.01s)
=== RUN   TestSearchOpensearch
=== RUN   TestSearchOpensearch/0Basic_Explorer_request
=== RUN   TestSearchOpensearch/1Basic_Explorer_request,_but_without_SELECT_*
--- PASS: TestSearchOpensearch (0.01s)
--- PASS: TestSearchOpensearch/0Basic_Explorer_request (0.01s)
--- PASS: TestSearchOpensearch/1Basic_Explorer_request,_but_without_SELECT_* (0.00s)
=== RUN   TestHighlighter
--- PASS: TestHighlighter (0.01s)
=== RUN   TestAsyncSearchHandler
=== RUN   TestAsyncSearchHandler/Facets:_aggregate_by_field_+_additionally_match_user_(filter)(0)
=== RUN   TestAsyncSearchHandler/ListByField:_query_one_field,_last_'size'_results,_return_list_of_just_that_field,_no_timestamp,_etc.(1)
=== RUN   TestAsyncSearchHandler/ListAllFields:_search_all_fields,_return_JSON_+_count_(we_don't_return_count_atm)(2)
=== RUN   TestAsyncSearchHandler/Histogram:_possible_query_nr_1(3)
=== RUN   TestAsyncSearchHandler/Histogram:_possible_query_nr_2(4)
=== RUN   TestAsyncSearchHandler/Earliest/latest_timestamp(5)
=== RUN   TestAsyncSearchHandler/VERY_simple_ListAllFields(6)
=== RUN   TestAsyncSearchHandler/Timestamp_in_epoch_millis_+_select_one_field(7)
--- PASS: TestAsyncSearchHandler (0.07s)
--- PASS: TestAsyncSearchHandler/Facets:_aggregate_by_field_+_additionally_match_user_(filter)(0) (0.01s)
--- PASS: TestAsyncSearchHandler/ListByField:_query_one_field,_last_'size'_results,_return_list_of_just_that_field,_no_timestamp,_etc.(1) (0.01s)
--- PASS: TestAsyncSearchHandler/ListAllFields:_search_all_fields,_return_JSON_+_count_(we_don't_return_count_atm)(2) (0.01s)
--- PASS: TestAsyncSearchHandler/Histogram:_possible_query_nr_1(3) (0.01s)
--- PASS: TestAsyncSearchHandler/Histogram:_possible_query_nr_2(4) (0.01s)
--- PASS: TestAsyncSearchHandler/Earliest/latest_timestamp(5) (0.01s)
--- PASS: TestAsyncSearchHandler/VERY_simple_ListAllFields(6) (0.00s)
--- PASS: TestAsyncSearchHandler/Timestamp_in_epoch_millis_+_select_one_field(7) (0.01s)
=== RUN   TestAsyncSearchHandlerSpecialCharacters
=== RUN   TestAsyncSearchHandlerSpecialCharacters/0
--- PASS: TestAsyncSearchHandlerSpecialCharacters (0.01s)
--- PASS: TestAsyncSearchHandlerSpecialCharacters/0 (0.01s)
=== RUN   TestSearchHandler
=== RUN   TestSearchHandler/Match_all(0)
=== RUN   TestSearchHandler/Term_as_dictionary(1)
=== RUN   TestSearchHandler/Term_as_array(2)
=== RUN   TestSearchHandler/Sample_log_query(3)
=== RUN   TestSearchHandler/Multiple_bool_query(4)
=== RUN   TestSearchHandler/Match_phrase(5)
=== RUN   TestSearchHandler/Match(6)
=== RUN   TestSearchHandler/Terms(7)
=== RUN   TestSearchHandler/Exists(8)
=== RUN   TestSearchHandler/Simple_query_string(9)
=== RUN   TestSearchHandler/Simple_query_string_wildcard(10)
=== RUN   TestSearchHandler/Simple_wildcard(11)
=== RUN   TestSearchHandler/Simple_prefix_ver1(12)
=== RUN   TestSearchHandler/Simple_prefix_ver2(13)
=== RUN   TestSearchHandler/Query_string,_wildcards_don't_work_properly(14)
=== RUN   TestSearchHandler/Empty_bool(15)
=== RUN   TestSearchHandler/Simplest_'match_phrase'(16)
=== RUN   TestSearchHandler/More_nested_'match_phrase'(17)
=== RUN   TestSearchHandler/Simple_nested(18)
=== RUN   TestSearchHandler/random_simple_test(19)
=== RUN   TestSearchHandler/termWithCompoundValue(20)
=== RUN   TestSearchHandler/count(*)_as_/_search_query._With_filter(21)
=== RUN   TestSearchHandler/count(*)_as_/_search_or_/logs-*-/_search_query._Without_filter(22)
=== RUN   TestSearchHandler/count(*)_as_/_search_query._With_filter(23)
=== RUN   TestSearchHandler/count(*)_as_/_search_or_/logs-*-/_search_query._Without_filter(24)
=== RUN   TestSearchHandler/_search,_only_one_so_far_with_fields,_we're_not_sure_if_SELECT_*_is_correct,_or_should_be_SELECT_@timestamp(25)
=== RUN   TestSearchHandler/Empty_must(26)
=== RUN   TestSearchHandler/Empty_must_not(27)
=== RUN   TestSearchHandler/Empty_should(28)
=== RUN   TestSearchHandler/Empty_all_bools(29)
=== RUN   TestSearchHandler/Some_bools_empty,_some_not(30)
=== RUN   TestSearchHandler/Match_all_(empty_query)(31)
=== RUN   TestSearchHandler/Constant_score_query(32)
=== RUN   TestSearchHandler/Match_phrase_using__id_field(33)
=== RUN   TestSearchHandler/Comments_in_filter(34)
=== RUN   TestSearchHandler/Terms_with_range(35)
=== RUN   TestSearchHandler/Simple_regexp_(can_be_simply_transformed_to_one_LIKE)(36)
=== RUN   TestSearchHandler/Simple_regexp_(can_be_simply_transformed_to_one_LIKE),_with__,_which_needs_to_be_escaped(37)
=== RUN   TestSearchHandler/Complex_regexp_1_(can't_be_transformed_to_LIKE)(38)
=== RUN   TestSearchHandler/Complex_regexp_2_(can't_be_transformed_to_LIKE)(39)
=== RUN   TestSearchHandler/Escaping_of_',_\,_\t_and_\n(40)
=== RUN   TestSearchHandler/ids,_0_values(41)
=== RUN   TestSearchHandler/ids,_1_value(42)
=== RUN   TestSearchHandler/ids,_2+_values(43)
=== RUN   TestSearchHandler/ids_with_DateTime64(9)_(trailing_zeroes)(44)
=== RUN   TestSearchHandler/ids_with_DateTime64(9)_(no_trailing_zeroes)(45)
=== RUN   TestSearchHandler/ids_with_DateTime64(0)(46)
=== RUN   TestSearchHandler/ids_with_DateTime64(1)(47)
--- PASS: TestSearchHandler (0.20s)
--- PASS: TestSearchHandler/Match_all(0) (0.00s)
--- PASS: TestSearchHandler/Term_as_dictionary(1) (0.00s)
--- PASS: TestSearchHandler/Term_as_array(2) (0.01s)
--- PASS: TestSearchHandler/Sample_log_query(3) (0.01s)
--- PASS: TestSearchHandler/Multiple_bool_query(4) (0.01s)
--- PASS: TestSearchHandler/Match_phrase(5) (0.01s)
--- PASS: TestSearchHandler/Match(6) (0.00s)
--- PASS: TestSearchHandler/Terms(7) (0.00s)
--- PASS: TestSearchHandler/Exists(8) (0.01s)
--- PASS: TestSearchHandler/Simple_query_string(9) (0.00s)
--- PASS: TestSearchHandler/Simple_query_string_wildcard(10) (0.00s)
--- PASS: TestSearchHandler/Simple_wildcard(11) (0.00s)
--- PASS: TestSearchHandler/Simple_prefix_ver1(12) (0.00s)
--- PASS: TestSearchHandler/Simple_prefix_ver2(13) (0.00s)
--- PASS: TestSearchHandler/Query_string,_wildcards_don't_work_properly(14) (0.00s)
--- PASS: TestSearchHandler/Empty_bool(15) (0.00s)
--- PASS: TestSearchHandler/Simplest_'match_phrase'(16) (0.00s)
--- PASS: TestSearchHandler/More_nested_'match_phrase'(17) (0.00s)
--- PASS: TestSearchHandler/Simple_nested(18) (0.01s)
--- PASS: TestSearchHandler/random_simple_test(19) (0.00s)
--- PASS: TestSearchHandler/termWithCompoundValue(20) (0.00s)
--- PASS: TestSearchHandler/count(*)_as_/_search_query._With_filter(21) (0.00s)
--- PASS: TestSearchHandler/count(*)_as_/_search_or_/logs-*-/_search_query._Without_filter(22) (0.00s)
--- PASS: TestSearchHandler/count(*)_as_/_search_query._With_filter(23) (0.00s)
--- PASS: TestSearchHandler/count(*)_as_/_search_or_/logs-*-/_search_query._Without_filter(24) (0.00s)
--- PASS: TestSearchHandler/_search,_only_one_so_far_with_fields,_we're_not_sure_if_SELECT_*_is_correct,_or_should_be_SELECT_@timestamp(25) (0.00s)
--- PASS: TestSearchHandler/Empty_must(26) (0.00s)
--- PASS: TestSearchHandler/Empty_must_not(27) (0.00s)
--- PASS: TestSearchHandler/Empty_should(28) (0.00s)
--- PASS: TestSearchHandler/Empty_all_bools(29) (0.00s)
--- PASS: TestSearchHandler/Some_bools_empty,_some_not(30) (0.00s)
--- PASS: TestSearchHandler/Match_all_(empty_query)(31) (0.00s)
--- PASS: TestSearchHandler/Constant_score_query(32) (0.00s)
--- PASS: TestSearchHandler/Match_phrase_using__id_field(33) (0.00s)
--- PASS: TestSearchHandler/Comments_in_filter(34) (0.01s)
--- PASS: TestSearchHandler/Terms_with_range(35) (0.01s)
--- PASS: TestSearchHandler/Simple_regexp_(can_be_simply_transformed_to_one_LIKE)(36) (0.00s)
--- PASS: TestSearchHandler/Simple_regexp_(can_be_simply_transformed_to_one_LIKE),_with__,_which_needs_to_be_escaped(37) (0.00s)
--- PASS: TestSearchHandler/Complex_regexp_1_(can't_be_transformed_to_LIKE)(38) (0.00s)
--- PASS: TestSearchHandler/Complex_regexp_2_(can't_be_transformed_to_LIKE)(39) (0.00s)
--- PASS: TestSearchHandler/Escaping_of_',_\,_\t_and_\n(40) (0.00s)
--- PASS: TestSearchHandler/ids,_0_values(41) (0.00s)
--- PASS: TestSearchHandler/ids,_1_value(42) (0.00s)
--- PASS: TestSearchHandler/ids,_2+_values(43) (0.00s)
--- PASS: TestSearchHandler/ids_with_DateTime64(9)_(trailing_zeroes)(44) (0.00s)
--- PASS: TestSearchHandler/ids_with_DateTime64(9)_(no_trailing_zeroes)(45) (0.00s)
--- PASS: TestSearchHandler/ids_with_DateTime64(0)(46) (0.00s)
--- PASS: TestSearchHandler/ids_with_DateTime64(1)(47) (0.00s)
=== RUN   TestSearchHandlerRuntimeMappings
=== RUN   TestSearchHandlerRuntimeMappings/Match_all_-_runtime_mappings(0)
--- PASS: TestSearchHandlerRuntimeMappings (0.00s)
--- PASS: TestSearchHandlerRuntimeMappings/Match_all_-_runtime_mappings(0) (0.00s)
=== RUN   TestSearchHandlerNoAttrsConfig
=== RUN   TestSearchHandlerNoAttrsConfig/Test_empty_ANDs,_ORs_and_NOTs..._idk,_this_test_is_very_old_and_weird,_better_write_to_Krzysiek_if_it_fails_for_you
--- PASS: TestSearchHandlerNoAttrsConfig (0.00s)
--- PASS: TestSearchHandlerNoAttrsConfig/Test_empty_ANDs,_ORs_and_NOTs..._idk,_this_test_is_very_old_and_weird,_better_write_to_Krzysiek_if_it_fails_for_you (0.00s)
=== RUN   TestAsyncSearchFilter
=== RUN   TestAsyncSearchFilter/Empty_filter_clause
=== RUN   TestAsyncSearchFilter/Filter_with_now_in_range
=== RUN   TestAsyncSearchFilter/Range_with_int_timestamps
=== RUN   TestAsyncSearchFilter/Empty_filter
=== RUN   TestAsyncSearchFilter/Empty_filter_with_other_clauses
--- PASS: TestAsyncSearchFilter (0.05s)
--- PASS: TestAsyncSearchFilter/Empty_filter_clause (0.00s)
--- PASS: TestAsyncSearchFilter/Filter_with_now_in_range (0.01s)
--- PASS: TestAsyncSearchFilter/Range_with_int_timestamps (0.02s)
--- PASS: TestAsyncSearchFilter/Empty_filter (0.01s)
--- PASS: TestAsyncSearchFilter/Empty_filter_with_other_clauses (0.01s)
=== RUN   TestHandlingDateTimeFields
--- PASS: TestHandlingDateTimeFields (0.02s)
=== RUN   TestNumericFacetsQueries
=== RUN   TestNumericFacetsQueries/0facets,_int64_as_key,_3_(<10)_values
=== RUN   TestNumericFacetsQueries/0facets,_int64_as_key,_3_(<10)_values#01
--- PASS: TestNumericFacetsQueries (0.01s)
--- PASS: TestNumericFacetsQueries/0facets,_int64_as_key,_3_(<10)_values (0.00s)
--- PASS: TestNumericFacetsQueries/0facets,_int64_as_key,_3_(<10)_values#01 (0.01s)
=== RUN   TestSearchTrackTotalCount
=== RUN   TestSearchTrackTotalCount/0_We_can't_deduct_hits_count_from_the_rows_list,_we_should_send_count(*)_LIMIT_1_request
=== RUN   TestSearchTrackTotalCount/0_We_can't_deduct_hits_count_from_the_rows_list,_we_should_send_count(*)_LIMIT_1_request#01
=== RUN   TestSearchTrackTotalCount/1_We_can_deduct_hits_count_from_the_rows_list,_we_shouldn't_any_count(*)_request
=== RUN   TestSearchTrackTotalCount/1_We_can_deduct_hits_count_from_the_rows_list,_we_shouldn't_any_count(*)_request#01
=== RUN   TestSearchTrackTotalCount/2_We_can_deduct_hits_count_from_the_rows_list,_we_shouldn't_any_count(*)_request,_we_should_return_gte_1
=== RUN   TestSearchTrackTotalCount/2_We_can_deduct_hits_count_from_the_rows_list,_we_shouldn't_any_count(*)_request,_we_should_return_gte_1#01
=== RUN   TestSearchTrackTotalCount/3_We_can_deduct_hits_count_from_the_rows_list,_we_shouldn't_any_count(*)_request,_we_should_return_eq_1
=== RUN   TestSearchTrackTotalCount/3_We_can_deduct_hits_count_from_the_rows_list,_we_shouldn't_any_count(*)_request,_we_should_return_eq_1#01
=== RUN   TestSearchTrackTotalCount/4_track_total_hits:_false
=== RUN   TestSearchTrackTotalCount/4_track_total_hits:_false#01
=== RUN   TestSearchTrackTotalCount/5_track_total_hits:_true,_size_>=_count(*)
=== RUN   TestSearchTrackTotalCount/5_track_total_hits:_true,_size_>=_count(*)#01
=== RUN   TestSearchTrackTotalCount/6_track_total_hits:_true,_size_<_count(*)
=== RUN   TestSearchTrackTotalCount/6_track_total_hits:_true,_size_<_count(*)#01
=== RUN   TestSearchTrackTotalCount/7_Turing_regression_test
=== RUN   TestSearchTrackTotalCount/7_Turing_regression_test#01
=== RUN   TestSearchTrackTotalCount/8_Turing_regression_test
=== RUN   TestSearchTrackTotalCount/8_Turing_regression_test#01
--- PASS: TestSearchTrackTotalCount (0.07s)
--- PASS: TestSearchTrackTotalCount/0_We_can't_deduct_hits_count_from_the_rows_list,_we_should_send_count(*)_LIMIT_1_request (0.00s)
--- PASS: TestSearchTrackTotalCount/0_We_can't_deduct_hits_count_from_the_rows_list,_we_should_send_count(*)_LIMIT_1_request#01 (0.00s)
--- PASS: TestSearchTrackTotalCount/1_We_can_deduct_hits_count_from_the_rows_list,_we_shouldn't_any_count(*)_request (0.00s)
--- PASS: TestSearchTrackTotalCount/1_We_can_deduct_hits_count_from_the_rows_list,_we_shouldn't_any_count(*)_request#01 (0.01s)
--- PASS: TestSearchTrackTotalCount/2_We_can_deduct_hits_count_from_the_rows_list,_we_shouldn't_any_count(*)_request,_we_should_return_gte_1 (0.00s)
--- PASS: TestSearchTrackTotalCount/2_We_can_deduct_hits_count_from_the_rows_list,_we_shouldn't_any_count(*)_request,_we_should_return_gte_1#01 (0.00s)
--- PASS: TestSearchTrackTotalCount/3_We_can_deduct_hits_count_from_the_rows_list,_we_shouldn't_any_count(*)_request,_we_should_return_eq_1 (0.00s)
--- PASS: TestSearchTrackTotalCount/3_We_can_deduct_hits_count_from_the_rows_list,_we_shouldn't_any_count(*)_request,_we_should_return_eq_1#01 (0.01s)
--- PASS: TestSearchTrackTotalCount/4_track_total_hits:_false (0.00s)
--- PASS: TestSearchTrackTotalCount/4_track_total_hits:_false#01 (0.01s)
--- PASS: TestSearchTrackTotalCount/5_track_total_hits:_true,_size_>=_count(*) (0.00s)
--- PASS: TestSearchTrackTotalCount/5_track_total_hits:_true,_size_>=_count(*)#01 (0.01s)
--- PASS: TestSearchTrackTotalCount/6_track_total_hits:_true,_size_<_count(*) (0.00s)
--- PASS: TestSearchTrackTotalCount/6_track_total_hits:_true,_size_<_count(*)#01 (0.01s)
--- PASS: TestSearchTrackTotalCount/7_Turing_regression_test (0.00s)
--- PASS: TestSearchTrackTotalCount/7_Turing_regression_test#01 (0.01s)
--- PASS: TestSearchTrackTotalCount/8_Turing_regression_test (0.00s)
--- PASS: TestSearchTrackTotalCount/8_Turing_regression_test#01 (0.01s)
=== RUN   TestFullQueryTestWIP
==================
WARNING: DATA RACE
Write at 0x0000032eafe0 by goroutine 756:
github.com/QuesmaOrg/quesma/quesma/logger.InitSimpleLoggerForTests()
/home/runner/work/quesma/quesma/quesma/logger/logger.go:122 +0x53e
github.com/QuesmaOrg/quesma/quesma/quesma.TestFullQueryTestWIP()
/home/runner/work/quesma/quesma/quesma/quesma/search_test.go:865 +0x36
testing.tRunner()
/opt/hostedtoolcache/go/1.23.6/x64/src/testing/testing.go:1690 +0x226
testing.(*T).Run.gowrap1()
/opt/hostedtoolcache/go/1.23.6/x64/src/testing/testing.go:1743 +0x44

Previous read at 0x0000032eafe0 by goroutine 752:
github.com/QuesmaOrg/quesma/quesma/logger.Debug()
/home/runner/work/quesma/quesma/quesma/logger/logger.go:210 +0x22f
github.com/QuesmaOrg/quesma/quesma/quesma/ui.(*QuesmaManagementConsole).processChannelMessage()
/home/runner/work/quesma/quesma/quesma/quesma/ui/management_console.go:177 +0x24c
github.com/QuesmaOrg/quesma/quesma/quesma/ui.(*QuesmaManagementConsole).RunOnlyChannelProcessor()
/home/runner/work/quesma/quesma/quesma/quesma/ui/management_console.go:313 +0x33
github.com/QuesmaOrg/quesma/quesma/quesma.NewQueryRunnerDefaultForTests.gowrap1()
/home/runner/work/quesma/quesma/quesma/quesma/search.go:140 +0x17

Goroutine 756 (running) created at:
testing.(*T).Run()
/opt/hostedtoolcache/go/1.23.6/x64/src/testing/testing.go:1743 +0x825
testing.runTests.func1()
/opt/hostedtoolcache/go/1.23.6/x64/src/testing/testing.go:2168 +0x85
testing.tRunner()
/opt/hostedtoolcache/go/1.23.6/x64/src/testing/testing.go:1690 +0x226
testing.runTests()
/opt/hostedtoolcache/go/1.23.6/x64/src/testing/testing.go:2166 +0x8be
testing.(*M).Run()
/opt/hostedtoolcache/go/1.23.6/x64/src/testing/testing.go:2034 +0xf17
main.main()
_testmain.go:133 +0x164

Goroutine 752 (running) created at:
github.com/QuesmaOrg/quesma/quesma/quesma.NewQueryRunnerDefaultForTests()
/home/runner/work/quesma/quesma/quesma/quesma/search.go:140 +0x83c
github.com/QuesmaOrg/quesma/quesma/quesma.TestSearchTrackTotalCount.func1()
/home/runner/work/quesma/quesma/quesma/quesma/search_test.go:804 +0xa64
github.com/QuesmaOrg/quesma/quesma/quesma.TestSearchTrackTotalCount.func2()
/home/runner/work/quesma/quesma/quesma/quesma/search_test.go:858 +0xc7
testing.tRunner()
/opt/hostedtoolcache/go/1.23.6/x64/src/testing/testing.go:1690 +0x226
testing.(*T).Run.gowrap1()
/opt/hostedtoolcache/go/1.23.6/x64/src/testing/testing.go:1743 +0x44
==================
=== RUN   TestFullQueryTestWIP/0_We_can't_deduct_hits_count_from_the_rows_list,_we_should_send_count(*)_LIMIT_1_request
=== NAME  TestFullQueryTestWIP
testing.go:1399: race detected during execution of test
=== NAME  TestFullQueryTestWIP/0_We_can't_deduct_hits_count_from_the_rows_list,_we_should_send_count(*)_LIMIT_1_request
search_test.go:958: We need to stop "unit" testing aggregation queries, because e.g. transformations aren't performed in tests whatsoever. Tests pass, but in real world things sometimes break. It's WIP.
=== RUN   TestFullQueryTestWIP/0_We_can't_deduct_hits_count_from_the_rows_list,_we_should_send_count(*)_LIMIT_1_request#01
search_test.go:958: We need to stop "unit" testing aggregation queries, because e.g. transformations aren't performed in tests whatsoever. Tests pass, but in real world things sometimes break. It's WIP.
=== RUN   TestFullQueryTestWIP/1_We_can_deduct_hits_count_from_the_rows_list,_we_shouldn't_any_count(*)_request
search_test.go:958: We need to stop "unit" testing aggregation queries, because e.g. transformations aren't performed in tests whatsoever. Tests pass, but in real world things sometimes break. It's WIP.
=== RUN   TestFullQueryTestWIP/1_We_can_deduct_hits_count_from_the_rows_list,_we_shouldn't_any_count(*)_request#01
search_test.go:958: We need to stop "unit" testing aggregation queries, because e.g. transformations aren't performed in tests whatsoever. Tests pass, but in real world things sometimes break. It's WIP.
=== RUN   TestFullQueryTestWIP/2_We_can_deduct_hits_count_from_the_rows_list,_we_shouldn't_any_count(*)_request,_we_should_return_gte_1
search_test.go:958: We need to stop "unit" testing aggregation queries, because e.g. transformations aren't performed in tests whatsoever. Tests pass, but in real world things sometimes break. It's WIP.
=== RUN   TestFullQueryTestWIP/2_We_can_deduct_hits_count_from_the_rows_list,_we_shouldn't_any_count(*)_request,_we_should_return_gte_1#01
search_test.go:958: We need to stop "unit" testing aggregation queries, because e.g. transformations aren't performed in tests whatsoever. Tests pass, but in real world things sometimes break. It's WIP.
=== RUN   TestFullQueryTestWIP/3_We_can_deduct_hits_count_from_the_rows_list,_we_shouldn't_any_count(*)_request,_we_should_return_eq_1
search_test.go:958: We need to stop "unit" testing aggregation queries, because e.g. transformations aren't performed in tests whatsoever. Tests pass, but in real world things sometimes break. It's WIP.
=== RUN   TestFullQueryTestWIP/3_We_can_deduct_hits_count_from_the_rows_list,_we_shouldn't_any_count(*)_request,_we_should_return_eq_1#01
search_test.go:958: We need to stop "unit" testing aggregation queries, because e.g. transformations aren't performed in tests whatsoever. Tests pass, but in real world things sometimes break. It's WIP.
=== RUN   TestFullQueryTestWIP/4_track_total_hits:_false
search_test.go:958: We need to stop "unit" testing aggregation queries, because e.g. transformations aren't performed in tests whatsoever. Tests pass, but in real world things sometimes break. It's WIP.
=== RUN   TestFullQueryTestWIP/4_track_total_hits:_false#01
search_test.go:958: We need to stop "unit" testing aggregation queries, because e.g. transformations aren't performed in tests whatsoever. Tests pass, but in real world things sometimes break. It's WIP.
=== RUN   TestFullQueryTestWIP/5_track_total_hits:_true,_size_>=_count(*)
search_test.go:958: We need to stop "unit" testing aggregation queries, because e.g. transformations aren't performed in tests whatsoever. Tests pass, but in real world things sometimes break. It's WIP.
=== RUN   TestFullQueryTestWIP/5_track_total_hits:_true,_size_>=_count(*)#01
search_test.go:958: We need to stop "unit" testing aggregation queries, because e.g. transformations aren't performed in tests whatsoever. Tests pass, but in real world things sometimes break. It's WIP.
=== RUN   TestFullQueryTestWIP/6_track_total_hits:_true,_size_<_count(*)
search_test.go:958: We need to stop "unit" testing aggregation queries, because e.g. transformations aren't performed in tests whatsoever. Tests pass, but in real world things sometimes break. It's WIP.
=== RUN   TestFullQueryTestWIP/6_track_total_hits:_true,_size_<_count(*)#01
search_test.go:958: We need to stop "unit" testing aggregation queries, because e.g. transformations aren't performed in tests whatsoever. Tests pass, but in real world things sometimes break. It's WIP.
=== RUN   TestFullQueryTestWIP/7_Turing_regression_test
Feb 10 13:50:29.696 DBG field 'score' referenced, but not found in schema, falling back to original name request_id=0194f020-3141-7c08-8b1a-611face75e4e
Feb 10 13:50:29.696 WRN applyPhysicalFromExpression: physical table name is not set
Feb 10 13:50:29.696 DBG Got field already resolved @timestamp
Feb 10 13:50:29.696 DBG Got field already resolved @timestamp
Feb 10 13:50:29.696 DBG Got field already resolved @timestamp
Feb 10 13:50:29.697 DBG Got field already resolved @timestamp
Feb 10 13:50:29.699 DBG Received debug info from secondary source: 0194f020-3141-7c08-8b1a-611face75e4e
=== RUN   TestFullQueryTestWIP/7_Turing_regression_test#01
Feb 10 13:50:29.699 INF async search request id: quesma_async_0194f020-3343-79d9-9a54-5911f603792a started async_id=quesma_async_0194f020-3343-79d9-9a54-5911f603792a request_id=0194f020-3141-7c08-8b1a-611face75e4e
Feb 10 13:50:29.700 DBG field 'score' referenced, but not found in schema, falling back to original name async_id=quesma_async_0194f020-3343-79d9-9a54-5911f603792a request_id=0194f020-3141-7c08-8b1a-611face75e4e
Feb 10 13:50:29.700 WRN applyPhysicalFromExpression: physical table name is not set
Feb 10 13:50:29.700 DBG Got field already resolved @timestamp
Feb 10 13:50:29.700 DBG Got field already resolved @timestamp
Feb 10 13:50:29.700 DBG Got field already resolved @timestamp
Feb 10 13:50:29.700 DBG Got field already resolved @timestamp
Feb 10 13:50:29.702 DBG Received debug info from secondary source: 0194f020-3141-7c08-8b1a-611face75e4e
=== RUN   TestFullQueryTestWIP/8_Turing_regression_test
Feb 10 13:50:29.706 DBG field 'path_id' referenced, but not found in schema, falling back to original name request_id=0194f020-3141-7c08-8b1a-611face75e4e
Feb 10 13:50:29.706 WRN applyPhysicalFromExpression: physical table name is not set
Feb 10 13:50:29.707 DBG Received debug info from secondary source: 0194f020-3141-7c08-8b1a-611face75e4e
=== RUN   TestFullQueryTestWIP/8_Turing_regression_test#01
Feb 10 13:50:29.708 INF async search request id: quesma_async_0194f020-334c-70a3-9c29-e406570c822f started async_id=quesma_async_0194f020-334c-70a3-9c29-e406570c822f request_id=0194f020-3141-7c08-8b1a-611face75e4e
Feb 10 13:50:29.708 DBG field 'path_id' referenced, but not found in schema, falling back to original name async_id=quesma_async_0194f020-334c-70a3-9c29-e406570c822f request_id=0194f020-3141-7c08-8b1a-611face75e4e
Feb 10 13:50:29.708 WRN applyPhysicalFromExpression: physical table name is not set
Feb 10 13:50:29.709 DBG Received debug info from secondary source: 0194f020-3141-7c08-8b1a-611face75e4e
--- FAIL: TestFullQueryTestWIP (0.03s)
--- SKIP: TestFullQueryTestWIP/0_We_can't_deduct_hits_count_from_the_rows_list,_we_should_send_count(*)_LIMIT_1_request (0.00s)
--- SKIP: TestFullQueryTestWIP/0_We_can't_deduct_hits_count_from_the_rows_list,_we_should_send_count(*)_LIMIT_1_request#01 (0.00s)
--- SKIP: TestFullQueryTestWIP/1_We_can_deduct_hits_count_from_the_rows_list,_we_shouldn't_any_count(*)_request (0.00s)
--- SKIP: TestFullQueryTestWIP/1_We_can_deduct_hits_count_from_the_rows_list,_we_shouldn't_any_count(*)_request#01 (0.00s)
--- SKIP: TestFullQueryTestWIP/2_We_can_deduct_hits_count_from_the_rows_list,_we_shouldn't_any_count(*)_request,_we_should_return_gte_1 (0.00s)
--- SKIP: TestFullQueryTestWIP/2_We_can_deduct_hits_count_from_the_rows_list,_we_shouldn't_any_count(*)_request,_we_should_return_gte_1#01 (0.00s)
--- SKIP: TestFullQueryTestWIP/3_We_can_deduct_hits_count_from_the_rows_list,_we_shouldn't_any_count(*)_request,_we_should_return_eq_1 (0.00s)
--- SKIP: TestFullQueryTestWIP/3_We_can_deduct_hits_count_from_the_rows_list,_we_shouldn't_any_count(*)_request,_we_should_return_eq_1#01 (0.00s)
--- SKIP: TestFullQueryTestWIP/4_track_total_hits:_false (0.00s)
--- SKIP: TestFullQueryTestWIP/4_track_total_hits:_false#01 (0.00s)
--- SKIP: TestFullQueryTestWIP/5_track_total_hits:_true,_size_>=_count(*) (0.00s)
--- SKIP: TestFullQueryTestWIP/5_track_total_hits:_true,_size_>=_count(*)#01 (0.00s)
--- SKIP: TestFullQueryTestWIP/6_track_total_hits:_true,_size_<_count(*) (0.00s)
--- SKIP: TestFullQueryTestWIP/6_track_total_hits:_true,_size_<_count(*)#01 (0.00s)
--- PASS: TestFullQueryTestWIP/7_Turing_regression_test (0.00s)
--- PASS: TestFullQueryTestWIP/7_Turing_regression_test#01 (0.01s)
--- PASS: TestFullQueryTestWIP/8_Turing_regression_test (0.00s)
--- PASS: TestFullQueryTestWIP/8_Turing_regression_test#01 (0.01s)
=== RUN   TestSearchAfterParameter_sortByJustTimestamp
=== RUN   TestSearchAfterParameter_sortByJustTimestamp/TestSearchAfterParameter:_handleSearch
Feb 10 13:50:29.720 WRN applyPhysicalFromExpression: physical table name is not set
Feb 10 13:50:29.721 DBG Got field already resolved @timestamp
Feb 10 13:50:29.721 DBG Got field already resolved message
Feb 10 13:50:29.721 DBG Got field already resolved @timestamp
Feb 10 13:50:29.723 WRN applyPhysicalFromExpression: physical table name is not set
Feb 10 13:50:29.723 DBG Got field already resolved @timestamp
Feb 10 13:50:29.723 DBG Got field already resolved message
Feb 10 13:50:29.723 DBG Got field already resolved @timestamp
Feb 10 13:50:29.724 DBG Received debug info from secondary source: 0194f020-3141-7c08-8b1a-611face75e4e
Feb 10 13:50:29.726 WRN applyPhysicalFromExpression: physical table name is not set
Feb 10 13:50:29.726 DBG Got field already resolved @timestamp
Feb 10 13:50:29.726 DBG Got field already resolved message
Feb 10 13:50:29.726 DBG Got field already resolved @timestamp
Feb 10 13:50:29.727 DBG Received debug info from secondary source: 0194f020-3141-7c08-8b1a-611face75e4e
Feb 10 13:50:29.729 WRN applyPhysicalFromExpression: physical table name is not set
Feb 10 13:50:29.729 DBG Got field already resolved @timestamp
Feb 10 13:50:29.729 DBG Got field already resolved message
Feb 10 13:50:29.729 DBG Got field already resolved @timestamp
Feb 10 13:50:29.730 DBG Received debug info from secondary source: 0194f020-3141-7c08-8b1a-611face75e4e
Feb 10 13:50:29.732 DBG Received debug info from secondary source: 0194f020-3141-7c08-8b1a-611face75e4e
=== RUN   TestSearchAfterParameter_sortByJustTimestamp/TestSearchAfterParameter:_handleAsyncSearch
Feb 10 13:50:29.733 INF async search request id: quesma_async_0194f020-3365-7b9e-9cc9-ed800e4ab7af started async_id=quesma_async_0194f020-3365-7b9e-9cc9-ed800e4ab7af request_id=0194f020-3141-7c08-8b1a-611face75e4e
Feb 10 13:50:29.734 WRN applyPhysicalFromExpression: physical table name is not set
Feb 10 13:50:29.734 DBG Got field already resolved @timestamp
Feb 10 13:50:29.734 DBG Got field already resolved message
Feb 10 13:50:29.734 DBG Got field already resolved @timestamp
Feb 10 13:50:29.736 DBG Received debug info from secondary source: 0194f020-3141-7c08-8b1a-611face75e4e
Feb 10 13:50:29.741 INF async search request id: quesma_async_0194f020-336d-793e-9f10-e54d25eda102 started async_id=quesma_async_0194f020-336d-793e-9f10-e54d25eda102 request_id=0194f020-3141-7c08-8b1a-611face75e4e
Feb 10 13:50:29.742 WRN applyPhysicalFromExpression: physical table name is not set
Feb 10 13:50:29.742 DBG Got field already resolved @timestamp
Feb 10 13:50:29.742 DBG Got field already resolved message
Feb 10 13:50:29.742 DBG Got field already resolved @timestamp
Feb 10 13:50:29.743 DBG Received debug info from secondary source: 0194f020-3141-7c08-8b1a-611face75e4e
Feb 10 13:50:29.749 INF async search request id: quesma_async_0194f020-3375-7c1e-9b34-4c0016ab0df8 started async_id=quesma_async_0194f020-3375-7c1e-9b34-4c0016ab0df8 request_id=0194f020-3141-7c08-8b1a-611face75e4e
Feb 10 13:50:29.750 WRN applyPhysicalFromExpression: physical table name is not set
Feb 10 13:50:29.750 DBG Got field already resolved @timestamp
Feb 10 13:50:29.750 DBG Got field already resolved message
Feb 10 13:50:29.750 DBG Got field already resolved @timestamp
Feb 10 13:50:29.751 DBG Received debug info from secondary source: 0194f020-3141-7c08-8b1a-611face75e4e
Feb 10 13:50:29.757 INF async search request id: quesma_async_0194f020-337d-716a-905d-76c360f3fc7a started async_id=quesma_async_0194f020-337d-716a-905d-76c360f3fc7a request_id=0194f020-3141-7c08-8b1a-611face75e4e
Feb 10 13:50:29.757 WRN applyPhysicalFromExpression: physical table name is not set
Feb 10 13:50:29.757 DBG Got field already resolved @timestamp
Feb 10 13:50:29.757 DBG Got field already resolved message
Feb 10 13:50:29.757 DBG Got field already resolved @timestamp
Feb 10 13:50:29.758 DBG Received debug info from secondary source: 0194f020-3141-7c08-8b1a-611face75e4e
--- PASS: TestSearchAfterParameter_sortByJustTimestamp (0.04s)
--- PASS: TestSearchAfterParameter_sortByJustTimestamp/TestSearchAfterParameter:_handleSearch (0.01s)
--- PASS: TestSearchAfterParameter_sortByJustTimestamp/TestSearchAfterParameter:_handleAsyncSearch (0.03s)
=== RUN   TestSearchAfterParameter_sortByJustOneStringField
=== RUN   TestSearchAfterParameter_sortByJustOneStringField/TestSearchAfterParameter:_handleSearch
Feb 10 13:50:29.764 WRN applyPhysicalFromExpression: physical table name is not set
Feb 10 13:50:29.764 DBG Got field already resolved message
Feb 10 13:50:29.764 DBG Got field already resolved message
Feb 10 13:50:29.765 DBG Received debug info from secondary source: 0194f020-3141-7c08-8b1a-611face75e4e
Feb 10 13:50:29.765 WRN applyPhysicalFromExpression: physical table name is not set
Feb 10 13:50:29.766 DBG Got field already resolved message
Feb 10 13:50:29.766 DBG Got field already resolved message
Feb 10 13:50:29.767 DBG Received debug info from secondary source: 0194f020-3141-7c08-8b1a-611face75e4e
Feb 10 13:50:29.767 WRN applyPhysicalFromExpression: physical table name is not set
Feb 10 13:50:29.767 DBG Got field already resolved message
Feb 10 13:50:29.767 DBG Got field already resolved message
Feb 10 13:50:29.768 DBG Received debug info from secondary source: 0194f020-3141-7c08-8b1a-611face75e4e
Feb 10 13:50:29.768 WRN applyPhysicalFromExpression: physical table name is not set
Feb 10 13:50:29.768 DBG Got field already resolved message
Feb 10 13:50:29.768 DBG Got field already resolved message
Feb 10 13:50:29.769 DBG Received debug info from secondary source: 0194f020-3141-7c08-8b1a-611face75e4e
=== RUN   TestSearchAfterParameter_sortByJustOneStringField/TestSearchAfterParameter:_handleAsyncSearch
Feb 10 13:50:29.769 INF async search request id: quesma_async_0194f020-3389-7ed1-98d5-a1630ca84390 started async_id=quesma_async_0194f020-3389-7ed1-98d5-a1630ca84390 request_id=0194f020-3141-7c08-8b1a-611face75e4e
Feb 10 13:50:29.770 WRN applyPhysicalFromExpression: physical table name is not set
Feb 10 13:50:29.770 DBG Got field already resolved message
Feb 10 13:50:29.770 DBG Got field already resolved message
Feb 10 13:50:29.771 DBG Received debug info from secondary source: 0194f020-3141-7c08-8b1a-611face75e4e
Feb 10 13:50:29.776 INF async search request id: quesma_async_0194f020-3390-7253-b7b0-2257b19cc6b4 started async_id=quesma_async_0194f020-3390-7253-b7b0-2257b19cc6b4 request_id=0194f020-3141-7c08-8b1a-611face75e4e
Feb 10 13:50:29.776 WRN applyPhysicalFromExpression: physical table name is not set
Feb 10 13:50:29.776 DBG Got field already resolved message
Feb 10 13:50:29.776 DBG Got field already resolved message
Feb 10 13:50:29.777 DBG Received debug info from secondary source: 0194f020-3141-7c08-8b1a-611face75e4e
Feb 10 13:50:29.781 INF async search request id: quesma_async_0194f020-3395-78e9-af29-c2b7b0283c99 started async_id=quesma_async_0194f020-3395-78e9-af29-c2b7b0283c99 request_id=0194f020-3141-7c08-8b1a-611face75e4e
Feb 10 13:50:29.781 WRN applyPhysicalFromExpression: physical table name is not set
Feb 10 13:50:29.781 DBG Got field already resolved message
Feb 10 13:50:29.782 DBG Got field already resolved message
Feb 10 13:50:29.782 DBG Received debug info from secondary source: 0194f020-3141-7c08-8b1a-611face75e4e
Feb 10 13:50:29.786 INF async search request id: quesma_async_0194f020-339a-7b35-be1e-625c37eefb37 started async_id=quesma_async_0194f020-339a-7b35-be1e-625c37eefb37 request_id=0194f020-3141-7c08-8b1a-611face75e4e
Feb 10 13:50:29.787 WRN applyPhysicalFromExpression: physical table name is not set
Feb 10 13:50:29.787 DBG Got field already resolved message
Feb 10 13:50:29.787 DBG Got field already resolved message
Feb 10 13:50:29.788 DBG Received debug info from secondary source: 0194f020-3141-7c08-8b1a-611face75e4e
--- PASS: TestSearchAfterParameter_sortByJustOneStringField (0.03s)
--- PASS: TestSearchAfterParameter_sortByJustOneStringField/TestSearchAfterParameter:_handleSearch (0.01s)
--- PASS: TestSearchAfterParameter_sortByJustOneStringField/TestSearchAfterParameter:_handleAsyncSearch (0.02s)
=== RUN   TestSearchAfterParameter_sortByMultipleFields
=== RUN   TestSearchAfterParameter_sortByMultipleFields/TestSearchAfterParameter:_handleSearch
Feb 10 13:50:29.792 WRN applyPhysicalFromExpression: physical table name is not set
Feb 10 13:50:29.792 DBG Got field already resolved @timestamp
Feb 10 13:50:29.793 DBG Got field already resolved bicep_size
Feb 10 13:50:29.793 DBG Got field already resolved message
Feb 10 13:50:29.793 DBG Got field already resolved @timestamp
Feb 10 13:50:29.793 DBG Got field already resolved message
Feb 10 13:50:29.793 DBG Got field already resolved bicep_size
Feb 10 13:50:29.794 WRN applyPhysicalFromExpression: physical table name is not set
Feb 10 13:50:29.795 DBG Got field already resolved @timestamp
Feb 10 13:50:29.795 DBG Received debug info from secondary source: 0194f020-3141-7c08-8b1a-611face75e4e
Feb 10 13:50:29.795 DBG Got field already resolved bicep_size
Feb 10 13:50:29.795 DBG Got field already resolved message
Feb 10 13:50:29.795 DBG Got field already resolved @timestamp
Feb 10 13:50:29.795 DBG Got field already resolved message
Feb 10 13:50:29.795 DBG Got field already resolved bicep_size
Feb 10 13:50:29.797 DBG Received debug info from secondary source: 0194f020-3141-7c08-8b1a-611face75e4e
Feb 10 13:50:29.797 WRN applyPhysicalFromExpression: physical table name is not set
Feb 10 13:50:29.797 DBG Got field already resolved @timestamp
Feb 10 13:50:29.797 DBG Got field already resolved bicep_size
Feb 10 13:50:29.797 DBG Got field already resolved message
Feb 10 13:50:29.797 DBG Got field already resolved @timestamp
Feb 10 13:50:29.798 DBG Got field already resolved message
Feb 10 13:50:29.798 DBG Got field already resolved bicep_size
Feb 10 13:50:29.799 DBG Received debug info from secondary source: 0194f020-3141-7c08-8b1a-611face75e4e
Feb 10 13:50:29.799 WRN applyPhysicalFromExpression: physical table name is not set
Feb 10 13:50:29.800 DBG Got field already resolved @timestamp
Feb 10 13:50:29.800 DBG Got field already resolved bicep_size
Feb 10 13:50:29.800 DBG Got field already resolved message
Feb 10 13:50:29.800 DBG Got field already resolved @timestamp
Feb 10 13:50:29.800 DBG Got field already resolved message
Feb 10 13:50:29.800 DBG Got field already resolved bicep_size
Feb 10 13:50:29.802 DBG Received debug info from secondary source: 0194f020-3141-7c08-8b1a-611face75e4e
=== RUN   TestSearchAfterParameter_sortByMultipleFields/TestSearchAfterParameter:_handleAsyncSearch
Feb 10 13:50:29.803 INF async search request id: quesma_async_0194f020-33ab-7046-b3b5-24b04bb43971 started async_id=quesma_async_0194f020-33ab-7046-b3b5-24b04bb43971 request_id=0194f020-3141-7c08-8b1a-611face75e4e
Feb 10 13:50:29.803 WRN applyPhysicalFromExpression: physical table name is not set
Feb 10 13:50:29.803 DBG Got field already resolved @timestamp
Feb 10 13:50:29.803 DBG Got field already resolved bicep_size
Feb 10 13:50:29.803 DBG Got field already resolved message
Feb 10 13:50:29.803 DBG Got field already resolved @timestamp
Feb 10 13:50:29.803 DBG Got field already resolved message
Feb 10 13:50:29.803 DBG Got field already resolved bicep_size
Feb 10 13:50:29.805 DBG Received debug info from secondary source: 0194f020-3141-7c08-8b1a-611face75e4e
Feb 10 13:50:29.809 INF async search request id: quesma_async_0194f020-33b1-77ec-9b0f-b676cce0158f started async_id=quesma_async_0194f020-33b1-77ec-9b0f-b676cce0158f request_id=0194f020-3141-7c08-8b1a-611face75e4e
Feb 10 13:50:29.809 WRN applyPhysicalFromExpression: physical table name is not set
Feb 10 13:50:29.809 DBG Got field already resolved @timestamp
Feb 10 13:50:29.810 DBG Got field already resolved bicep_size
Feb 10 13:50:29.810 DBG Got field already resolved message
Feb 10 13:50:29.810 DBG Got field already resolved @timestamp
Feb 10 13:50:29.810 DBG Got field already resolved message
Feb 10 13:50:29.810 DBG Got field already resolved bicep_size
Feb 10 13:50:29.811 DBG Received debug info from secondary source: 0194f020-3141-7c08-8b1a-611face75e4e
Feb 10 13:50:29.817 INF async search request id: quesma_async_0194f020-33b9-7194-850d-969c447d3c84 started async_id=quesma_async_0194f020-33b9-7194-850d-969c447d3c84 request_id=0194f020-3141-7c08-8b1a-611face75e4e
Feb 10 13:50:29.817 WRN applyPhysicalFromExpression: physical table name is not set
Feb 10 13:50:29.817 DBG Got field already resolved @timestamp
Feb 10 13:50:29.817 DBG Got field already resolved bicep_size
Feb 10 13:50:29.817 DBG Got field already resolved message
Feb 10 13:50:29.817 DBG Got field already resolved @timestamp
Feb 10 13:50:29.817 DBG Got field already resolved message
Feb 10 13:50:29.818 DBG Got field already resolved bicep_size
Feb 10 13:50:29.819 DBG Received debug info from secondary source: 0194f020-3141-7c08-8b1a-611face75e4e
Feb 10 13:50:29.824 INF async search request id: quesma_async_0194f020-33c0-7ae9-8842-bca0bdfccc09 started async_id=quesma_async_0194f020-33c0-7ae9-8842-bca0bdfccc09 request_id=0194f020-3141-7c08-8b1a-611face75e4e
Feb 10 13:50:29.825 WRN applyPhysicalFromExpression: physical table name is not set
Feb 10 13:50:29.825 DBG Got field already resolved @timestamp
Feb 10 13:50:29.825 DBG Got field already resolved bicep_size
Feb 10 13:50:29.825 DBG Got field already resolved message
Feb 10 13:50:29.825 DBG Got field already resolved @timestamp
Feb 10 13:50:29.825 DBG Got field already resolved message
Feb 10 13:50:29.825 DBG Got field already resolved bicep_size
Feb 10 13:50:29.827 DBG Received debug info from secondary source: 0194f020-3141-7c08-8b1a-611face75e4e
--- PASS: TestSearchAfterParameter_sortByMultipleFields (0.04s)
--- PASS: TestSearchAfterParameter_sortByMultipleFields/TestSearchAfterParameter:_handleSearch (0.01s)
--- PASS: TestSearchAfterParameter_sortByMultipleFields/TestSearchAfterParameter:_handleAsyncSearch (0.03s)
=== RUN   TestSearchAfterParameter_sortByNoField
=== RUN   TestSearchAfterParameter_sortByNoField/TestSearchAfterParameter:_handleSearch
Feb 10 13:50:29.832 DBG field '_score' referenced, but not found in schema, falling back to original name request_id=0194f020-3141-7c08-8b1a-611face75e4e
Feb 10 13:50:29.832 WRN applyPhysicalFromExpression: physical table name is not set
Feb 10 13:50:29.832 DBG Got field already resolved @timestamp
Feb 10 13:50:29.832 DBG Got field already resolved bicep_size
Feb 10 13:50:29.832 DBG Got field already resolved message
Feb 10 13:50:29.833 DBG Received debug info from secondary source: 0194f020-3141-7c08-8b1a-611face75e4e
=== RUN   TestSearchAfterParameter_sortByNoField/TestSearchAfterParameter:_handleAsyncSearch
Feb 10 13:50:29.834 INF async search request id: quesma_async_0194f020-33ca-71c0-aed4-bb9b584fc16e started async_id=quesma_async_0194f020-33ca-71c0-aed4-bb9b584fc16e request_id=0194f020-3141-7c08-8b1a-611face75e4e
Feb 10 13:50:29.834 DBG field '_score' referenced, but not found in schema, falling back to original name async_id=quesma_async_0194f020-33ca-71c0-aed4-bb9b584fc16e request_id=0194f020-3141-7c08-8b1a-611face75e4e
Feb 10 13:50:29.834 WRN applyPhysicalFromExpression: physical table name is not set
Feb 10 13:50:29.834 DBG Got field already resolved @timestamp
Feb 10 13:50:29.834 DBG Got field already resolved bicep_size
Feb 10 13:50:29.834 DBG Got field already resolved message
Feb 10 13:50:29.835 DBG Received debug info from secondary source: 0194f020-3141-7c08-8b1a-611face75e4e
--- PASS: TestSearchAfterParameter_sortByNoField (0.01s)
--- PASS: TestSearchAfterParameter_sortByNoField/TestSearchAfterParameter:_handleSearch (0.00s)
--- PASS: TestSearchAfterParameter_sortByNoField/TestSearchAfterParameter:_handleAsyncSearch (0.01s)
=== RUN   Test_backendConnectorValidation
Feb 10 13:50:29.840 DBG Endpoints:

Feb 10 13:50:29.840 INF Dependency injection into *quesma_api.Quesma :SKIP - No dependencies to inject. It doesn't implement DependenciesSetter interface.
Feb 10 13:50:29.840 INF Dependency injection into *quesma_api.Pipeline :Injecting dependencies intopipeline(0xc0060757a0)
Feb 10 13:50:29.840 INF Dependency injection into *quesma_api.Pipeline :OK - Injected Dependencies
Feb 10 13:50:29.840 INF Dependency injection into *processors.PostgresToMySqlProcessor :SKIP - No dependencies to inject. It doesn't implement DependenciesSetter interface.
Feb 10 13:50:29.840 INF Dependency injection into *backend_connectors.MySqlBackendConnector :SKIP - No dependencies to inject. It doesn't implement DependenciesSetter interface.
Feb 10 13:50:29.841 INF Dependency injection into *quesma_api.NoopBackendConnector :SKIP - No dependencies to inject. It doesn't implement DependenciesSetter interface.
Feb 10 13:50:29.841 DBG Component tree:
*quesma_api.Quesma(0xc000723950)
pipeline(0xc0060757a0)
PostgresToMySqlProcessor
mysql
noop

--- PASS: Test_backendConnectorValidation (0.00s)
=== RUN   Test_fallbackScenario
Feb 10 13:50:29.841 INF BasicHTTPFrontendConnector::8888, index: 0, pipeline:0
Feb 10 13:50:29.841 DBG Endpoints:
:8888:
pipeline 0, connector 0

Feb 10 13:50:29.841 INF Dependency injection into *quesma_api.Quesma :SKIP - No dependencies to inject. It doesn't implement DependenciesSetter interface.
Feb 10 13:50:29.841 INF Dependency injection into *quesma_api.Pipeline :Injecting dependencies intopipeline(0xc006075800)
Feb 10 13:50:29.842 INF Dependency injection into *quesma_api.Pipeline :OK - Injected Dependencies
Feb 10 13:50:29.842 INF Dependency injection into *frontend_connectors.BasicHTTPFrontendConnector :Injecting dependencies intoBasicHTTPFrontendConnector
Feb 10 13:50:29.842 INF Dependency injection into *frontend_connectors.BasicHTTPFrontendConnector :OK - Injected Dependencies
Feb 10 13:50:29.842 INF Dependency injection into *quesma_api.PathRouter :SKIP - No dependencies to inject. It doesn't implement DependenciesSetter interface.
Feb 10 13:50:29.842 INF Dependency injection into *frontend_connectors.Dispatcher :OK - Injected Dependencies
Feb 10 13:50:29.842 INF Dependency injection into *quesma_api.NoopBackendConnector :SKIP - No dependencies to inject. It doesn't implement DependenciesSetter interface.
Feb 10 13:50:29.842 DBG Component tree:
*quesma_api.Quesma(0xc000786180)
pipeline(0xc006075800)
BasicHTTPFrontendConnector
*quesma_api.PathRouter(0xc003765660)
*frontend_connectors.Dispatcher(0xc003716eb0)
noop

Feb 10 13:50:29.843 INF Starting pipeline &{ [0xc0064813f0] [] map[0:0x3315080] 0xc006483490}
Feb 10 13:50:29.843 INF Starting frontend connector :8888 component=pipeline(0xc006075800)
Feb 10 13:50:29.843 INF HTTP server started on :8888 component=BasicHTTPFrontendConnector
unknown

unknown

unknown

unknown

Feb 10 13:50:30.848 ERR HTTP server stopped error="http: Server closed" component=BasicHTTPFrontendConnector
--- PASS: Test_fallbackScenario (1.01s)
=== RUN   Test_scenario1
Feb 10 13:50:30.848 INF BasicHTTPFrontendConnector::8888, index: 0, pipeline:0
Feb 10 13:50:30.848 INF BasicHTTPFrontendConnector::8888, index: 0, pipeline:1
Feb 10 13:50:30.849 DBG Endpoints:
:8888:
pipeline 0, connector 0
pipeline 1, connector 0

Feb 10 13:50:30.849 INF Sharing frontend connector 0xc0039f94a0 with 0xc0039f9550
Feb 10 13:50:30.849 INF Sharing frontend connector 0xc0039f94a0 with 0xc0039f94a0
Feb 10 13:50:30.849 INF Dependency injection into *quesma_api.Quesma :SKIP - No dependencies to inject. It doesn't implement DependenciesSetter interface.
Feb 10 13:50:30.849 INF Dependency injection into *quesma_api.Pipeline :Injecting dependencies intopipeline(0xc003a33020)
Feb 10 13:50:30.849 INF Dependency injection into *quesma_api.Pipeline :OK - Injected Dependencies
Feb 10 13:50:30.849 INF Dependency injection into *frontend_connectors.BasicHTTPFrontendConnector :Injecting dependencies intoBasicHTTPFrontendConnector
Feb 10 13:50:30.850 INF Dependency injection into *frontend_connectors.BasicHTTPFrontendConnector :OK - Injected Dependencies
Feb 10 13:50:30.850 INF Dependency injection into *quesma_api.PathRouter :SKIP - No dependencies to inject. It doesn't implement DependenciesSetter interface.
Feb 10 13:50:30.850 INF Dependency injection into *frontend_connectors.Dispatcher :OK - Injected Dependencies
Feb 10 13:50:30.850 INF Dependency injection into *quesma.IngestProcessor :Injecting dependencies intoIngestProcessor
Feb 10 13:50:30.850 INF Dependency injection into *quesma.IngestProcessor :OK - Injected Dependencies
Feb 10 13:50:30.850 INF Dependency injection into *processors.ABTestProcessor :SKIP - No dependencies to inject. It doesn't implement DependenciesSetter interface.
Feb 10 13:50:30.850 INF Dependency injection into *quesma_api.NoopBackendConnector :SKIP - No dependencies to inject. It doesn't implement DependenciesSetter interface.
Feb 10 13:50:30.851 INF Dependency injection into *quesma_api.Pipeline :Injecting dependencies intopipeline(0xc003a33080)
Feb 10 13:50:30.851 INF Dependency injection into *quesma_api.Pipeline :OK - Injected Dependencies
Feb 10 13:50:30.851 INF Dependency injection into *frontend_connectors.BasicHTTPFrontendConnector :Injecting dependencies intoBasicHTTPFrontendConnector
Feb 10 13:50:30.851 INF Dependency injection into *frontend_connectors.BasicHTTPFrontendConnector :OK - Injected Dependencies
Feb 10 13:50:30.851 INF Dependency injection into *quesma_api.PathRouter :SKIP - No dependencies to inject. It doesn't implement DependenciesSetter interface.
Feb 10 13:50:30.851 INF Dependency injection into *frontend_connectors.Dispatcher :OK - Injected Dependencies
Feb 10 13:50:30.851 INF Dependency injection into *quesma.QueryProcessor :SKIP - No dependencies to inject. It doesn't implement DependenciesSetter interface.
Feb 10 13:50:30.851 INF Dependency injection into *processors.ABTestProcessor :SKIP - No dependencies to inject. It doesn't implement DependenciesSetter interface.
Feb 10 13:50:30.852 INF Dependency injection into *quesma_api.NoopBackendConnector :SKIP - No dependencies to inject. It doesn't implement DependenciesSetter interface.
Feb 10 13:50:30.852 DBG Component tree:
*quesma_api.Quesma(0xc0002077a0)
pipeline(0xc003a33020)
BasicHTTPFrontendConnector
*quesma_api.PathRouter(0xc005be95a0)
*frontend_connectors.Dispatcher(0xc0001688c0)
IngestProcessor
ABTestProcessor
noop
pipeline(0xc003a33080)
BasicHTTPFrontendConnector
*quesma_api.PathRouter(0xc005be95a0)
*frontend_connectors.Dispatcher(0xc0001688c0)
QueryProcessor
ABTestProcessor
noop

Feb 10 13:50:30.852 INF Starting pipeline &{ [0xc0039f94a0] [0xc004fd6140 0xc000168a00] map[0:0x3315080] 0xc0062b3110}
Feb 10 13:50:30.852 INF Starting frontend connector :8888 component=pipeline(0xc003a33020)
Feb 10 13:50:30.852 INF Starting pipeline &{ [0xc0039f94a0] [0xc000207a10 0xc000168b40] map[0:0x3315080] 0xc0062b3420}
Feb 10 13:50:30.852 INF HTTP server started on :8888 component=BasicHTTPFrontendConnector
Feb 10 13:50:30.852 INF Starting frontend connector :8888 component=pipeline(0xc003a33080)
Feb 10 13:50:31.854 INF IngestProcessor: handling message []interface {} component=IngestProcessor
bulk->IngestProcessor->InnerIngestProcessor1->0ABIngestTestProcessor
bulk->IngestProcessor->InnerIngestProcessor2->0ABIngestTestProcessor

Feb 10 13:50:31.858 INF IngestProcessor: handling message []interface {} component=IngestProcessor
doc->IngestProcessor->InnerIngestProcessor1->0ABIngestTestProcessor
doc->IngestProcessor->InnerIngestProcessor2->0ABIngestTestProcessor

ABTestProcessor processor: Responses are equal

ABTestProcessor processor: Responses are not equal

--- PASS: Test_scenario1 (1.02s)
Feb 10 13:50:31.866 ERR HTTP server stopped error="http: Server closed" component=BasicHTTPFrontendConnector
=== RUN   Test_middleware
Feb 10 13:50:31.866 INF BasicHTTPFrontendConnector::8888, index: 0, pipeline:0
Feb 10 13:50:31.867 DBG Endpoints:
:8888:
pipeline 0, connector 0

Feb 10 13:50:31.867 INF Dependency injection into *quesma_api.Quesma :SKIP - No dependencies to inject. It doesn't implement DependenciesSetter interface.
Feb 10 13:50:31.867 INF Dependency injection into *quesma_api.Pipeline :Injecting dependencies intopipeline(0xc00067c060)
Feb 10 13:50:31.867 INF Dependency injection into *quesma_api.Pipeline :OK - Injected Dependencies
Feb 10 13:50:31.867 INF Dependency injection into *frontend_connectors.BasicHTTPFrontendConnector :Injecting dependencies intoBasicHTTPFrontendConnector
Feb 10 13:50:31.867 INF Dependency injection into *frontend_connectors.BasicHTTPFrontendConnector :OK - Injected Dependencies
Feb 10 13:50:31.867 INF Dependency injection into *quesma_api.PathRouter :SKIP - No dependencies to inject. It doesn't implement DependenciesSetter interface.
Feb 10 13:50:31.868 INF Dependency injection into *frontend_connectors.Dispatcher :OK - Injected Dependencies
Feb 10 13:50:31.868 INF Dependency injection into *quesma.IngestProcessor :Injecting dependencies intoIngestProcessor
Feb 10 13:50:31.868 INF Dependency injection into *quesma.IngestProcessor :OK - Injected Dependencies
Feb 10 13:50:31.868 INF Dependency injection into *quesma_api.NoopBackendConnector :SKIP - No dependencies to inject. It doesn't implement DependenciesSetter interface.
Feb 10 13:50:31.868 DBG Component tree:
*quesma_api.Quesma(0xc0007969c0)
pipeline(0xc00067c060)
BasicHTTPFrontendConnector
*quesma_api.PathRouter(0xc00020e580)
*frontend_connectors.Dispatcher(0xc0021e2b40)
IngestProcessor
noop

Feb 10 13:50:31.868 INF Starting pipeline &{ [0xc002720210] [0xc00271e400] map[0:0x3315080] 0xc0022baaf0}
Feb 10 13:50:31.868 INF Starting frontend connector :8888 component=pipeline(0xc00067c060)
Feb 10 13:50:31.868 INF HTTP server started on :8888 component=BasicHTTPFrontendConnector
middleware

middleware

middleware

middleware

Feb 10 13:50:32.871 ERR HTTP server stopped error="http: Server closed" component=BasicHTTPFrontendConnector
Feb 10 13:50:32.871 INF BasicHTTPFrontendConnector::8888, index: 0, pipeline:0
Feb 10 13:50:32.871 DBG Endpoints:
:8888:
pipeline 0, connector 0

Feb 10 13:50:32.871 INF Dependency injection into *quesma_api.Quesma :SKIP - No dependencies to inject. It doesn't implement DependenciesSetter interface.
Feb 10 13:50:32.871 INF Dependency injection into *quesma_api.Pipeline :Injecting dependencies intopipeline(0xc00067d3e0)
Feb 10 13:50:32.872 INF Dependency injection into *quesma_api.Pipeline :OK - Injected Dependencies
Feb 10 13:50:32.872 INF Dependency injection into *frontend_connectors.BasicHTTPFrontendConnector :Injecting dependencies intoBasicHTTPFrontendConnector
Feb 10 13:50:32.872 INF Dependency injection into *frontend_connectors.BasicHTTPFrontendConnector :OK - Injected Dependencies
Feb 10 13:50:32.872 INF Dependency injection into *quesma_api.PathRouter :SKIP - No dependencies to inject. It doesn't implement DependenciesSetter interface.
Feb 10 13:50:32.872 INF Dependency injection into *frontend_connectors.Dispatcher :OK - Injected Dependencies
Feb 10 13:50:32.872 INF Dependency injection into *quesma.IngestProcessor :Injecting dependencies intoIngestProcessor
Feb 10 13:50:32.872 INF Dependency injection into *quesma.IngestProcessor :OK - Injected Dependencies
Feb 10 13:50:32.872 INF Dependency injection into *quesma_api.NoopBackendConnector :SKIP - No dependencies to inject. It doesn't implement DependenciesSetter interface.
Feb 10 13:50:32.872 DBG Component tree:
*quesma_api.Quesma(0xc003474180)
pipeline(0xc00067d3e0)
BasicHTTPFrontendConnector
*quesma_api.PathRouter(0xc00020e840)
*frontend_connectors.Dispatcher(0xc0021e3860)
IngestProcessor
noop

Feb 10 13:50:32.873 INF Starting pipeline &{ [0xc002720370] [0xc00271eb00] map[0:0x3315080] 0xc0000e8700}
Feb 10 13:50:32.873 INF Starting frontend connector :8888 component=pipeline(0xc00067d3e0)
Feb 10 13:50:32.873 INF HTTP server started on :8888 component=BasicHTTPFrontendConnector




Feb 10 13:50:33.875 ERR HTTP server stopped error="http: Server closed" component=BasicHTTPFrontendConnector
--- PASS: Test_middleware (2.01s)
=== RUN   Test_QuesmaBuild
Feb 10 13:50:33.876 INF BasicHTTPFrontendConnector::8888, index: 0, pipeline:0
Feb 10 13:50:33.876 INF BasicHTTPFrontendConnector::8889, index: 0, pipeline:1
Feb 10 13:50:33.876 DBG Endpoints:
:8888:
pipeline 0, connector 0
:8889:
pipeline 1, connector 0

Feb 10 13:50:33.876 INF Dependency injection into *quesma_api.Quesma :SKIP - No dependencies to inject. It doesn't implement DependenciesSetter interface.
Feb 10 13:50:33.877 INF Dependency injection into *quesma_api.Pipeline :Injecting dependencies intopipeline(0xc000351a40)
Feb 10 13:50:33.877 INF Dependency injection into *quesma_api.Pipeline :OK - Injected Dependencies
Feb 10 13:50:33.877 INF Dependency injection into *frontend_connectors.BasicHTTPFrontendConnector :Injecting dependencies intoBasicHTTPFrontendConnector
Feb 10 13:50:33.877 INF Dependency injection into *frontend_connectors.BasicHTTPFrontendConnector :OK - Injected Dependencies
Feb 10 13:50:33.877 INF Dependency injection into *quesma_api.PathRouter :SKIP - No dependencies to inject. It doesn't implement DependenciesSetter interface.
Feb 10 13:50:33.877 INF Dependency injection into *frontend_connectors.Dispatcher :OK - Injected Dependencies
Feb 10 13:50:33.877 INF Dependency injection into *quesma_api.NoopBackendConnector :SKIP - No dependencies to inject. It doesn't implement DependenciesSetter interface.
Feb 10 13:50:33.877 INF Dependency injection into *quesma_api.Pipeline :Injecting dependencies intopipeline(0xc000351aa0)
Feb 10 13:50:33.877 INF Dependency injection into *quesma_api.Pipeline :OK - Injected Dependencies
Feb 10 13:50:33.877 INF Dependency injection into *frontend_connectors.BasicHTTPFrontendConnector :Injecting dependencies intoBasicHTTPFrontendConnector
Feb 10 13:50:33.878 INF Dependency injection into *frontend_connectors.BasicHTTPFrontendConnector :OK - Injected Dependencies
Feb 10 13:50:33.878 INF Dependency injection into *quesma_api.PathRouter :SKIP - No dependencies to inject. It doesn't implement DependenciesSetter interface.
Feb 10 13:50:33.878 INF Dependency injection into *frontend_connectors.Dispatcher :OK - Injected Dependencies
Feb 10 13:50:33.878 INF Dependency injection into *quesma_api.NoopBackendConnector :SKIP - No dependencies to inject. It doesn't implement DependenciesSetter interface.
Feb 10 13:50:33.878 DBG Component tree:
*quesma_api.Quesma(0xc00117f170)
pipeline(0xc000351a40)
BasicHTTPFrontendConnector
*quesma_api.PathRouter(0xc0064f5ce0)
*frontend_connectors.Dispatcher(0xc0034f1ef0)
noop
pipeline(0xc000351aa0)
BasicHTTPFrontendConnector
*quesma_api.PathRouter(0xc0064f5d40)
*frontend_connectors.Dispatcher(0xc00021e2d0)
noop

Feb 10 13:50:33.878 INF BasicHTTPFrontendConnector::8888, index: 0, pipeline:0
Feb 10 13:50:33.878 INF BasicHTTPFrontendConnector::8888, index: 0, pipeline:1
Feb 10 13:50:33.878 DBG Endpoints:
:8888:
pipeline 0, connector 0
pipeline 1, connector 0

Feb 10 13:50:33.879 INF Sharing frontend connector 0xc003472210 with 0xc0034722c0
Feb 10 13:50:33.879 INF Sharing frontend connector 0xc003472210 with 0xc003472210
Feb 10 13:50:33.879 INF Dependency injection into *quesma_api.Quesma :SKIP - No dependencies to inject. It doesn't implement DependenciesSetter interface.
Feb 10 13:50:33.879 INF Dependency injection into *quesma_api.Pipeline :Injecting dependencies intopipeline(0xc000351b00)
Feb 10 13:50:33.879 INF Dependency injection into *quesma_api.Pipeline :OK - Injected Dependencies
Feb 10 13:50:33.879 INF Dependency injection into *frontend_connectors.BasicHTTPFrontendConnector :Injecting dependencies intoBasicHTTPFrontendConnector
Feb 10 13:50:33.879 INF Dependency injection into *frontend_connectors.BasicHTTPFrontendConnector :OK - Injected Dependencies
Feb 10 13:50:33.879 INF Dependency injection into *quesma_api.PathRouter :SKIP - No dependencies to inject. It doesn't implement DependenciesSetter interface.
Feb 10 13:50:33.879 INF Dependency injection into *frontend_connectors.Dispatcher :OK - Injected Dependencies
Feb 10 13:50:33.879 INF Dependency injection into *quesma_api.NoopBackendConnector :SKIP - No dependencies to inject. It doesn't implement DependenciesSetter interface.
Feb 10 13:50:33.880 INF Dependency injection into *quesma_api.Pipeline :Injecting dependencies intopipeline(0xc000351b60)
Feb 10 13:50:33.880 INF Dependency injection into *quesma_api.Pipeline :OK - Injected Dependencies
Feb 10 13:50:33.880 INF Dependency injection into *frontend_connectors.BasicHTTPFrontendConnector :Injecting dependencies intoBasicHTTPFrontendConnector
Feb 10 13:50:33.880 INF Dependency injection into *frontend_connectors.BasicHTTPFrontendConnector :OK - Injected Dependencies
Feb 10 13:50:33.880 INF Dependency injection into *quesma_api.PathRouter :SKIP - No dependencies to inject. It doesn't implement DependenciesSetter interface.
Feb 10 13:50:33.880 INF Dependency injection into *frontend_connectors.Dispatcher :OK - Injected Dependencies
Feb 10 13:50:33.880 INF Dependency injection into *quesma_api.NoopBackendConnector :SKIP - No dependencies to inject. It doesn't implement DependenciesSetter interface.
Feb 10 13:50:33.880 DBG Component tree:
*quesma_api.Quesma(0xc00371a180)
pipeline(0xc000351b00)
BasicHTTPFrontendConnector
*quesma_api.PathRouter(0xc0064f5f60)
*frontend_connectors.Dispatcher(0xc00021efa0)
noop
pipeline(0xc000351b60)
BasicHTTPFrontendConnector
*quesma_api.PathRouter(0xc0064f5f60)
*frontend_connectors.Dispatcher(0xc00021efa0)
noop

Feb 10 13:50:33.880 INF BasicHTTPFrontendConnector::8888, index: 0, pipeline:0
Feb 10 13:50:33.881 INF BasicHTTPFrontendConnector::8888, index: 1, pipeline:0
Feb 10 13:50:33.881 DBG Endpoints:
:8888:
pipeline 0, connector 0
pipeline 0, connector 1

Feb 10 13:50:33.881 INF Sharing frontend connector 0xc003472370 with 0xc003472420
Feb 10 13:50:33.881 INF Sharing frontend connector 0xc003472370 with 0xc003472370
Feb 10 13:50:33.881 INF Dependency injection into *quesma_api.Quesma :SKIP - No dependencies to inject. It doesn't implement DependenciesSetter interface.
Feb 10 13:50:33.881 INF Dependency injection into *quesma_api.Pipeline :Injecting dependencies intopipeline(0xc000351bc0)
Feb 10 13:50:33.881 INF Dependency injection into *quesma_api.Pipeline :OK - Injected Dependencies
Feb 10 13:50:33.881 INF Dependency injection into *frontend_connectors.BasicHTTPFrontendConnector :Injecting dependencies intoBasicHTTPFrontendConnector
Feb 10 13:50:33.881 INF Dependency injection into *frontend_connectors.BasicHTTPFrontendConnector :OK - Injected Dependencies
Feb 10 13:50:33.881 INF Dependency injection into *quesma_api.PathRouter :SKIP - No dependencies to inject. It doesn't implement DependenciesSetter interface.
Feb 10 13:50:33.882 INF Dependency injection into *frontend_connectors.Dispatcher :OK - Injected Dependencies
Feb 10 13:50:33.882 INF Dependency injection into *frontend_connectors.BasicHTTPFrontendConnector :Injecting dependencies intoBasicHTTPFrontendConnector
Feb 10 13:50:33.882 INF Dependency injection into *frontend_connectors.BasicHTTPFrontendConnector :OK - Injected Dependencies
Feb 10 13:50:33.882 INF Dependency injection into *quesma_api.PathRouter :SKIP - No dependencies to inject. It doesn't implement DependenciesSetter interface.
Feb 10 13:50:33.882 INF Dependency injection into *frontend_connectors.Dispatcher :OK - Injected Dependencies
Feb 10 13:50:33.882 INF Dependency injection into *quesma_api.NoopBackendConnector :SKIP - No dependencies to inject. It doesn't implement DependenciesSetter interface.
Feb 10 13:50:33.882 DBG Component tree:
*quesma_api.Quesma(0xc00371b3e0)
pipeline(0xc000351bc0)
BasicHTTPFrontendConnector
*quesma_api.PathRouter(0xc0001ac280)
*frontend_connectors.Dispatcher(0xc00021f810)
BasicHTTPFrontendConnector
*quesma_api.PathRouter(0xc0001ac280)
*frontend_connectors.Dispatcher(0xc00021f810)
noop

--- PASS: Test_QuesmaBuild (0.01s)
=== RUN   Test_complex_scenario1
Feb 10 13:50:33.882 INF BasicHTTPFrontendConnector::8888, index: 0, pipeline:0
Feb 10 13:50:33.883 DBG Endpoints:
:8888:
pipeline 0, connector 0

Feb 10 13:50:33.883 INF Dependency injection into *quesma_api.Quesma :SKIP - No dependencies to inject. It doesn't implement DependenciesSetter interface.
Feb 10 13:50:33.883 INF Dependency injection into *quesma_api.Pipeline :Injecting dependencies intopipeline(0xc000351c20)
Feb 10 13:50:33.883 INF Dependency injection into *quesma_api.Pipeline :OK - Injected Dependencies
Feb 10 13:50:33.883 INF Dependency injection into *frontend_connectors.BasicHTTPFrontendConnector :Injecting dependencies intoBasicHTTPFrontendConnector
Feb 10 13:50:33.883 INF Dependency injection into *frontend_connectors.BasicHTTPFrontendConnector :OK - Injected Dependencies
Feb 10 13:50:33.883 INF Dependency injection into *quesma_api.PathRouter :SKIP - No dependencies to inject. It doesn't implement DependenciesSetter interface.
Feb 10 13:50:33.883 INF Dependency injection into *frontend_connectors.Dispatcher :OK - Injected Dependencies
Feb 10 13:50:33.883 INF Dependency injection into *quesma.QueryComplexProcessor :SKIP - No dependencies to inject. It doesn't implement DependenciesSetter interface.
Feb 10 13:50:33.884 INF Dependency injection into *quesma_api.NoopBackendConnector :SKIP - No dependencies to inject. It doesn't implement DependenciesSetter interface.
Feb 10 13:50:33.884 DBG Component tree:
*quesma_api.Quesma(0xc0024f82d0)
pipeline(0xc000351c20)
BasicHTTPFrontendConnector
*quesma_api.PathRouter(0xc0001ac4e0)
*frontend_connectors.Dispatcher(0xc000040730)
QueryProcessor
noop

Feb 10 13:50:33.884 INF Starting pipeline &{ [0xc0034724d0] [0xc0024f8390] map[0:0x3315080] 0xc0002bc700}
Feb 10 13:50:33.884 INF Starting frontend connector :8888 component=pipeline(0xc000351c20)
Feb 10 13:50:33.884 INF HTTP server started on :8888 component=BasicHTTPFrontendConnector
Feb 10 13:50:34.885 DBG BaseProcessor: Handle
Feb 10 13:50:34.885 DBG SimpleQueryTransformationPipeline: ParseQuery
Feb 10 13:50:34.885 DBG SimpleQueryTransformationPipeline: Transform
Feb 10 13:50:34.885 DBG BaseProcessor: executeQuery:SELECT count(*) FROM (SELECT 1 FROM __quesma_table_name LIMIT 10000)
Feb 10 13:50:34.886 DBG BaseProcessor: executeQuery:SELECT * FROM __quesma_table_name LIMIT 10
Feb 10 13:50:34.886 DBG SimpleQueryTransformationPipeline: TransformResults
Feb 10 13:50:34.886 DBG SimpleQueryTransformationPipeline: ComposeResults
qqq->
--- PASS: Test_complex_scenario1 (1.01s)
FAIL
Feb 10 13:50:34.887 ERR HTTP server stopped error="http: Server closed" component=BasicHTTPFrontendConnector
FAIL	github.com/QuesmaOrg/quesma/quesma/quesma	5.746s
=== RUN   TestAsyncQueriesEvictorTimePassed
--- PASS: TestAsyncQueriesEvictorTimePassed (0.00s)
=== RUN   TestAsyncQueriesEvictorStillAlive
--- PASS: TestAsyncQueriesEvictorStillAlive (0.00s)
PASS
ok  	github.com/QuesmaOrg/quesma/quesma/quesma/async_search_storage	1.015s
=== RUN   TestQuesmaConfigurationLoading
Using config file: [./test_configs/test_config_v2.yaml]
=== RUN   TestQuesmaConfigurationLoading/logs-generic-default
=== RUN   TestQuesmaConfigurationLoading/device-logs
=== RUN   TestQuesmaConfigurationLoading/example-elastic-index
--- PASS: TestQuesmaConfigurationLoading (0.01s)
--- PASS: TestQuesmaConfigurationLoading/logs-generic-default (0.00s)
--- PASS: TestQuesmaConfigurationLoading/device-logs (0.00s)
--- PASS: TestQuesmaConfigurationLoading/example-elastic-index (0.00s)
=== RUN   TestQuesmaTransparentProxyConfiguration
Using config file: [./test_configs/quesma_as_transparent_proxy.yml]
--- PASS: TestQuesmaTransparentProxyConfiguration (0.00s)
=== RUN   TestQuesmaTransparentProxyWithoutNoopConfiguration
config_v2_test.go:87: not working yet
--- SKIP: TestQuesmaTransparentProxyWithoutNoopConfiguration (0.00s)
=== RUN   TestQuesmaAddingHydrolixTablesToExistingElasticsearch
Using config file: [./test_configs/quesma_adding_two_hydrolix_tables.yaml]
--- PASS: TestQuesmaAddingHydrolixTablesToExistingElasticsearch (0.00s)
=== RUN   TestIngestWithSingleConnector
Using config file: [./test_configs/ingest_with_single_connector.yaml]
--- PASS: TestIngestWithSingleConnector (0.00s)
=== RUN   TestQuesmaHydrolixQueryOnly
Using config file: [./test_configs/quesma_hydrolix_tables_query_only.yaml]
--- PASS: TestQuesmaHydrolixQueryOnly (0.00s)
=== RUN   TestHasCommonTable
Using config file: [./test_configs/has_common_table.yaml]
--- PASS: TestHasCommonTable (0.00s)
=== RUN   TestInvalidDualTarget
Using config file: [./test_configs/invalid_dual_target.yaml]
--- PASS: TestInvalidDualTarget (0.00s)
=== RUN   TestMatchName
=== RUN   TestMatchName/logs-generic-default->logs-generic-default*[true]
=== RUN   TestMatchName/logs-generic-default->logs-generic-*[true]
=== RUN   TestMatchName/logs-generic-default-foo->logs-generic-*[true]
=== RUN   TestMatchName/logs-generic-->logs-generic-*[true]
=== RUN   TestMatchName/logs-generic->logs-generic-*[false]
=== RUN   TestMatchName/logs2-generic->logs-generic-*[false]
=== RUN   TestMatchName/logs-generic-default->logs-*-default[true]
=== RUN   TestMatchName/logs-specific->logs-generic-*[false]
=== RUN   TestMatchName/logs-generic-123->logs-generic-*[true]
=== RUN   TestMatchName/logs-generic-default-foo-bar->logs-generic-*[true]
=== RUN   TestMatchName/logs-generic-abc->logs-generic-*[true]
=== RUN   TestMatchName/logs-custom-default->logs-*-default[true]
=== RUN   TestMatchName/logs-custom-default->logs-generic-*[false]
=== RUN   TestMatchName/logs-custom-specific->logs-custom-*[true]
=== RUN   TestMatchName/logs-custom-specific-123->logs-custom-*[true]
=== RUN   TestMatchName/logs-custom-abc->logs-custom-*[true]
--- PASS: TestMatchName (0.00s)
--- PASS: TestMatchName/logs-generic-default->logs-generic-default*[true] (0.00s)
--- PASS: TestMatchName/logs-generic-default->logs-generic-*[true] (0.00s)
--- PASS: TestMatchName/logs-generic-default-foo->logs-generic-*[true] (0.00s)
--- PASS: TestMatchName/logs-generic-->logs-generic-*[true] (0.00s)
--- PASS: TestMatchName/logs-generic->logs-generic-*[false] (0.00s)
--- PASS: TestMatchName/logs2-generic->logs-generic-*[false] (0.00s)
--- PASS: TestMatchName/logs-generic-default->logs-*-default[true] (0.00s)
--- PASS: TestMatchName/logs-specific->logs-generic-*[false] (0.00s)
--- PASS: TestMatchName/logs-generic-123->logs-generic-*[true] (0.00s)
--- PASS: TestMatchName/logs-generic-default-foo-bar->logs-generic-*[true] (0.00s)
--- PASS: TestMatchName/logs-generic-abc->logs-generic-*[true] (0.00s)
--- PASS: TestMatchName/logs-custom-default->logs-*-default[true] (0.00s)
--- PASS: TestMatchName/logs-custom-default->logs-generic-*[false] (0.00s)
--- PASS: TestMatchName/logs-custom-specific->logs-custom-*[true] (0.00s)
--- PASS: TestMatchName/logs-custom-specific-123->logs-custom-*[true] (0.00s)
--- PASS: TestMatchName/logs-custom-abc->logs-custom-*[true] (0.00s)
=== RUN   TestTargetNewVariant
Using config file: [./test_configs/target_new_variant.yaml]
--- PASS: TestTargetNewVariant (0.00s)
=== RUN   TestTargetLegacyVariant
Using config file: [./test_configs/target_legacy_variant.yaml]
--- PASS: TestTargetLegacyVariant (0.00s)
=== RUN   TestUseCommonTableGlobalProperty
Using config file: [./test_configs/use_common_table_global_property.yaml]
--- PASS: TestUseCommonTableGlobalProperty (0.00s)
=== RUN   TestIngestOptimizers
Using config file: [./test_configs/ingest_only_optimizers.yaml]
--- PASS: TestIngestOptimizers (0.00s)
=== RUN   TestEnv2Json_arrays
--- PASS: TestEnv2Json_arrays (0.00s)
=== RUN   TestEnv2Json_arraysByName
Using config file: [./test_configs/test_config_v2.yaml]
--- PASS: TestEnv2Json_arraysByName (0.01s)
=== RUN   TestEnv2Json_empty
--- PASS: TestEnv2Json_empty (0.00s)
=== RUN   TestEnv2Json_jsonMerge
--- PASS: TestEnv2Json_jsonMerge (0.00s)
PASS
ok  	github.com/QuesmaOrg/quesma/quesma/quesma/config	1.072s
=== RUN   Test_unmarshalElasticResponse
=== RUN   Test_unmarshalElasticResponse/bulk_response_with_no_errors_(1)
=== RUN   Test_unmarshalElasticResponse/bulk_response_with_no_errors_(2)
=== RUN   Test_unmarshalElasticResponse/bulk_response_with_some_error
--- PASS: Test_unmarshalElasticResponse (0.00s)
--- PASS: Test_unmarshalElasticResponse/bulk_response_with_no_errors_(1) (0.00s)
--- PASS: Test_unmarshalElasticResponse/bulk_response_with_no_errors_(2) (0.00s)
--- PASS: Test_unmarshalElasticResponse/bulk_response_with_some_error (0.00s)
=== RUN   Test_BulkForEach
--- PASS: Test_BulkForEach (0.00s)
=== RUN   Test_BulkForEachDeleteOnly
--- PASS: Test_BulkForEachDeleteOnly (0.00s)
=== RUN   TestSplitBulkSampleData
--- PASS: TestSplitBulkSampleData (0.00s)
=== RUN   TestSplitBulkDelete
--- PASS: TestSplitBulkDelete (0.00s)
=== RUN   TestSplitBulkUpdatePayload
--- PASS: TestSplitBulkUpdatePayload (0.00s)
=== RUN   TestSplitBulkMixedPayload
--- PASS: TestSplitBulkMixedPayload (0.00s)
PASS
ok  	github.com/QuesmaOrg/quesma/quesma/quesma/functionality/bulk	1.018s
=== RUN   TestFieldCaps
--- PASS: TestFieldCaps (0.00s)
=== RUN   TestFieldCapsWithAliases
--- PASS: TestFieldCapsWithAliases (0.00s)
=== RUN   TestFieldCapsMultipleIndexes
--- PASS: TestFieldCapsMultipleIndexes (0.00s)
=== RUN   TestFieldCapsMultipleIndexesConflictingEntries
--- PASS: TestFieldCapsMultipleIndexesConflictingEntries (0.00s)
=== RUN   Test_merge
=== RUN   Test_merge/different_types
=== RUN   Test_merge/same_types,_different_indices
--- PASS: Test_merge (0.00s)
--- PASS: Test_merge/different_types (0.00s)
--- PASS: Test_merge/same_types,_different_indices (0.00s)
PASS
ok  	github.com/QuesmaOrg/quesma/quesma/quesma/functionality/field_capabilities	1.017s
=== RUN   Test_combineSourcesFromElasticWithRegistry
=== RUN   Test_combineSourcesFromElasticWithRegistry/index_not_enabled_in_config,_some_unrelated_index_in_Elastic
=== RUN   Test_combineSourcesFromElasticWithRegistry/index_enabled_in_config,_not_present_in_the_data_source;_decoy_index_in_Elastic
=== RUN   Test_combineSourcesFromElasticWithRegistry/index_enabled_in_config,_present_in_the_data_source
--- PASS: Test_combineSourcesFromElasticWithRegistry (0.00s)
--- PASS: Test_combineSourcesFromElasticWithRegistry/index_not_enabled_in_config,_some_unrelated_index_in_Elastic (0.00s)
--- PASS: Test_combineSourcesFromElasticWithRegistry/index_enabled_in_config,_not_present_in_the_data_source;_decoy_index_in_Elastic (0.00s)
--- PASS: Test_combineSourcesFromElasticWithRegistry/index_enabled_in_config,_present_in_the_data_source (0.00s)
PASS
ok  	github.com/QuesmaOrg/quesma/quesma/quesma/functionality/resolve	1.011s
=== RUN   TestHandleTermsEnumRequest
--- PASS: TestHandleTermsEnumRequest (0.00s)
=== RUN   TestIfHandleTermsEnumUsesSchema
--- PASS: TestIfHandleTermsEnumUsesSchema (0.00s)
PASS
ok  	github.com/QuesmaOrg/quesma/quesma/quesma/functionality/terms_enum	1.023s
=== RUN   TestLogPanic
Feb 10 13:50:32.000 ERR Panic recovered: test
goroutine 7 [running]:
runtime/debug.Stack()
/opt/hostedtoolcache/go/1.23.6/x64/src/runtime/debug/stack.go:26 +0x67
github.com/QuesmaOrg/quesma/quesma/quesma/recovery.commonRecovery({0x826140, 0x94cb80}, 0x8a9c98)
/home/runner/work/quesma/quesma/quesma/quesma/recovery/recovery_strategies.go:32 +0x1ab
github.com/QuesmaOrg/quesma/quesma/quesma/recovery.LogPanic()
/home/runner/work/quesma/quesma/quesma/quesma/recovery/recovery_strategies.go:39 +0x36
panic({0x826140?, 0x94cb80?})
/opt/hostedtoolcache/go/1.23.6/x64/src/runtime/panic.go:785 +0x132
github.com/QuesmaOrg/quesma/quesma/quesma/recovery.TestLogPanic.func1()
/home/runner/work/quesma/quesma/quesma/quesma/recovery/recovery_strategies_test.go:14 +0x45
github.com/QuesmaOrg/quesma/quesma/quesma/recovery.TestLogPanic(0xc00012e820)
/home/runner/work/quesma/quesma/quesma/quesma/recovery/recovery_strategies_test.go:18 +0x46
testing.tRunner(0xc00012e820, 0x8a9b88)
/opt/hostedtoolcache/go/1.23.6/x64/src/testing/testing.go:1690 +0x227
created by testing.(*T).Run in goroutine 1
/opt/hostedtoolcache/go/1.23.6/x64/src/testing/testing.go:1743 +0x826

--- PASS: TestLogPanic (0.00s)
PASS
ok  	github.com/QuesmaOrg/quesma/quesma/quesma/recovery	1.018s
=== RUN   TestCommentedJson
--- PASS: TestCommentedJson (0.00s)
=== RUN   TestJSONClone
--- PASS: TestJSONClone (0.00s)
=== RUN   TestParseNDJSON
--- PASS: TestParseNDJSON (0.00s)
=== RUN   TestParseRequestBody
--- PASS: TestParseRequestBody (0.00s)
=== RUN   TestParseRequestBody2
--- PASS: TestParseRequestBody2 (0.00s)
PASS
ok  	github.com/QuesmaOrg/quesma/quesma/quesma/types	1.012s
=== RUN   TestHtmlPages
Feb 10 13:50:34.000 DBG Received debug info from secondary source: b1c4a89e-4905-5e3c-b57f-dc92627d011e
Feb 10 13:50:34.000 DBG Received debug info from primary source: b1c4a89e-4905-5e3c-b57f-dc92627d011e
=== RUN   TestHtmlPages/queries_got_our_id
=== RUN   TestHtmlPages/queries_got_no_XSS
=== RUN   TestHtmlPages/reason_got_no_XSS
=== RUN   TestHtmlPages/logs_got_no_XSS
=== RUN   TestHtmlPages/statistics_got_no_XSS
=== RUN   TestHtmlPages/schema_got_no_XSS_and_no_panic
--- PASS: TestHtmlPages (0.00s)
--- PASS: TestHtmlPages/queries_got_our_id (0.00s)
--- PASS: TestHtmlPages/queries_got_no_XSS (0.00s)
--- PASS: TestHtmlPages/reason_got_no_XSS (0.00s)
--- PASS: TestHtmlPages/logs_got_no_XSS (0.00s)
--- PASS: TestHtmlPages/statistics_got_no_XSS (0.00s)
--- PASS: TestHtmlPages/schema_got_no_XSS_and_no_panic (0.00s)
=== RUN   TestHtmlSchemaPage
=== RUN   TestHtmlSchemaPage/schema_got_no_XSS_and_no_panic
--- PASS: TestHtmlSchemaPage (0.00s)
--- PASS: TestHtmlSchemaPage/schema_got_no_XSS_and_no_panic (0.00s)
PASS
ok  	github.com/QuesmaOrg/quesma/quesma/quesma/ui	1.019s
=== RUN   TestHtmlBuffer_Html
=== RUN   TestHtmlBuffer_Html/Html_without_any_escaping
=== RUN   TestHtmlBuffer_Html/Text_with_XSS_escaping
--- PASS: TestHtmlBuffer_Html (0.00s)
--- PASS: TestHtmlBuffer_Html/Html_without_any_escaping (0.00s)
--- PASS: TestHtmlBuffer_Html/Text_with_XSS_escaping (0.00s)
PASS
ok  	github.com/QuesmaOrg/quesma/quesma/quesma/ui/internal/builder	1.012s
=== RUN   Test_SchemaToHierarchicalSchema
--- PASS: Test_SchemaToHierarchicalSchema (0.00s)
=== RUN   TestSchema_ResolveField
=== RUN   TestSchema_ResolveField/empty_schema
=== RUN   TestSchema_ResolveField/should_resolve_field
=== RUN   TestSchema_ResolveField/should_not_resolve_field
=== RUN   TestSchema_ResolveField/should_resolve_aliased_field
=== RUN   TestSchema_ResolveField/should_not_resolve_aliased_field
--- PASS: TestSchema_ResolveField (0.00s)
--- PASS: TestSchema_ResolveField/empty_schema (0.00s)
--- PASS: TestSchema_ResolveField/should_resolve_field (0.00s)
--- PASS: TestSchema_ResolveField/should_not_resolve_field (0.00s)
--- PASS: TestSchema_ResolveField/should_resolve_aliased_field (0.00s)
--- PASS: TestSchema_ResolveField/should_not_resolve_aliased_field (0.00s)
=== RUN   TestSchema_ResolveFieldByInternalName
=== RUN   TestSchema_ResolveFieldByInternalName/empty_schema
=== RUN   TestSchema_ResolveFieldByInternalName/schema_with_fields_with_internal_separators,_lookup_by_property_name
=== RUN   TestSchema_ResolveFieldByInternalName/schema_with_fields_with_internal_separators,_lookup_by_internal_name
--- PASS: TestSchema_ResolveFieldByInternalName (0.00s)
--- PASS: TestSchema_ResolveFieldByInternalName/empty_schema (0.00s)
--- PASS: TestSchema_ResolveFieldByInternalName/schema_with_fields_with_internal_separators,_lookup_by_property_name (0.00s)
--- PASS: TestSchema_ResolveFieldByInternalName/schema_with_fields_with_internal_separators,_lookup_by_internal_name (0.00s)
=== RUN   Test_schemaRegistry_FindSchema
=== RUN   Test_schemaRegistry_FindSchema/schema_not_found
=== RUN   Test_schemaRegistry_FindSchema/schema_inferred,_no_mappings
Feb 10 13:50:35.000 DBG loading schema for table some_table
=== RUN   Test_schemaRegistry_FindSchema/schema_inferred,_with_type_mappings_(deprecated)
Feb 10 13:50:35.000 DBG loading schema for table some_table
=== RUN   Test_schemaRegistry_FindSchema/schema_inferred,_with_type_mappings_not_backed_by_db_(deprecated)
Feb 10 13:50:35.000 DBG loading schema for table some_table
=== RUN   Test_schemaRegistry_FindSchema/schema_inferred,_with_type_mappings_not_backed_by_db
Feb 10 13:50:35.000 DBG loading schema for table some_table
=== RUN   Test_schemaRegistry_FindSchema/schema_explicitly_configured,_nothing_in_db
=== RUN   Test_schemaRegistry_FindSchema/schema_inferred,_with_mapping_overrides
Feb 10 13:50:35.000 DBG loading schema for table some_table
=== RUN   Test_schemaRegistry_FindSchema/schema_inferred,_with_aliases
Feb 10 13:50:35.000 DBG loading schema for table some_table
=== RUN   Test_schemaRegistry_FindSchema/schema_inferred,_with_aliases_[deprecated_config]
Feb 10 13:50:35.000 DBG loading schema for table some_table
=== RUN   Test_schemaRegistry_FindSchema/schema_inferred,_requesting_nonexistent_schema
Feb 10 13:50:35.000 DBG loading schema for table some_table
--- PASS: Test_schemaRegistry_FindSchema (0.00s)
--- PASS: Test_schemaRegistry_FindSchema/schema_not_found (0.00s)
--- PASS: Test_schemaRegistry_FindSchema/schema_inferred,_no_mappings (0.00s)
--- PASS: Test_schemaRegistry_FindSchema/schema_inferred,_with_type_mappings_(deprecated) (0.00s)
--- PASS: Test_schemaRegistry_FindSchema/schema_inferred,_with_type_mappings_not_backed_by_db_(deprecated) (0.00s)
--- PASS: Test_schemaRegistry_FindSchema/schema_inferred,_with_type_mappings_not_backed_by_db (0.00s)
--- PASS: Test_schemaRegistry_FindSchema/schema_explicitly_configured,_nothing_in_db (0.00s)
--- PASS: Test_schemaRegistry_FindSchema/schema_inferred,_with_mapping_overrides (0.00s)
--- PASS: Test_schemaRegistry_FindSchema/schema_inferred,_with_aliases (0.00s)
--- PASS: Test_schemaRegistry_FindSchema/schema_inferred,_with_aliases_[deprecated_config] (0.00s)
--- PASS: Test_schemaRegistry_FindSchema/schema_inferred,_requesting_nonexistent_schema (0.00s)
=== RUN   Test_schemaRegistry_UpdateDynamicConfiguration
Feb 10 13:50:35.000 DBG loading schema for table some_table
Feb 10 13:50:35.000 DBG loading schema for table some_table
--- PASS: Test_schemaRegistry_UpdateDynamicConfiguration (0.00s)
PASS
ok  	github.com/QuesmaOrg/quesma/quesma/schema	1.018s
=== RUN   TestStatistics_process
--- PASS: TestStatistics_process (0.00s)
=== RUN   TestRequestsStatistics_typical
--- PASS: TestRequestsStatistics_typical (0.00s)
=== RUN   TestRequestsStatistics_empty
=== RUN   TestRequestsStatistics_empty/empty_store
--- PASS: TestRequestsStatistics_empty (0.00s)
--- PASS: TestRequestsStatistics_empty/empty_store (0.00s)
PASS
ok  	github.com/QuesmaOrg/quesma/quesma/stats	1.015s
=== RUN   TestErrorStatisticsStore_ReturnTop5Errors
=== RUN   TestErrorStatisticsStore_ReturnTop5Errors/empty
=== RUN   TestErrorStatisticsStore_ReturnTop5Errors/one
=== RUN   TestErrorStatisticsStore_ReturnTop5Errors/two
=== RUN   TestErrorStatisticsStore_ReturnTop5Errors/many
=== RUN   TestErrorStatisticsStore_ReturnTop5Errors/cleanup
--- PASS: TestErrorStatisticsStore_ReturnTop5Errors (0.01s)
--- PASS: TestErrorStatisticsStore_ReturnTop5Errors/empty (0.00s)
--- PASS: TestErrorStatisticsStore_ReturnTop5Errors/one (0.00s)
--- PASS: TestErrorStatisticsStore_ReturnTop5Errors/two (0.00s)
--- PASS: TestErrorStatisticsStore_ReturnTop5Errors/many (0.00s)
--- PASS: TestErrorStatisticsStore_ReturnTop5Errors/cleanup (0.01s)
PASS
ok  	github.com/QuesmaOrg/quesma/quesma/stats/errorstats	1.022s
?   	github.com/QuesmaOrg/quesma/quesma/telemetry/headers	[no test files]
=== RUN   TestTableResolver
=== RUN   TestTableResolver/elastic_fallback
Feb 10 13:50:36.000 INF Index registry updating state.
Feb 10 13:50:36.000 INF Clickhouse tables updated: map[]
Feb 10 13:50:36.000 INF Elastic tables updated: map[]
Feb 10 13:50:36.000 DBG Decision for pipeline 'Ingest', pattern 'some-index':  Pass to Elasticsearch. Using default wildcard ('*') configuration for Ingest processor (defaultWildcard).
=== RUN   TestTableResolver/all
Feb 10 13:50:36.000 INF Index registry updating state.
Feb 10 13:50:36.000 INF Clickhouse tables updated: map[]
Feb 10 13:50:36.000 INF Elastic tables updated: map[]
Feb 10 13:50:36.000 DBG Decision for pipeline 'Query', pattern '*':  Returns error: 'inconsistent A/B testing configuration - index  (A/B testing: true) and index  (A/B testing: false)'. One of the indexes matching the pattern does A/B testing, while another index does not - inconsistency. ().
=== RUN   TestTableResolver/empty_*
Feb 10 13:50:36.000 INF Index registry updating state.
Feb 10 13:50:36.000 INF Clickhouse tables updated: map[index1:{index1 false} index2:{index2 false}]
Feb 10 13:50:36.000 INF Elastic tables updated: map[]
Feb 10 13:50:36.000 DBG Decision for pipeline 'Query', pattern '*':  Returns error: 'inconsistent A/B testing configuration - index  (A/B testing: true) and index  (A/B testing: false)'. One of the indexes matching the pattern does A/B testing, while another index does not - inconsistency. ().
=== RUN   TestTableResolver/query_all,_indices_in_both_connectors
Feb 10 13:50:36.000 INF Index registry updating state.
Feb 10 13:50:36.000 INF Clickhouse tables updated: map[index1:{index1 false} index2:{index2 false}]
Feb 10 13:50:36.000 INF Elastic tables updated: map[index3:{index3 false}]
Feb 10 13:50:36.000 DBG Decision for pipeline 'Query', pattern '*':  Returns error: 'inconsistent A/B testing configuration - index  (A/B testing: true) and index  (A/B testing: false)'. One of the indexes matching the pattern does A/B testing, while another index does not - inconsistency. ().
=== RUN   TestTableResolver/ingest_with_a_pattern
Feb 10 13:50:36.000 INF Index registry updating state.
Feb 10 13:50:36.000 INF Clickhouse tables updated: map[index1:{index1 false} index2:{index2 false}]
Feb 10 13:50:36.000 INF Elastic tables updated: map[index3:{index3 false}]
Feb 10 13:50:36.000 DBG Decision for pipeline 'Ingest', pattern '*':  Returns error: 'pattern is not allowed'. Pattern is not allowed. (singleIndexSplitter).
=== RUN   TestTableResolver/query_closed_index
Feb 10 13:50:36.000 INF Index registry updating state.
Feb 10 13:50:36.000 INF Clickhouse tables updated: map[closed:{closed false}]
Feb 10 13:50:36.000 INF Elastic tables updated: map[]
Feb 10 13:50:36.000 DBG Decision for pipeline 'Query', pattern 'closed':  Returns a closed index message. Index is disabled in config. (disabled).
=== RUN   TestTableResolver/ingest_closed_index
Feb 10 13:50:36.000 INF Index registry updating state.
Feb 10 13:50:36.000 INF Clickhouse tables updated: map[closed:{closed false}]
Feb 10 13:50:36.000 INF Elastic tables updated: map[]
Feb 10 13:50:36.000 DBG Decision for pipeline 'Query', pattern 'closed':  Returns a closed index message. Index is disabled in config. (disabled).
=== RUN   TestTableResolver/ingest_closed_index#01
Feb 10 13:50:36.000 INF Index registry updating state.
Feb 10 13:50:36.000 INF Clickhouse tables updated: map[closed:{closed false}]
Feb 10 13:50:36.000 INF Elastic tables updated: map[]
Feb 10 13:50:36.000 DBG Decision for pipeline 'Query', pattern 'closed-common-table':  Returns a closed index message. Index is disabled in config. (disabled).
=== RUN   TestTableResolver/ingest_closed_index#02
Feb 10 13:50:36.000 INF Index registry updating state.
Feb 10 13:50:36.000 INF Clickhouse tables updated: map[closed:{closed false}]
Feb 10 13:50:36.000 INF Elastic tables updated: map[]
Feb 10 13:50:36.000 DBG Decision for pipeline 'Query', pattern 'unknown-target':  Returns error: 'Q2001: Not supported search condition. : unsupported target: unknown'. Unsupported configuration (singleIndex).
=== RUN   TestTableResolver/ingest_to_index1
Feb 10 13:50:36.000 INF Index registry updating state.
Feb 10 13:50:36.000 INF Clickhouse tables updated: map[index1:{index1 false}]
Feb 10 13:50:36.000 INF Elastic tables updated: map[]
Feb 10 13:50:36.000 DBG Decision for pipeline 'Ingest', pattern 'index1':  Pass to clickhouse. Table: 'index1' . Indexes: [index1]. Enabled in the config.  (singleIndex).
=== RUN   TestTableResolver/query_from_index1
Feb 10 13:50:36.000 INF Index registry updating state.
Feb 10 13:50:36.000 INF Clickhouse tables updated: map[index1:{index1 false}]
Feb 10 13:50:36.000 INF Elastic tables updated: map[]
Feb 10 13:50:36.000 DBG Decision for pipeline 'Query', pattern 'index1':  Pass to clickhouse. Table: 'index1' . Indexes: [index1]. Enabled in the config.  (singleIndex).
=== RUN   TestTableResolver/ingest_to_index2
Feb 10 13:50:36.000 INF Index registry updating state.
Feb 10 13:50:36.000 INF Clickhouse tables updated: map[index2:{index2 false}]
Feb 10 13:50:36.000 INF Elastic tables updated: map[]
Feb 10 13:50:36.000 DBG Decision for pipeline 'Ingest', pattern 'index2':  Pass to clickhouse. Table: 'quesma_common_table' . Common table. Indexes: [index2]. Common table will be used. (commonTable).
=== RUN   TestTableResolver/query_from_index2
Feb 10 13:50:36.000 INF Index registry updating state.
Feb 10 13:50:36.000 INF Clickhouse tables updated: map[index2:{index2 false}]
Feb 10 13:50:36.000 INF Elastic tables updated: map[]
Feb 10 13:50:36.000 DBG Decision for pipeline 'Query', pattern 'index2':  Pass to clickhouse. Table: 'quesma_common_table' . Common table. Indexes: [index2]. Common table will be used. (commonTable).
=== RUN   TestTableResolver/query_from_index1,index2
Feb 10 13:50:36.000 INF Index registry updating state.
Feb 10 13:50:36.000 INF Clickhouse tables updated: map[]
Feb 10 13:50:36.000 INF Elastic tables updated: map[index3:{index3 false}]
Feb 10 13:50:36.000 DBG Decision for pipeline 'Query', pattern 'index1,index2':  Returns error: 'incompatible decisions for two indexes (different ClickHouse table) - &{quesma_common_table [index2] %!s(bool=true)} and &{index1 [index1] %!s(bool=false)}'. Incompatible decisions for two indexes - they use a different ClickHouse table ().
=== RUN   TestTableResolver/query_from_index1,index-not-existing
Feb 10 13:50:36.000 INF Index registry updating state.
Feb 10 13:50:36.000 INF Clickhouse tables updated: map[]
Feb 10 13:50:36.000 INF Elastic tables updated: map[index1,index-not-existing:{index1,index-not-existing false}]
Feb 10 13:50:36.000 DBG Decision for pipeline 'Query', pattern 'index1,index-not-existing':  Returns error: 'incompatible decisions for two indexes - they use different connectors: could not find connector &{index1 [index1] %!s(bool=false)} used for index  in decisions: [%!s(*quesma_api.ConnectorDecisionElastic=&{false})]'. Incompatible decisions for two indexes - they use different connectors ().
=== RUN   TestTableResolver/ingest_to_index3
Feb 10 13:50:36.000 INF Index registry updating state.
Feb 10 13:50:36.000 INF Clickhouse tables updated: map[]
Feb 10 13:50:36.000 INF Elastic tables updated: map[index3:{index3 false}]
Feb 10 13:50:36.000 DBG Decision for pipeline 'Ingest', pattern 'index3':  Pass to Elasticsearch. Enabled in the config.  (singleIndex).
=== RUN   TestTableResolver/query_from_index3
Feb 10 13:50:36.000 INF Index registry updating state.
Feb 10 13:50:36.000 INF Clickhouse tables updated: map[]
Feb 10 13:50:36.000 INF Elastic tables updated: map[index3:{index3 false}]
Feb 10 13:50:36.000 DBG Decision for pipeline 'Query', pattern 'index3':  Pass to Elasticsearch. Enabled in the config.  (singleIndex).
=== RUN   TestTableResolver/query_pattern
Feb 10 13:50:36.000 INF Index registry updating state.
Feb 10 13:50:36.000 INF Clickhouse tables updated: map[index2:{index2 true}]
Feb 10 13:50:36.000 INF Elastic tables updated: map[]
Feb 10 13:50:36.000 DBG Decision for pipeline 'Query', pattern 'index2,foo*':  Pass to clickhouse. Table: 'quesma_common_table' . Common table. Indexes: [index2]. Common table will be used. (commonTable).
=== RUN   TestTableResolver/query_pattern_(not_existing_virtual_table)
Feb 10 13:50:36.000 INF Index registry updating state.
Feb 10 13:50:36.000 INF Clickhouse tables updated: map[common-index1:{common-index1 true}]
Feb 10 13:50:36.000 INF Elastic tables updated: map[]
Feb 10 13:50:36.000 DBG Decision for pipeline 'Query', pattern 'common-index1,common-index2':  Pass to clickhouse. Table: 'quesma_common_table' . Common table. Indexes: [common-index1 common-index2]. Merged decisions ().
=== RUN   TestTableResolver/query_kibana_internals
Feb 10 13:50:36.000 INF Index registry updating state.
Feb 10 13:50:36.000 INF Clickhouse tables updated: map[]
Feb 10 13:50:36.000 INF Elastic tables updated: map[]
Feb 10 13:50:36.000 DBG Decision for pipeline 'Query', pattern '.kibana':  Pass to Elasticsearch. Management call. It's kibana internals (kibanaInternal).
=== RUN   TestTableResolver/ingest_kibana_internals
Feb 10 13:50:36.000 INF Index registry updating state.
Feb 10 13:50:36.000 INF Clickhouse tables updated: map[]
Feb 10 13:50:36.000 INF Elastic tables updated: map[]
Feb 10 13:50:36.000 DBG Decision for pipeline 'Ingest', pattern '.kibana':  Pass to Elasticsearch. Management call. It's kibana internals (kibanaInternal).
=== RUN   TestTableResolver/ingest_not_configured_index
Feb 10 13:50:36.000 INF Index registry updating state.
Feb 10 13:50:36.000 INF Clickhouse tables updated: map[]
Feb 10 13:50:36.000 INF Elastic tables updated: map[]
Feb 10 13:50:36.000 DBG Decision for pipeline 'Ingest', pattern 'not-configured':  Pass to Elasticsearch. Using default wildcard ('*') configuration for Ingest processor (defaultWildcard).
=== RUN   TestTableResolver/double_write
Feb 10 13:50:36.000 INF Index registry updating state.
Feb 10 13:50:36.000 INF Clickhouse tables updated: map[]
Feb 10 13:50:36.000 INF Elastic tables updated: map[]
Feb 10 13:50:36.000 DBG Decision for pipeline 'Ingest', pattern 'logs':  Pass to clickhouse. Table: 'logs' . Indexes: [logs]. Pass to Elasticsearch. Enabled in the config. Dual write is enabled. (singleIndex).
=== RUN   TestTableResolver/A/B_testing
Feb 10 13:50:36.000 INF Index registry updating state.
Feb 10 13:50:36.000 INF Clickhouse tables updated: map[]
Feb 10 13:50:36.000 INF Elastic tables updated: map[]
Feb 10 13:50:36.000 DBG Decision for pipeline 'Query', pattern 'logs':  Pass to clickhouse. Table: 'logs' . Indexes: [logs]. Pass to Elasticsearch. Enable AB testing. Enabled in the config. A/B testing. (singleIndex).
=== RUN   TestTableResolver/A/B_testing_(pattern)
Feb 10 13:50:36.000 INF Index registry updating state.
Feb 10 13:50:36.000 INF Clickhouse tables updated: map[]
Feb 10 13:50:36.000 INF Elastic tables updated: map[]
Feb 10 13:50:36.000 DBG Decision for pipeline 'Query', pattern 'logs*':  Pass to clickhouse. Table: 'logs' . Indexes: [logs]. Pass to Elasticsearch. Enable AB testing. Enabled in the config. A/B testing. (singleIndex).
=== RUN   TestTableResolver/query_both_connectors
Feb 10 13:50:36.000 INF Index registry updating state.
Feb 10 13:50:36.000 INF Clickhouse tables updated: map[index1:{index1 false}]
Feb 10 13:50:36.000 INF Elastic tables updated: map[logs:{logs false}]
Feb 10 13:50:36.000 DBG Decision for pipeline 'Query', pattern 'logs,index1':  Returns error: 'inconsistent A/B testing configuration - index  (A/B testing: true) and index  (A/B testing: false)'. One of the indexes matching the pattern does A/B testing, while another index does not - inconsistency. ().
=== RUN   TestTableResolver/query_elastic_with_pattern
Feb 10 13:50:36.000 INF Index registry updating state.
Feb 10 13:50:36.000 INF Clickhouse tables updated: map[]
Feb 10 13:50:36.000 INF Elastic tables updated: map[logs:{logs false}]
Feb 10 13:50:36.000 DBG Decision for pipeline 'Query', pattern 'some-elastic-logs*':  Pass to Elasticsearch. Enabled in the config.  (singleIndex).
=== RUN   TestTableResolver/non_matching_pattern
Feb 10 13:50:36.000 INF Index registry updating state.
Feb 10 13:50:36.000 INF Clickhouse tables updated: map[]
Feb 10 13:50:36.000 INF Elastic tables updated: map[logs:{logs false}]
Feb 10 13:50:36.000 DBG Decision for pipeline 'Query', pattern 'some-non-matching-pattern*':  Returns an empty result. No indexes matched, no decisions made. ().
=== RUN   TestTableResolver/query_internal_index
Feb 10 13:50:36.000 INF Index registry updating state.
Feb 10 13:50:36.000 INF Clickhouse tables updated: map[]
Feb 10 13:50:36.000 INF Elastic tables updated: map[]
Feb 10 13:50:36.000 DBG Decision for pipeline 'Query', pattern 'quesma_common_table':  Returns error: 'common table is not allowed to be queried directly'. It's internal table. Not allowed to be queried directly. (commonTable).
--- PASS: TestTableResolver (0.02s)
--- PASS: TestTableResolver/elastic_fallback (0.00s)
--- PASS: TestTableResolver/all (0.00s)
--- PASS: TestTableResolver/empty_* (0.00s)
--- PASS: TestTableResolver/query_all,_indices_in_both_connectors (0.00s)
--- PASS: TestTableResolver/ingest_with_a_pattern (0.00s)
--- PASS: TestTableResolver/query_closed_index (0.00s)
--- PASS: TestTableResolver/ingest_closed_index (0.00s)
--- PASS: TestTableResolver/ingest_closed_index#01 (0.00s)
--- PASS: TestTableResolver/ingest_closed_index#02 (0.00s)
--- PASS: TestTableResolver/ingest_to_index1 (0.00s)
--- PASS: TestTableResolver/query_from_index1 (0.00s)
--- PASS: TestTableResolver/ingest_to_index2 (0.00s)
--- PASS: TestTableResolver/query_from_index2 (0.00s)
--- PASS: TestTableResolver/query_from_index1,index2 (0.00s)
--- PASS: TestTableResolver/query_from_index1,index-not-existing (0.00s)
--- PASS: TestTableResolver/ingest_to_index3 (0.00s)
--- PASS: TestTableResolver/query_from_index3 (0.00s)
--- PASS: TestTableResolver/query_pattern (0.00s)
--- PASS: TestTableResolver/query_pattern_(not_existing_virtual_table) (0.00s)
--- PASS: TestTableResolver/query_kibana_internals (0.00s)
--- PASS: TestTableResolver/ingest_kibana_internals (0.00s)
--- PASS: TestTableResolver/ingest_not_configured_index (0.00s)
--- PASS: TestTableResolver/double_write (0.00s)
--- PASS: TestTableResolver/A/B_testing (0.00s)
--- PASS: TestTableResolver/A/B_testing_(pattern) (0.00s)
--- PASS: TestTableResolver/query_both_connectors (0.00s)
--- PASS: TestTableResolver/query_elastic_with_pattern (0.00s)
--- PASS: TestTableResolver/non_matching_pattern (0.00s)
--- PASS: TestTableResolver/query_internal_index (0.00s)
PASS
ok  	github.com/QuesmaOrg/quesma/quesma/table_resolver	1.031s
?   	github.com/QuesmaOrg/quesma/quesma/util/healthcheck	[no test files]
?   	github.com/QuesmaOrg/quesma/quesma/util/regex	[no test files]
=== RUN   TestDurationMeasurement_Aggregate
--- PASS: TestDurationMeasurement_Aggregate (0.00s)
=== RUN   TestDurationMeasurement_Percentiles
--- PASS: TestDurationMeasurement_Percentiles (0.05s)
=== RUN   TestDurationMeasurement_Percentiles_no_samples
--- PASS: TestDurationMeasurement_Percentiles_no_samples (0.00s)
=== RUN   TestDurationMeasurement_Percentiles_single_sample
--- PASS: TestDurationMeasurement_Percentiles_single_sample (0.00s)
=== RUN   TestMultiCounter_Add
--- PASS: TestMultiCounter_Add (0.00s)
=== RUN   TestPhoneHome_ParseElastic
--- PASS: TestPhoneHome_ParseElastic (0.00s)
=== RUN   TestAgent_CollectElastic_Version
--- PASS: TestAgent_CollectElastic_Version (0.00s)
=== RUN   TestGetTopNValues
=== RUN   TestGetTopNValues/LessThanN
=== RUN   TestGetTopNValues/EqualToN
=== RUN   TestGetTopNValues/MoreThanN
=== RUN   TestGetTopNValues/EmptyMap
=== RUN   TestGetTopNValues/NegativeN
--- PASS: TestGetTopNValues (0.00s)
--- PASS: TestGetTopNValues/LessThanN (0.00s)
--- PASS: TestGetTopNValues/EqualToN (0.00s)
--- PASS: TestGetTopNValues/MoreThanN (0.00s)
--- PASS: TestGetTopNValues/EmptyMap (0.00s)
--- PASS: TestGetTopNValues/NegativeN (0.00s)
=== RUN   TestProcessUserAgent
=== RUN   TestProcessUserAgent/Kibana/1.0
=== RUN   TestProcessUserAgent/Chrome/123
=== RUN   TestProcessUserAgent/Go-http-client/1.1
=== RUN   TestProcessUserAgent/Mozilla/5.0_(Macintosh;_Intel_Mac_OS_X_10_15_7)_AppleWebKit/537.36_(KHTML,_like_Gecko)_Chrome/122.0.0.0_Safari/537.36
--- PASS: TestProcessUserAgent (0.00s)
--- PASS: TestProcessUserAgent/Kibana/1.0 (0.00s)
--- PASS: TestProcessUserAgent/Chrome/123 (0.00s)
--- PASS: TestProcessUserAgent/Go-http-client/1.1 (0.00s)
--- PASS: TestProcessUserAgent/Mozilla/5.0_(Macintosh;_Intel_Mac_OS_X_10_15_7)_AppleWebKit/537.36_(KHTML,_like_Gecko)_Chrome/122.0.0.0_Safari/537.36 (0.00s)
PASS
ok  	github.com/QuesmaOrg/quesma/quesma/telemetry	1.066s
?   	github.com/QuesmaOrg/quesma/quesma/v2/core/diag	[no test files]
?   	github.com/QuesmaOrg/quesma/quesma/v2/core/routes	[no test files]
?   	github.com/QuesmaOrg/quesma/quesma/v2/core/tracing	[no test files]
=== RUN   TestCompress
--- PASS: TestCompress (0.01s)
=== RUN   TestFindTimestampPrecision
--- PASS: TestFindTimestampPrecision (0.00s)
=== RUN   Test_ParseInterval
=== RUN   Test_ParseInterval/1minute
=== RUN   Test_ParseInterval/5minutes
=== RUN   Test_ParseInterval/15minutes
=== RUN   Test_ParseInterval/1hour
=== RUN   Test_ParseInterval/1day
=== RUN   Test_ParseInterval/1week
=== RUN   Test_ParseInterval/1month
=== RUN   Test_ParseInterval/1year
--- PASS: Test_ParseInterval (0.00s)
--- PASS: Test_ParseInterval/1minute (0.00s)
--- PASS: Test_ParseInterval/5minutes (0.00s)
--- PASS: Test_ParseInterval/15minutes (0.00s)
--- PASS: Test_ParseInterval/1hour (0.00s)
--- PASS: Test_ParseInterval/1day (0.00s)
--- PASS: Test_ParseInterval/1week (0.00s)
--- PASS: Test_ParseInterval/1month (0.00s)
--- PASS: Test_ParseInterval/1year (0.00s)
=== RUN   TestBigIntToIpv6
--- PASS: TestBigIntToIpv6 (0.00s)
=== RUN   TestFlattenMap
=== RUN   TestFlattenMap/Flatten_simple_map
=== RUN   TestFlattenMap/Flatten_nested_map
--- PASS: TestFlattenMap (0.00s)
--- PASS: TestFlattenMap/Flatten_simple_map (0.00s)
--- PASS: TestFlattenMap/Flatten_nested_map (0.00s)
=== RUN   TestRewriteArrayOfObject_Transform
=== RUN   TestRewriteArrayOfObject_Transform/Rewrite_array_of_objects
=== RUN   TestRewriteArrayOfObject_Transform/Rewrite_array_of_objects#01
=== RUN   TestRewriteArrayOfObject_Transform/Rewrite_array_of_objects._Keep_array_of_non_objects
=== RUN   TestRewriteArrayOfObject_Transform/Do_not_touch_array_of_non_objects
=== RUN   TestRewriteArrayOfObject_Transform/Do_not_touch_non-array_objects
=== RUN   TestRewriteArrayOfObject_Transform/Do_not_touch_nested_objects
=== RUN   TestRewriteArrayOfObject_Transform/Rewrite_array_of_objects._Known_limitation._Nested_arrays_are_not_supported.
--- PASS: TestRewriteArrayOfObject_Transform (0.00s)
--- PASS: TestRewriteArrayOfObject_Transform/Rewrite_array_of_objects (0.00s)
--- PASS: TestRewriteArrayOfObject_Transform/Rewrite_array_of_objects#01 (0.00s)
--- PASS: TestRewriteArrayOfObject_Transform/Rewrite_array_of_objects._Keep_array_of_non_objects (0.00s)
--- PASS: TestRewriteArrayOfObject_Transform/Do_not_touch_array_of_non_objects (0.00s)
--- PASS: TestRewriteArrayOfObject_Transform/Do_not_touch_non-array_objects (0.00s)
--- PASS: TestRewriteArrayOfObject_Transform/Do_not_touch_nested_objects (0.00s)
--- PASS: TestRewriteArrayOfObject_Transform/Rewrite_array_of_objects._Known_limitation._Nested_arrays_are_not_supported. (0.00s)
=== RUN   TestJSONDiff
=== RUN   TestJSONDiff/Test_1
=== RUN   TestJSONDiff/Test_2
=== RUN   TestJSONDiff/invalid_type
=== RUN   TestJSONDiff/missing_value
=== RUN   TestJSONDiff/array_length
=== RUN   TestJSONDiff/array_element_difference
=== RUN   TestJSONDiff/array_element_difference#01
=== RUN   TestJSONDiff/object_difference
=== RUN   TestJSONDiff/deep_path_difference
=== RUN   TestJSONDiff/deep_path_difference#01
=== RUN   TestJSONDiff/array_sort_difference_
=== RUN   TestJSONDiff/array_sort_difference_(with_key_extractor)
=== RUN   TestJSONDiff/array_sort_difference_#01
=== RUN   TestJSONDiff/dates
=== RUN   TestJSONDiff/dates_2
=== RUN   TestJSONDiff/dates_3
=== RUN   TestJSONDiff/dates_4
--- PASS: TestJSONDiff (0.00s)
--- PASS: TestJSONDiff/Test_1 (0.00s)
--- PASS: TestJSONDiff/Test_2 (0.00s)
--- PASS: TestJSONDiff/invalid_type (0.00s)
--- PASS: TestJSONDiff/missing_value (0.00s)
--- PASS: TestJSONDiff/array_length (0.00s)
--- PASS: TestJSONDiff/array_element_difference (0.00s)
--- PASS: TestJSONDiff/array_element_difference#01 (0.00s)
--- PASS: TestJSONDiff/object_difference (0.00s)
--- PASS: TestJSONDiff/deep_path_difference (0.00s)
--- PASS: TestJSONDiff/deep_path_difference#01 (0.00s)
--- PASS: TestJSONDiff/array_sort_difference_ (0.00s)
--- PASS: TestJSONDiff/array_sort_difference_(with_key_extractor) (0.00s)
--- PASS: TestJSONDiff/array_sort_difference_#01 (0.00s)
--- PASS: TestJSONDiff/dates (0.00s)
--- PASS: TestJSONDiff/dates_2 (0.00s)
--- PASS: TestJSONDiff/dates_3 (0.00s)
--- PASS: TestJSONDiff/dates_4 (0.00s)
=== RUN   TestIsSmaller
--- PASS: TestIsSmaller (0.00s)
=== RUN   TestIsFloat64AnInt64
--- PASS: TestIsFloat64AnInt64 (0.00s)
=== RUN   TestDistinct
=== RUN   TestDistinct/should_return_nil_for_nil_slice
=== RUN   TestDistinct/should_return_empty_for_empty_slice
=== RUN   TestDistinct/should_return_same_slice_for_distinct_elements
=== RUN   TestDistinct/should_return_distinct_elements
--- PASS: TestDistinct (0.00s)
--- PASS: TestDistinct/should_return_nil_for_nil_slice (0.00s)
--- PASS: TestDistinct/should_return_empty_for_empty_slice (0.00s)
--- PASS: TestDistinct/should_return_same_slice_for_distinct_elements (0.00s)
--- PASS: TestDistinct/should_return_distinct_elements (0.00s)
=== RUN   TestSqlPrettyPrint
--- PASS: TestSqlPrettyPrint (0.00s)
=== RUN   TestSqlPrettyPrint_multipleSqls
--- PASS: TestSqlPrettyPrint_multipleSqls (0.00s)
=== RUN   TestSqlPrettPrintBackticks
--- PASS: TestSqlPrettPrintBackticks (0.00s)
=== RUN   TestInvalidSql
--- PASS: TestInvalidSql (0.00s)
=== RUN   TestGroupBySql
--- PASS: TestGroupBySql (0.00s)
=== RUN   TestPrettySubQuery
--- PASS: TestPrettySubQuery (0.00s)
=== RUN   TestDontExpand
--- PASS: TestDontExpand (0.00s)
=== RUN   TestSqlWith
--- PASS: TestSqlWith (0.00s)
=== RUN   TestSqlPrettyPancake
--- PASS: TestSqlPrettyPancake (0.00s)
=== RUN   TestSqlPrettyPancake2
--- PASS: TestSqlPrettyPancake2 (0.00s)
=== RUN   TestSqlPrettyPancakeWith
--- PASS: TestSqlPrettyPancakeWith (0.00s)
=== RUN   TestJsonPrettifyShortenArrays
--- PASS: TestJsonPrettifyShortenArrays (0.00s)
=== RUN   TestMapDifference
--- PASS: TestMapDifference (0.00s)
=== RUN   TestMapDifference_arraysTypeDifference
--- PASS: TestMapDifference_arraysTypeDifference (0.00s)
=== RUN   TestMapDifference_compareValues_different
--- PASS: TestMapDifference_compareValues_different (0.00s)
=== RUN   TestMapDifference_compareValues_floatEqualsInt
--- PASS: TestMapDifference_compareValues_floatEqualsInt (0.00s)
=== RUN   TestMapDifference_compareFullArrays
--- PASS: TestMapDifference_compareFullArrays (0.00s)
=== RUN   TestJsonDifference
--- PASS: TestJsonDifference (0.00s)
=== RUN   TestMergeMaps
=== RUN   TestMergeMaps/TestMergeMaps_0
=== RUN   TestMergeMaps/TestMergeMaps_1
=== RUN   TestMergeMaps/TestMergeMaps_2
=== RUN   TestMergeMaps/TestMergeMaps_3
--- PASS: TestMergeMaps (0.00s)
--- PASS: TestMergeMaps/TestMergeMaps_0 (0.00s)
--- PASS: TestMergeMaps/TestMergeMaps_1 (0.00s)
--- PASS: TestMergeMaps/TestMergeMaps_2 (0.00s)
--- PASS: TestMergeMaps/TestMergeMaps_3 (0.00s)
=== RUN   TestIsSqlEqual
=== RUN   TestIsSqlEqual/TestIsSqlEqual_0
=== RUN   TestIsSqlEqual/TestIsSqlEqual_1
=== RUN   TestIsSqlEqual/TestIsSqlEqual_2
=== RUN   TestIsSqlEqual/TestIsSqlEqual_3
=== RUN   TestIsSqlEqual/TestIsSqlEqual_4
=== RUN   TestIsSqlEqual/TestIsSqlEqual_5
=== RUN   TestIsSqlEqual/TestIsSqlEqual_6
--- PASS: TestIsSqlEqual (0.00s)
--- PASS: TestIsSqlEqual/TestIsSqlEqual_0 (0.00s)
--- PASS: TestIsSqlEqual/TestIsSqlEqual_1 (0.00s)
--- PASS: TestIsSqlEqual/TestIsSqlEqual_2 (0.00s)
--- PASS: TestIsSqlEqual/TestIsSqlEqual_3 (0.00s)
--- PASS: TestIsSqlEqual/TestIsSqlEqual_4 (0.00s)
--- PASS: TestIsSqlEqual/TestIsSqlEqual_5 (0.00s)
--- PASS: TestIsSqlEqual/TestIsSqlEqual_6 (0.00s)
=== RUN   TestAlmostEmpty
=== RUN   TestAlmostEmpty/TestAlmostEmpty_0
=== RUN   TestAlmostEmpty/TestAlmostEmpty_1
=== RUN   TestAlmostEmpty/TestAlmostEmpty_2
--- PASS: TestAlmostEmpty (0.00s)
--- PASS: TestAlmostEmpty/TestAlmostEmpty_0 (0.00s)
--- PASS: TestAlmostEmpty/TestAlmostEmpty_1 (0.00s)
--- PASS: TestAlmostEmpty/TestAlmostEmpty_2 (0.00s)
=== RUN   TestFilterNonEmpty
=== RUN   TestFilterNonEmpty/0
=== RUN   TestFilterNonEmpty/1
=== RUN   TestFilterNonEmpty/2
--- PASS: TestFilterNonEmpty (0.00s)
--- PASS: TestFilterNonEmpty/0 (0.00s)
--- PASS: TestFilterNonEmpty/1 (0.00s)
--- PASS: TestFilterNonEmpty/2 (0.00s)
=== RUN   Test_equal
--- PASS: Test_equal (0.00s)
=== RUN   TestExtractInt64
--- PASS: TestExtractInt64 (0.00s)
=== RUN   TestFieldEncoding
=== RUN   TestFieldEncoding/@timestamp
=== RUN   TestFieldEncoding/timestamp
=== RUN   TestFieldEncoding/9field
=== RUN   TestFieldEncoding/field9
=== RUN   TestFieldEncoding/field::
=== RUN   TestFieldEncoding/host.name
--- PASS: TestFieldEncoding (0.00s)
--- PASS: TestFieldEncoding/@timestamp (0.00s)
--- PASS: TestFieldEncoding/timestamp (0.00s)
--- PASS: TestFieldEncoding/9field (0.00s)
--- PASS: TestFieldEncoding/field9 (0.00s)
--- PASS: TestFieldEncoding/field:: (0.00s)
--- PASS: TestFieldEncoding/host.name (0.00s)
=== RUN   TestExtractUsernameFromBasicAuthHeader
=== RUN   TestExtractUsernameFromBasicAuthHeader/valid_header
=== RUN   TestExtractUsernameFromBasicAuthHeader/invalid_format
=== RUN   TestExtractUsernameFromBasicAuthHeader/invalid_base64
=== RUN   TestExtractUsernameFromBasicAuthHeader/invalid_decoded_format
=== RUN   TestExtractUsernameFromBasicAuthHeader/bearer_token
--- PASS: TestExtractUsernameFromBasicAuthHeader (0.00s)
--- PASS: TestExtractUsernameFromBasicAuthHeader/valid_header (0.00s)
--- PASS: TestExtractUsernameFromBasicAuthHeader/invalid_format (0.00s)
--- PASS: TestExtractUsernameFromBasicAuthHeader/invalid_base64 (0.00s)
--- PASS: TestExtractUsernameFromBasicAuthHeader/invalid_decoded_format (0.00s)
--- PASS: TestExtractUsernameFromBasicAuthHeader/bearer_token (0.00s)
=== RUN   TestTableNamePatternRegexp
=== RUN   TestTableNamePatternRegexp/foo_into_^foo$
=== RUN   TestTableNamePatternRegexp/foo*_into_^foo.*$
=== RUN   TestTableNamePatternRegexp/foo*bar_into_^foo.*bar$
=== RUN   TestTableNamePatternRegexp/foo*bar*_into_^foo.*bar.*$
=== RUN   TestTableNamePatternRegexp/foo*b[ar*_into_^foo.*b\[ar.*$
=== RUN   TestTableNamePatternRegexp/foo+bar_into_^foo\+bar$
=== RUN   TestTableNamePatternRegexp/foo|bar_into_^foo\|bar$
=== RUN   TestTableNamePatternRegexp/foo(bar_into_^foo\(bar$
=== RUN   TestTableNamePatternRegexp/foo)bar_into_^foo\)bar$
=== RUN   TestTableNamePatternRegexp/foo^bar_into_^foo\^bar$
=== RUN   TestTableNamePatternRegexp/foo$bar_into_^foo\$bar$
=== RUN   TestTableNamePatternRegexp/foo.bar_into_^foo\.bar$
=== RUN   TestTableNamePatternRegexp/foo\bar_into_^foo\\bar$
--- PASS: TestTableNamePatternRegexp (0.00s)
--- PASS: TestTableNamePatternRegexp/foo_into_^foo$ (0.00s)
--- PASS: TestTableNamePatternRegexp/foo*_into_^foo.*$ (0.00s)
--- PASS: TestTableNamePatternRegexp/foo*bar_into_^foo.*bar$ (0.00s)
--- PASS: TestTableNamePatternRegexp/foo*bar*_into_^foo.*bar.*$ (0.00s)
--- PASS: TestTableNamePatternRegexp/foo*b[ar*_into_^foo.*b\[ar.*$ (0.00s)
--- PASS: TestTableNamePatternRegexp/foo+bar_into_^foo\+bar$ (0.00s)
--- PASS: TestTableNamePatternRegexp/foo|bar_into_^foo\|bar$ (0.00s)
--- PASS: TestTableNamePatternRegexp/foo(bar_into_^foo\(bar$ (0.00s)
--- PASS: TestTableNamePatternRegexp/foo)bar_into_^foo\)bar$ (0.00s)
--- PASS: TestTableNamePatternRegexp/foo^bar_into_^foo\^bar$ (0.00s)
--- PASS: TestTableNamePatternRegexp/foo$bar_into_^foo\$bar$ (0.00s)
--- PASS: TestTableNamePatternRegexp/foo.bar_into_^foo\.bar$ (0.00s)
--- PASS: TestTableNamePatternRegexp/foo\bar_into_^foo\\bar$ (0.00s)
PASS
ok  	github.com/QuesmaOrg/quesma/quesma/util	1.039s
=== RUN   Test_dependencyInjection
Feb 10 13:50:37.000 INF Dependency injection into *quesma_api.componentWithDependency :OK - Injected Dependencies
Feb 10 13:50:37.000 INF Dependency injection into *quesma_api.componentWithoutDependencyInjection :SKIP - No dependencies to inject. It doesn't implement DependenciesSetter interface.
--- PASS: Test_dependencyInjection (0.00s)
=== RUN   TestPathRouter_Matches_ShouldIgnoreTrailingSlash
=== RUN   TestPathRouter_Matches_ShouldIgnoreTrailingSlash/GET_/i1,i2/_count
=== RUN   TestPathRouter_Matches_ShouldIgnoreTrailingSlash/POST_/i1,i2/_count
=== RUN   TestPathRouter_Matches_ShouldIgnoreTrailingSlash/GET_/_all/_count/
=== RUN   TestPathRouter_Matches_ShouldIgnoreTrailingSlash/PUT_/_all/_count/
=== RUN   TestPathRouter_Matches_ShouldIgnoreTrailingSlash/POST_/index1/_doc
=== RUN   TestPathRouter_Matches_ShouldIgnoreTrailingSlash/GET_/index1/_doc
=== RUN   TestPathRouter_Matches_ShouldIgnoreTrailingSlash/POST_/index2/_doc/
=== RUN   TestPathRouter_Matches_ShouldIgnoreTrailingSlash/GET_/indexABC/_bulk
=== RUN   TestPathRouter_Matches_ShouldIgnoreTrailingSlash/POST_/indexABC/_bulk/
--- PASS: TestPathRouter_Matches_ShouldIgnoreTrailingSlash (0.00s)
--- PASS: TestPathRouter_Matches_ShouldIgnoreTrailingSlash/GET_/i1,i2/_count (0.00s)
--- PASS: TestPathRouter_Matches_ShouldIgnoreTrailingSlash/POST_/i1,i2/_count (0.00s)
--- PASS: TestPathRouter_Matches_ShouldIgnoreTrailingSlash/GET_/_all/_count/ (0.00s)
--- PASS: TestPathRouter_Matches_ShouldIgnoreTrailingSlash/PUT_/_all/_count/ (0.00s)
--- PASS: TestPathRouter_Matches_ShouldIgnoreTrailingSlash/POST_/index1/_doc (0.00s)
--- PASS: TestPathRouter_Matches_ShouldIgnoreTrailingSlash/GET_/index1/_doc (0.00s)
--- PASS: TestPathRouter_Matches_ShouldIgnoreTrailingSlash/POST_/index2/_doc/ (0.00s)
--- PASS: TestPathRouter_Matches_ShouldIgnoreTrailingSlash/GET_/indexABC/_bulk (0.00s)
--- PASS: TestPathRouter_Matches_ShouldIgnoreTrailingSlash/POST_/indexABC/_bulk/ (0.00s)
=== RUN   TestShouldMatchMultipleHttpMethods
=== RUN   TestShouldMatchMultipleHttpMethods/POST_/index1/_bulk
=== RUN   TestShouldMatchMultipleHttpMethods/GET_/index1/_bulk
=== RUN   TestShouldMatchMultipleHttpMethods/PUT_/index1/_bulk
=== RUN   TestShouldMatchMultipleHttpMethods/DELETE_/index1/_bulk
--- PASS: TestShouldMatchMultipleHttpMethods (0.00s)
--- PASS: TestShouldMatchMultipleHttpMethods/POST_/index1/_bulk (0.00s)
--- PASS: TestShouldMatchMultipleHttpMethods/GET_/index1/_bulk (0.00s)
--- PASS: TestShouldMatchMultipleHttpMethods/PUT_/index1/_bulk (0.00s)
--- PASS: TestShouldMatchMultipleHttpMethods/DELETE_/index1/_bulk (0.00s)
PASS
ok  	github.com/QuesmaOrg/quesma/quesma/v2/core	1.011s
FAIL
Error: Process completed with exit code 1.
0s
0s
0s
