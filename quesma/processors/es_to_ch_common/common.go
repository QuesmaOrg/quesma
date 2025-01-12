// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package es_to_ch_common

// Shared code for Elasticsearch to Clickhouse Query/Ingest processors

const (
	IndexPattern = "index_pattern"
	PathPattern  = "path_pattern"
	Id           = "id"

	// Maybe to be removed, it's a dumb fallback handler
	Bypass = "true"
)

// Copied from `quesma/v2/core/routes/paths.go` to temporarily avoid import cycle
const (
	GlobalSearchPath          = "/_search"
	IndexSearchPath           = "/:index/_search"
	IndexAsyncSearchPath      = "/:index/_async_search"
	IndexCountPath            = "/:index/_count"
	IndexDocPath              = "/:index/_doc"
	IndexRefreshPath          = "/:index/_refresh"
	IndexBulkPath             = "/:index/_bulk"
	IndexMappingPath          = "/:index/_mapping"
	FieldCapsPath             = "/:index/_field_caps"
	TermsEnumPath             = "/:index/_terms_enum"
	EQLSearch                 = "/:index/_eql/search"
	ResolveIndexPath          = "/_resolve/index/:index"
	ClusterHealthPath         = "/_cluster/health"
	BulkPath                  = "/_bulk"
	AsyncSearchIdPrefix       = "/_async_search/"
	AsyncSearchIdPath         = "/_async_search/:id"
	AsyncSearchStatusPath     = "/_async_search/status/:id"
	KibanaInternalPrefix      = "/.kibana_"
	IndexPath                 = "/:index"
	ExecutePainlessScriptPath = "/_scripts/painless/_execute" // This path is used on the Kibana side to evaluate painless scripts when adding a new scripted field.

	// Quesma internal paths

	QuesmaTableResolverPath = "/:index/_quesma_table_resolver"
)

const (
	// RealSourceHeader is a header to determine what exactly processor has calld
	RealSourceHeader        = "X-Backend-Called"
	RealSourceClickHouse    = "CLICKHOUSE"
	RealSourceElasticsearch = "ELASTICSEARCH"
	RealSourceQuesma        = "NONE"  // if response is just processor's own rendered content, no DB is called
	RealSourceMixed         = "MIXED" // e.g. in case of _resolve API
)
