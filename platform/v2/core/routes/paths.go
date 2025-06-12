// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package routes

import (
	"strings"
)

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
	IndexPatternPitPath       = "/:index/_pit"
	PitPath                   = "/_pit"
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

	IndexMsearchPath  = "/:index/_msearch"
	GlobalMsearchPath = "/_msearch"

	// Quesma internal paths

	QuesmaTableResolverPath = "/:index/_quesma_table_resolver"
	QuesmaReloadTablsPath   = "/_quesma/reload-tables"
)

var notQueryPaths = []string{
	"_bulk",
	"_doc",
	"_field_caps",
	"_health",
	"_resolve",
	"_refresh",
}

func IsNotQueryPath(path string) bool {
	for _, p := range notQueryPaths {
		if strings.Contains(path, p) {
			return true
		}
	}
	return false
}

func IsQueryPath(path string) bool {
	return !IsNotQueryPath(path)
}
