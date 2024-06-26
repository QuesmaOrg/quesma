// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package routes

import (
	"strings"
)

const (
	GlobalSearchPath     = "/_search"
	IndexSearchPath      = "/:index/_search"
	IndexAsyncSearchPath = "/:index/_async_search"
	IndexCountPath       = "/:index/_count"
	IndexDocPath         = "/:index/_doc"
	IndexRefreshPath     = "/:index/_refresh"
	IndexBulkPath        = "/:index/_bulk"
	FieldCapsPath        = "/:index/_field_caps"
	TermsEnumPath        = "/:index/_terms_enum"
	EQLSearch            = "/:index/_eql/search"
	ResolveIndexPath     = "/_resolve/index/:index"
	ClusterHealthPath    = "/_cluster/health"
	BulkPath             = "/_bulk"
	AsyncSearchIdPrefix  = "/_async_search/"
	AsyncSearchIdPath    = "/_async_search/:id"
	KibanaInternalPrefix = "/.kibana_"
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
