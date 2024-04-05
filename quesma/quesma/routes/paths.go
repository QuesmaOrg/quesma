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
	IndexBulkPath        = "/:index/_bulk"
	FieldCapsPath        = "/:index/_field_caps"
	TermsEnumPath        = "/:index/_terms_enum"
	ClusterHealthPath    = "/_cluster/health"
	BulkPath             = "/_bulk"
	AsyncSearchIdPrefix  = "/_async_search/"
	AsyncSearchIdPath    = "/_async_search/:id"
)

var notQueryPaths = []string{
	"_bulk",
	"_doc",
	"_field_caps",
	"_health",
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
