package routes

import (
	"strings"
)

const (
	IndexSearchPath      = "/:index/_search"
	IndexAsyncSearchPath = "/:index/_async_search"
	IndexCountPath       = "/:index/_count"
	IndexDocPath         = "/:index/_doc"
	IndexBulkPath        = "/:index/_bulk"
	FieldCapsPath        = "/:index/_field_caps"
	TermsEnumPath        = "/:index/_terms_enum"
	ClusterHealthPath    = "/_cluster/health"
	BulkPath             = "/_bulk"
	AsyncSearchIdPath    = "/_async_search/:id"
)

var NotQueryPaths = []string{
	"_bulk",
	"_doc",
	"_field_caps",
	"_health",
}

func IsNotQueryPath(path string) bool {
	for _, p := range NotQueryPaths {
		if strings.Contains(path, p) {
			return true
		}
	}
	return false
}

func IsQueryPath(path string) bool {
	return !IsNotQueryPath(path)
}
