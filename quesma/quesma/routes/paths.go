package routes

import (
	"github.com/ucarion/urlpath"
	"strings"
)

const (
	IndexSearchPath      = "/:index/_search"
	IndexAsyncSearchPath = "/:index/_async_search"
	IndexDocPath         = "/:index/_doc"
	IndexBulkPath        = "/:index/_bulk"
	FieldCapsPath        = "/:index/_field_caps"
	TermsEnumPath        = "/:index/_terms_enum"
	ClusterHealthPath    = "/_cluster/health"
	BulkPath             = "/_bulk"
)

var NotQueryPaths = []string{
	"_bulk",
	"_doc",
	"_field_caps",
	"_health",
}

var (
	indexSearchPathMatcher  = urlpath.New(IndexSearchPath)
	indexAsyncSearchMatcher = urlpath.New(IndexAsyncSearchPath)
)

func IsIndexSearchPath(path string) bool {
	_, match := indexSearchPathMatcher.Match(path)
	return match
}

func IsIndexAsyncSearchPath(path string) bool {
	_, match := indexAsyncSearchMatcher.Match(path)
	return match
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
