package routes

import "github.com/ucarion/urlpath"

const (
	IndexSearchPath      = "/:index/_search"
	IndexAsyncSearchPath = "/:index/_async_search"
	IndexDocPath         = "/:index/_doc"
	IndexBulkPath        = "/:index/_bulk"
	FieldCapsPath        = "/:index/_field_caps"
	TermsEnumPath        = "/:index/_terms_enum"
	ClusterHealthPath    = "/_cluster/health"
	BulkPath             = "/_bulk"
	SearchPath           = "/_search"
)

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
