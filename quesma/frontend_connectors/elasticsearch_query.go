// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package frontend_connectors

import (
	"context"
	"net/http"
	"quesma/quesma/config"
	quesma_api "quesma_v2/core"
)

type ElasticsearchQueryFrontendConnector struct {
	BasicHTTPFrontendConnector
}

const ( // taken from `router.go`
	IndexSearchPath       = "/:index/_search"
	IndexAsyncSearchPath  = "/:index/_async_search"
	IndexCountPath        = "/:index/_count"
	IndexRefreshPath      = "/:index/_refresh"
	IndexMappingPath      = "/:index/_mapping"
	FieldCapsPath         = "/:index/_field_caps"
	TermsEnumPath         = "/:index/_terms_enum"
	EQLSearch             = "/:index/_eql/search"
	ResolveIndexPath      = "/_resolve/index/:index"
	ClusterHealthPath     = "/_cluster/health"
	BulkPath              = "/_bulk"
	AsyncSearchIdPrefix   = "/_async_search/"
	AsyncSearchIdPath     = "/_async_search/:id"
	AsyncSearchStatusPath = "/_async_search/status/:id"
	/*
		section on metadata/headers below
	*/
	SearchIndexTargetKey = "search_index_target"
)

func NewElasticsearchQueryFrontendConnector(endpoint string, cfg *config.QuesmaConfiguration) *ElasticsearchQueryFrontendConnector {

	basicHttpFrontendConnector := NewBasicHTTPFrontendConnector(endpoint, cfg)
	basicHttpFrontendConnector.responseMutator = func(w http.ResponseWriter) http.ResponseWriter {
		w.Header().Set("Content-Type", "application/json")
		return w
	}
	fc := &ElasticsearchQueryFrontendConnector{
		BasicHTTPFrontendConnector: *basicHttpFrontendConnector,
	}
	router := quesma_api.NewPathRouter()
	router.AddRoute(IndexSearchPath, searchHandler)
	router.AddRoute(IndexAsyncSearchPath, searchHandler)
	fc.AddRouter(router)
	return fc
}

func searchHandler(_ context.Context, request *quesma_api.Request, _ http.ResponseWriter) (*quesma_api.Result, error) {
	metadata := quesma_api.MakeNewMetadata()
	metadata[SearchIndexTargetKey] = getIndexFromRequest(request.OriginalRequest)
	return &quesma_api.Result{Meta: metadata, GenericResult: request.OriginalRequest}, nil
}

// getIndexFromRequest exist in `elasticsearch_ingest`, so it should be moved to a common packaga perhaps?
//func getIndexFromRequest(request *http.Request) string {
//	expectedUrl := urlpath.New("/:index/*")
//	match, _ := expectedUrl.Match(request.URL.Path) // safe to call at this level
//	return match.Params["index"]
//}
