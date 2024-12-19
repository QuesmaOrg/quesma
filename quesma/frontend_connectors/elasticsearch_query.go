// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package frontend_connectors

import (
	"context"
	"github.com/ucarion/urlpath"
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
	//SearchIndexTargetKey = "search_index_target"
	IndexPattern = "index_pattern"
	PathPattern  = "path_pattern"
	Id           = "id"
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
	router.AddRoute(IndexSearchPath, func(ctx context.Context, req *quesma_api.Request, writer http.ResponseWriter) (*quesma_api.Result, error) {
		metadata := quesma_api.MakeNewMetadata()
		metadata[IndexPattern] = getIndexPatternFromRequestURI(req.OriginalRequest, IndexSearchPath)
		metadata[PathPattern] = IndexSearchPath
		return &quesma_api.Result{Meta: metadata, GenericResult: req.OriginalRequest}, nil
	})
	router.AddRoute(IndexAsyncSearchPath, func(ctx context.Context, req *quesma_api.Request, writer http.ResponseWriter) (*quesma_api.Result, error) {
		metadata := quesma_api.MakeNewMetadata()
		metadata[IndexPattern] = getIndexPatternFromRequestURI(req.OriginalRequest, IndexSearchPath)
		metadata[PathPattern] = IndexAsyncSearchPath
		return &quesma_api.Result{Meta: metadata, GenericResult: req.OriginalRequest}, nil
	})
	router.AddRoute(AsyncSearchIdPath, func(ctx context.Context, req *quesma_api.Request, writer http.ResponseWriter) (*quesma_api.Result, error) {
		metadata := quesma_api.MakeNewMetadata()
		metadata[Id] = getIdFromRequestURI(req.OriginalRequest, AsyncSearchIdPath)
		metadata[PathPattern] = AsyncSearchIdPath
		return &quesma_api.Result{Meta: metadata, GenericResult: req.OriginalRequest}, nil
	})
	fc.AddRouter(router)
	return fc
}

func getIndexPatternFromRequestURI(request *http.Request, indexPath string) string {
	expectedUrl := urlpath.New(indexPath)
	match, _ := expectedUrl.Match(request.URL.Path) // safe to call at this level
	return match.Params["index"]
}

func getIdFromRequestURI(request *http.Request, idPath string) string {
	expectedUrl := urlpath.New(idPath)
	match, _ := expectedUrl.Match(request.URL.Path) // safe to call at this level
	return match.Params["id"]
}
