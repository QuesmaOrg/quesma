// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package frontend_connectors

import (
	"context"
	"github.com/ucarion/urlpath"
	"net/http"
	"quesma/elasticsearch"
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
	ResolveIndexPath      = "/_resolve/index/:index"
	ClusterHealthPath     = "/_cluster/health"
	AsyncSearchIdPath     = "/_async_search/:id"
	AsyncSearchStatusPath = "/_async_search/status/:id"
	IndexPath             = "/:index"
	/*
		section on metadata/headers below
	*/
	//SearchIndexTargetKey = "search_index_target"
	IndexPattern = "index_pattern"
	PathPattern  = "path_pattern"
	Id           = "id"

	// Maybe to be removed, it's a dumb fallback handler
	Bypass = "true"
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

	internalPaths := append(elasticsearch.InternalPaths, "/_stats")

	for _, esInternalPath := range internalPaths {
		router.Register(esInternalPath, quesma_api.Always(), func(ctx context.Context, req *quesma_api.Request, writer http.ResponseWriter) (*quesma_api.Result, error) {
			metadata := quesma_api.MakeNewMetadata()
			metadata[Bypass] = true
			return &quesma_api.Result{Meta: metadata, GenericResult: req.OriginalRequest}, nil
		})
	}
	// TODO: Somehow this messes up the router, so we need to fix it
	//router.Register(IndexPath, quesma_api.IsHTTPMethod("GET"), func(ctx context.Context, req *quesma_api.Request, writer http.ResponseWriter) (*quesma_api.Result, error) {
	//	metadata := quesma_api.MakeNewMetadata()
	//	metadata[IndexPattern] = getIndexPatternFromRequestURI(req.OriginalRequest, IndexPath)
	//	metadata[PathPattern] = IndexPath
	//	return &quesma_api.Result{Meta: metadata, GenericResult: req.OriginalRequest}, nil
	//})
	router.Register(IndexSearchPath, quesma_api.IsHTTPMethod("GET", "POST"), func(ctx context.Context, req *quesma_api.Request, writer http.ResponseWriter) (*quesma_api.Result, error) {
		metadata := quesma_api.MakeNewMetadata()
		metadata[IndexPattern] = getIndexPatternFromRequestURI(req.OriginalRequest, IndexSearchPath)
		metadata[PathPattern] = IndexSearchPath
		return &quesma_api.Result{Meta: metadata, GenericResult: req.OriginalRequest}, nil
	})
	router.Register(IndexAsyncSearchPath, quesma_api.IsHTTPMethod("POST"), func(ctx context.Context, req *quesma_api.Request, writer http.ResponseWriter) (*quesma_api.Result, error) {
		metadata := quesma_api.MakeNewMetadata()
		metadata[IndexPattern] = getIndexPatternFromRequestURI(req.OriginalRequest, IndexAsyncSearchPath)
		metadata[PathPattern] = IndexAsyncSearchPath
		return &quesma_api.Result{Meta: metadata, GenericResult: req.OriginalRequest}, nil
	})
	router.Register(AsyncSearchIdPath, quesma_api.IsHTTPMethod("GET", "DELETE"), func(ctx context.Context, req *quesma_api.Request, writer http.ResponseWriter) (*quesma_api.Result, error) {
		metadata := quesma_api.MakeNewMetadata()
		metadata[Id] = getIdFromRequestURI(req.OriginalRequest, AsyncSearchIdPath)
		metadata[PathPattern] = AsyncSearchIdPath
		return &quesma_api.Result{Meta: metadata, GenericResult: req.OriginalRequest}, nil
	})
	router.Register(AsyncSearchStatusPath, quesma_api.IsHTTPMethod("GET"), func(ctx context.Context, req *quesma_api.Request, writer http.ResponseWriter) (*quesma_api.Result, error) {
		metadata := quesma_api.MakeNewMetadata()
		metadata[Id] = getIdFromRequestURI(req.OriginalRequest, AsyncSearchStatusPath)
		metadata[PathPattern] = AsyncSearchStatusPath
		return &quesma_api.Result{Meta: metadata, GenericResult: req.OriginalRequest}, nil
	})
	router.Register(FieldCapsPath, quesma_api.IsHTTPMethod("GET", "POST"), func(ctx context.Context, req *quesma_api.Request, writer http.ResponseWriter) (*quesma_api.Result, error) {
		metadata := quesma_api.MakeNewMetadata()
		metadata[IndexPattern] = getIndexPatternFromRequestURI(req.OriginalRequest, FieldCapsPath)
		metadata[PathPattern] = FieldCapsPath
		return &quesma_api.Result{Meta: metadata, GenericResult: req.OriginalRequest}, nil
	})
	router.Register(ResolveIndexPath, quesma_api.IsHTTPMethod("GET"), func(ctx context.Context, req *quesma_api.Request, writer http.ResponseWriter) (*quesma_api.Result, error) {
		metadata := quesma_api.MakeNewMetadata()
		metadata[IndexPattern] = getIndexPatternFromRequestURI(req.OriginalRequest, ResolveIndexPath)
		metadata[PathPattern] = ResolveIndexPath
		return &quesma_api.Result{Meta: metadata, GenericResult: req.OriginalRequest}, nil
	})
	router.Register(ClusterHealthPath, quesma_api.IsHTTPMethod("GET"), func(ctx context.Context, req *quesma_api.Request, writer http.ResponseWriter) (*quesma_api.Result, error) {
		metadata := quesma_api.MakeNewMetadata()
		metadata[PathPattern] = ClusterHealthPath
		return &quesma_api.Result{Meta: metadata, GenericResult: req.OriginalRequest}, nil
	})
	router.Register(IndexRefreshPath, quesma_api.IsHTTPMethod("POST"), func(ctx context.Context, req *quesma_api.Request, writer http.ResponseWriter) (*quesma_api.Result, error) {
		metadata := quesma_api.MakeNewMetadata()
		metadata[IndexPattern] = getIndexPatternFromRequestURI(req.OriginalRequest, IndexRefreshPath)
		metadata[PathPattern] = IndexRefreshPath
		return &quesma_api.Result{Meta: metadata, GenericResult: req.OriginalRequest}, nil
	})
	router.Register(IndexMappingPath, quesma_api.IsHTTPMethod("GET"), func(ctx context.Context, req *quesma_api.Request, writer http.ResponseWriter) (*quesma_api.Result, error) {
		metadata := quesma_api.MakeNewMetadata()
		metadata[IndexPattern] = getIndexPatternFromRequestURI(req.OriginalRequest, IndexRefreshPath)
		metadata[PathPattern] = IndexMappingPath
		return &quesma_api.Result{Meta: metadata, GenericResult: req.OriginalRequest}, nil
	})
	router.Register(TermsEnumPath, quesma_api.IsHTTPMethod("POST"), func(ctx context.Context, req *quesma_api.Request, writer http.ResponseWriter) (*quesma_api.Result, error) {
		metadata := quesma_api.MakeNewMetadata()
		metadata[IndexPattern] = getIndexPatternFromRequestURI(req.OriginalRequest, TermsEnumPath)
		metadata[PathPattern] = TermsEnumPath
		return &quesma_api.Result{Meta: metadata, GenericResult: req.OriginalRequest}, nil
	})
	router.Register(IndexCountPath, quesma_api.IsHTTPMethod("GET"), func(ctx context.Context, req *quesma_api.Request, writer http.ResponseWriter) (*quesma_api.Result, error) {
		metadata := quesma_api.MakeNewMetadata()
		metadata[IndexPattern] = getIndexPatternFromRequestURI(req.OriginalRequest, IndexCountPath)
		metadata[PathPattern] = IndexCountPath
		return &quesma_api.Result{Meta: metadata, GenericResult: req.OriginalRequest}, nil
	})

	router.Register("*", quesma_api.Always(), func(ctx context.Context, req *quesma_api.Request, writer http.ResponseWriter) (*quesma_api.Result, error) {
		metadata := quesma_api.MakeNewMetadata()
		metadata[Bypass] = true
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
