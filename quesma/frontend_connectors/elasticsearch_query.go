// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package frontend_connectors

import (
	"context"
	"github.com/ucarion/urlpath"
	"net/http"
	"quesma/elasticsearch"
	"quesma/processors/es_to_ch_common"
	"quesma/quesma/config"
	quesma_api "quesma_v2/core"
)

type ElasticsearchQueryFrontendConnector struct {
	BasicHTTPFrontendConnector
}

const (
	IndexURLParam = "index"
	IdURLParam    = "id"
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
			metadata[es_to_ch_common.Bypass] = true
			return &quesma_api.Result{Meta: metadata, GenericResult: req.OriginalRequest}, nil
		})
	}
	// TODO: Somehow this messes up the router, so we need to fix it
	//router.Register(IndexPath, quesma_api.IsHTTPMethod("GET"), func(ctx context.Context, req *quesma_api.Request, writer http.ResponseWriter) (*quesma_api.Result, error) {
	//	metadata := quesma_api.MakeNewMetadata()
	//	metadata[IndexPattern] = getParamFromRequestURI(req.OriginalRequest, IndexPath)
	//	metadata[PathPattern] = IndexPath
	//	return &quesma_api.Result{Meta: metadata, GenericResult: req.OriginalRequest}, nil
	//})
	router.Register(es_to_ch_common.IndexSearchPath, quesma_api.IsHTTPMethod("GET", "POST"), func(ctx context.Context, req *quesma_api.Request, writer http.ResponseWriter) (*quesma_api.Result, error) {
		metadata := quesma_api.MakeNewMetadata()
		metadata[es_to_ch_common.IndexPattern] = getParamFromRequestURI(req.OriginalRequest, es_to_ch_common.IndexSearchPath, IndexURLParam)
		metadata[es_to_ch_common.PathPattern] = es_to_ch_common.IndexSearchPath
		return &quesma_api.Result{Meta: metadata, GenericResult: req.OriginalRequest}, nil
	})
	router.Register(es_to_ch_common.IndexAsyncSearchPath, quesma_api.IsHTTPMethod("POST"), func(ctx context.Context, req *quesma_api.Request, writer http.ResponseWriter) (*quesma_api.Result, error) {
		metadata := quesma_api.MakeNewMetadata()
		metadata[es_to_ch_common.IndexPattern] = getParamFromRequestURI(req.OriginalRequest, es_to_ch_common.IndexAsyncSearchPath, IndexURLParam)
		metadata[es_to_ch_common.PathPattern] = es_to_ch_common.IndexAsyncSearchPath
		return &quesma_api.Result{Meta: metadata, GenericResult: req.OriginalRequest}, nil
	})
	router.Register(es_to_ch_common.AsyncSearchIdPath, quesma_api.IsHTTPMethod("GET", "DELETE"), func(ctx context.Context, req *quesma_api.Request, writer http.ResponseWriter) (*quesma_api.Result, error) {
		metadata := quesma_api.MakeNewMetadata()
		metadata[es_to_ch_common.Id] = getParamFromRequestURI(req.OriginalRequest, es_to_ch_common.AsyncSearchIdPath, IdURLParam)
		metadata[es_to_ch_common.PathPattern] = es_to_ch_common.AsyncSearchIdPath
		return &quesma_api.Result{Meta: metadata, GenericResult: req.OriginalRequest}, nil
	})
	router.Register(es_to_ch_common.AsyncSearchStatusPath, quesma_api.IsHTTPMethod("GET"), func(ctx context.Context, req *quesma_api.Request, writer http.ResponseWriter) (*quesma_api.Result, error) {
		metadata := quesma_api.MakeNewMetadata()
		metadata[es_to_ch_common.Id] = getParamFromRequestURI(req.OriginalRequest, es_to_ch_common.AsyncSearchStatusPath, IdURLParam)
		metadata[es_to_ch_common.PathPattern] = es_to_ch_common.AsyncSearchStatusPath
		return &quesma_api.Result{Meta: metadata, GenericResult: req.OriginalRequest}, nil
	})
	router.Register(es_to_ch_common.FieldCapsPath, quesma_api.IsHTTPMethod("GET", "POST"), func(ctx context.Context, req *quesma_api.Request, writer http.ResponseWriter) (*quesma_api.Result, error) {
		metadata := quesma_api.MakeNewMetadata()
		metadata[es_to_ch_common.IndexPattern] = getParamFromRequestURI(req.OriginalRequest, es_to_ch_common.FieldCapsPath, IndexURLParam)
		metadata[es_to_ch_common.PathPattern] = es_to_ch_common.FieldCapsPath
		return &quesma_api.Result{Meta: metadata, GenericResult: req.OriginalRequest}, nil
	})
	router.Register(es_to_ch_common.ResolveIndexPath, quesma_api.IsHTTPMethod("GET"), func(ctx context.Context, req *quesma_api.Request, writer http.ResponseWriter) (*quesma_api.Result, error) {
		metadata := quesma_api.MakeNewMetadata()
		metadata[es_to_ch_common.IndexPattern] = getParamFromRequestURI(req.OriginalRequest, es_to_ch_common.ResolveIndexPath, IndexURLParam)
		metadata[es_to_ch_common.PathPattern] = es_to_ch_common.ResolveIndexPath
		return &quesma_api.Result{Meta: metadata, GenericResult: req.OriginalRequest}, nil
	})
	router.Register(es_to_ch_common.ClusterHealthPath, quesma_api.IsHTTPMethod("GET"), func(ctx context.Context, req *quesma_api.Request, writer http.ResponseWriter) (*quesma_api.Result, error) {
		metadata := quesma_api.MakeNewMetadata()
		metadata[es_to_ch_common.PathPattern] = es_to_ch_common.ClusterHealthPath
		return &quesma_api.Result{Meta: metadata, GenericResult: req.OriginalRequest}, nil
	})
	router.Register(es_to_ch_common.IndexRefreshPath, quesma_api.IsHTTPMethod("POST"), func(ctx context.Context, req *quesma_api.Request, writer http.ResponseWriter) (*quesma_api.Result, error) {
		metadata := quesma_api.MakeNewMetadata()
		metadata[es_to_ch_common.IndexPattern] = getParamFromRequestURI(req.OriginalRequest, es_to_ch_common.IndexRefreshPath, IndexURLParam)
		metadata[es_to_ch_common.PathPattern] = es_to_ch_common.IndexRefreshPath
		return &quesma_api.Result{Meta: metadata, GenericResult: req.OriginalRequest}, nil
	})
	router.Register(es_to_ch_common.IndexMappingPath, quesma_api.IsHTTPMethod("GET"), func(ctx context.Context, req *quesma_api.Request, writer http.ResponseWriter) (*quesma_api.Result, error) {
		metadata := quesma_api.MakeNewMetadata()
		metadata[es_to_ch_common.IndexPattern] = getParamFromRequestURI(req.OriginalRequest, es_to_ch_common.IndexRefreshPath, IndexURLParam)
		metadata[es_to_ch_common.PathPattern] = es_to_ch_common.IndexMappingPath
		return &quesma_api.Result{Meta: metadata, GenericResult: req.OriginalRequest}, nil
	})
	router.Register(es_to_ch_common.TermsEnumPath, quesma_api.IsHTTPMethod("POST"), func(ctx context.Context, req *quesma_api.Request, writer http.ResponseWriter) (*quesma_api.Result, error) {
		metadata := quesma_api.MakeNewMetadata()
		metadata[es_to_ch_common.IndexPattern] = getParamFromRequestURI(req.OriginalRequest, es_to_ch_common.TermsEnumPath, IndexURLParam)
		metadata[es_to_ch_common.PathPattern] = es_to_ch_common.TermsEnumPath
		return &quesma_api.Result{Meta: metadata, GenericResult: req.OriginalRequest}, nil
	})
	router.Register(es_to_ch_common.IndexCountPath, quesma_api.IsHTTPMethod("GET"), func(ctx context.Context, req *quesma_api.Request, writer http.ResponseWriter) (*quesma_api.Result, error) {
		metadata := quesma_api.MakeNewMetadata()
		metadata[es_to_ch_common.IndexPattern] = getParamFromRequestURI(req.OriginalRequest, es_to_ch_common.IndexCountPath, IndexURLParam)
		metadata[es_to_ch_common.PathPattern] = es_to_ch_common.IndexCountPath
		return &quesma_api.Result{Meta: metadata, GenericResult: req.OriginalRequest}, nil
	})

	router.Register("*", quesma_api.Always(), func(ctx context.Context, req *quesma_api.Request, writer http.ResponseWriter) (*quesma_api.Result, error) {
		metadata := quesma_api.MakeNewMetadata()
		metadata[es_to_ch_common.Bypass] = true
		return &quesma_api.Result{Meta: metadata, GenericResult: req.OriginalRequest}, nil
	})

	fc.AddRouter(router)
	return fc
}

func getParamFromRequestURI(request *http.Request, path string, param string) string {
	if request.URL == nil {
		return ""
	}
	expectedUrl := urlpath.New(path)
	if match, ok := expectedUrl.Match(request.URL.Path); !ok {
		return ""
	} else {
		return match.Params[param]
	}
}
