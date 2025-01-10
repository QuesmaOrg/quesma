// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package frontend_connectors

import (
	"context"
	"net/http"
	"quesma/elasticsearch"
	"quesma/processors/es_to_ch_common"
	"quesma/quesma/config"
	quesma_api "quesma_v2/core"
)

type ElasticsearchQueryFrontendConnector struct {
	*BasicHTTPFrontendConnector
}

func NewElasticsearchQueryFrontendConnector(endpoint string, cfg *config.QuesmaConfiguration) *ElasticsearchQueryFrontendConnector {

	basicHttpFrontendConnector := NewBasicHTTPFrontendConnector(endpoint, cfg)
	basicHttpFrontendConnector.responseMutator = func(w http.ResponseWriter) http.ResponseWriter {
		w.Header().Set("Content-Type", "application/json")
		return w
	}
	fc := &ElasticsearchQueryFrontendConnector{
		BasicHTTPFrontendConnector: basicHttpFrontendConnector,
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
	//	return &quesma_api.Result{Meta: metadata, GenericResult: req.OriginalRequest}, nil
	//})
	router.Register(es_to_ch_common.IndexSearchPath, quesma_api.IsHTTPMethod("GET", "POST"), func(ctx context.Context, req *quesma_api.Request, writer http.ResponseWriter) (*quesma_api.Result, error) {
		return setPathPattern(req, es_to_ch_common.IndexSearchPath), nil
	})
	router.Register(es_to_ch_common.IndexAsyncSearchPath, quesma_api.IsHTTPMethod("POST"), func(ctx context.Context, req *quesma_api.Request, writer http.ResponseWriter) (*quesma_api.Result, error) {
		return setPathPattern(req, es_to_ch_common.IndexAsyncSearchPath), nil
	})
	router.Register(es_to_ch_common.AsyncSearchIdPath, quesma_api.IsHTTPMethod("GET", "DELETE"), func(ctx context.Context, req *quesma_api.Request, writer http.ResponseWriter) (*quesma_api.Result, error) {
		return setPathPattern(req, es_to_ch_common.AsyncSearchIdPath), nil
	})
	router.Register(es_to_ch_common.AsyncSearchStatusPath, quesma_api.IsHTTPMethod("GET"), func(ctx context.Context, req *quesma_api.Request, writer http.ResponseWriter) (*quesma_api.Result, error) {
		return setPathPattern(req, es_to_ch_common.AsyncSearchStatusPath), nil
	})
	router.Register(es_to_ch_common.FieldCapsPath, quesma_api.IsHTTPMethod("GET", "POST"), func(ctx context.Context, req *quesma_api.Request, writer http.ResponseWriter) (*quesma_api.Result, error) {
		return setPathPattern(req, es_to_ch_common.FieldCapsPath), nil
	})
	router.Register(es_to_ch_common.ResolveIndexPath, quesma_api.IsHTTPMethod("GET"), func(ctx context.Context, req *quesma_api.Request, writer http.ResponseWriter) (*quesma_api.Result, error) {
		return setPathPattern(req, es_to_ch_common.ResolveIndexPath), nil
	})
	router.Register(es_to_ch_common.ClusterHealthPath, quesma_api.IsHTTPMethod("GET"), func(ctx context.Context, req *quesma_api.Request, writer http.ResponseWriter) (*quesma_api.Result, error) {
		return setPathPattern(req, es_to_ch_common.ClusterHealthPath), nil
	})
	router.Register(es_to_ch_common.IndexRefreshPath, quesma_api.IsHTTPMethod("POST"), func(ctx context.Context, req *quesma_api.Request, writer http.ResponseWriter) (*quesma_api.Result, error) {
		return setPathPattern(req, es_to_ch_common.IndexRefreshPath), nil
	})
	router.Register(es_to_ch_common.IndexMappingPath, quesma_api.IsHTTPMethod("GET"), func(ctx context.Context, req *quesma_api.Request, writer http.ResponseWriter) (*quesma_api.Result, error) {
		return setPathPattern(req, es_to_ch_common.IndexMappingPath), nil
	})
	router.Register(es_to_ch_common.TermsEnumPath, quesma_api.IsHTTPMethod("POST"), func(ctx context.Context, req *quesma_api.Request, writer http.ResponseWriter) (*quesma_api.Result, error) {
		return setPathPattern(req, es_to_ch_common.TermsEnumPath), nil
	})
	router.Register(es_to_ch_common.IndexCountPath, quesma_api.IsHTTPMethod("GET"), func(ctx context.Context, req *quesma_api.Request, writer http.ResponseWriter) (*quesma_api.Result, error) {
		return setPathPattern(req, es_to_ch_common.IndexCountPath), nil
	})

	router.Register("*", quesma_api.Always(), func(ctx context.Context, req *quesma_api.Request, writer http.ResponseWriter) (*quesma_api.Result, error) {
		return &quesma_api.Result{Meta: quesma_api.MakeNewMetadata(), GenericResult: req.OriginalRequest}, nil
	})

	fc.AddRouter(router)
	return fc
}

func setPathPattern(req *quesma_api.Request, pathPattern string) *quesma_api.Result {
	metadata := quesma_api.MakeNewMetadata()
	metadata[es_to_ch_common.PathPattern] = pathPattern
	return &quesma_api.Result{Meta: metadata, GenericResult: req.OriginalRequest}
}
