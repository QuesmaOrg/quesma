// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package frontend_connectors

import (
	"context"
	"github.com/QuesmaOrg/quesma/quesma/config"
	"github.com/QuesmaOrg/quesma/quesma/processors/es_to_ch_common"
	quesma_api "github.com/QuesmaOrg/quesma/quesma/v2/core"
	"net/http"
)

type ElasticsearchQueryFrontendConnector struct {
	*BasicHTTPFrontendConnector
}

func NewElasticsearchQueryFrontendConnector(endpoint string, esCfg config.ElasticsearchConfiguration, disableAuth bool) *ElasticsearchQueryFrontendConnector {
	// The esCfg is here only for F/E auth purposes and should probably not be wrapped againa in `config.QuesmaConfiguration`
	basicHttpFrontendConnector := NewBasicHTTPFrontendConnector(endpoint, &config.QuesmaConfiguration{Elasticsearch: esCfg, DisableAuth: disableAuth})
	basicHttpFrontendConnector.responseMutator = func(w http.ResponseWriter) http.ResponseWriter {
		w.Header().Set("Content-Type", "application/json")
		return w
	}
	fc := &ElasticsearchQueryFrontendConnector{
		BasicHTTPFrontendConnector: basicHttpFrontendConnector,
	}
	router := quesma_api.NewPathRouter()

	//TODO: Somehow this messes up the router, so we need to fix it
	//router.Register(IndexPath, quesma_api.IsHTTPMethod("GET"), func(ctx context.Context, req *quesma_api.Request, writer http.ResponseWriter) (*quesma_api.Result, error) {
	//	return &quesma_api.Result{Meta: metadata, GenericResult: req.OriginalRequest}, nil
	//})
	router.Register(es_to_ch_common.IndexSearchPath, quesma_api.IsHTTPMethod("GET", "POST"), func(ctx context.Context, req *quesma_api.Request, writer http.ResponseWriter) (*quesma_api.Result, error) {
		return es_to_ch_common.SetPathPattern(req, es_to_ch_common.IndexSearchPath), nil
	})
	router.Register(es_to_ch_common.IndexAsyncSearchPath, quesma_api.IsHTTPMethod("POST"), func(ctx context.Context, req *quesma_api.Request, writer http.ResponseWriter) (*quesma_api.Result, error) {
		return es_to_ch_common.SetPathPattern(req, es_to_ch_common.IndexAsyncSearchPath), nil
	})
	router.Register(es_to_ch_common.AsyncSearchIdPath, quesma_api.IsHTTPMethod("GET", "DELETE"), func(ctx context.Context, req *quesma_api.Request, writer http.ResponseWriter) (*quesma_api.Result, error) {
		return es_to_ch_common.SetPathPattern(req, es_to_ch_common.AsyncSearchIdPath), nil
	})
	router.Register(es_to_ch_common.AsyncSearchStatusPath, quesma_api.IsHTTPMethod("GET"), func(ctx context.Context, req *quesma_api.Request, writer http.ResponseWriter) (*quesma_api.Result, error) {
		return es_to_ch_common.SetPathPattern(req, es_to_ch_common.AsyncSearchStatusPath), nil
	})
	router.Register(es_to_ch_common.FieldCapsPath, quesma_api.IsHTTPMethod("GET", "POST"), func(ctx context.Context, req *quesma_api.Request, writer http.ResponseWriter) (*quesma_api.Result, error) {
		return es_to_ch_common.SetPathPattern(req, es_to_ch_common.FieldCapsPath), nil
	})
	router.Register(es_to_ch_common.ResolveIndexPath, quesma_api.IsHTTPMethod("GET"), func(ctx context.Context, req *quesma_api.Request, writer http.ResponseWriter) (*quesma_api.Result, error) {
		return es_to_ch_common.SetPathPattern(req, es_to_ch_common.ResolveIndexPath), nil
	})
	router.Register(es_to_ch_common.ClusterHealthPath, quesma_api.IsHTTPMethod("GET"), func(ctx context.Context, req *quesma_api.Request, writer http.ResponseWriter) (*quesma_api.Result, error) {
		return es_to_ch_common.SetPathPattern(req, es_to_ch_common.ClusterHealthPath), nil
	})
	router.Register(es_to_ch_common.IndexRefreshPath, quesma_api.IsHTTPMethod("POST"), func(ctx context.Context, req *quesma_api.Request, writer http.ResponseWriter) (*quesma_api.Result, error) {
		return es_to_ch_common.SetPathPattern(req, es_to_ch_common.IndexRefreshPath), nil
	})
	router.Register(es_to_ch_common.IndexMappingPath, quesma_api.IsHTTPMethod("GET"), func(ctx context.Context, req *quesma_api.Request, writer http.ResponseWriter) (*quesma_api.Result, error) {
		return es_to_ch_common.SetPathPattern(req, es_to_ch_common.IndexMappingPath), nil
	})
	router.Register(es_to_ch_common.TermsEnumPath, quesma_api.IsHTTPMethod("POST"), func(ctx context.Context, req *quesma_api.Request, writer http.ResponseWriter) (*quesma_api.Result, error) {
		return es_to_ch_common.SetPathPattern(req, es_to_ch_common.TermsEnumPath), nil
	})
	router.Register(es_to_ch_common.IndexCountPath, quesma_api.IsHTTPMethod("GET"), func(ctx context.Context, req *quesma_api.Request, writer http.ResponseWriter) (*quesma_api.Result, error) {
		return es_to_ch_common.SetPathPattern(req, es_to_ch_common.IndexCountPath), nil
	})

	router.Register("*", quesma_api.Always(), func(ctx context.Context, req *quesma_api.Request, writer http.ResponseWriter) (*quesma_api.Result, error) {
		metadata := quesma_api.MakeNewMetadata()
		metadata[es_to_ch_common.Bypass] = true
		return &quesma_api.Result{Meta: metadata, GenericResult: req.OriginalRequest}, nil
	})

	fc.AddRouter(router)
	return fc
}
