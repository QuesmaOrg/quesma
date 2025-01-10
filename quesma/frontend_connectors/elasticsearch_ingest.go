// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package frontend_connectors

import (
	"context"
	"net/http"
	"quesma/processors/es_to_ch_common"
	quesma_api "quesma_v2/core"
)

type ElasticsearchIngestFrontendConnector struct {
	BasicHTTPFrontendConnector
}

func NewElasticsearchIngestFrontendConnector(endpoint string) *ElasticsearchIngestFrontendConnector {
	fc := &ElasticsearchIngestFrontendConnector{
		BasicHTTPFrontendConnector: BasicHTTPFrontendConnector{
			endpoint:        endpoint,
			responseMutator: setContentType,
		},
	}
	router := quesma_api.NewPathRouter()
	router.Register(es_to_ch_common.IndexBulkPath, quesma_api.IsHTTPMethod("POST", "PUT"), func(ctx context.Context, req *quesma_api.Request, writer http.ResponseWriter) (*quesma_api.Result, error) {
		return es_to_ch_common.SetPathPattern(req, es_to_ch_common.IndexBulkPath), nil
	})
	router.Register(es_to_ch_common.BulkPath, quesma_api.IsHTTPMethod("POST", "PUT"), func(ctx context.Context, req *quesma_api.Request, writer http.ResponseWriter) (*quesma_api.Result, error) {
		return es_to_ch_common.SetPathPattern(req, es_to_ch_common.BulkPath), nil
	})
	router.Register(es_to_ch_common.IndexMappingPath, quesma_api.IsHTTPMethod("PUT"), func(ctx context.Context, req *quesma_api.Request, writer http.ResponseWriter) (*quesma_api.Result, error) {
		return es_to_ch_common.SetPathPattern(req, es_to_ch_common.IndexMappingPath), nil
	})
	router.Register(es_to_ch_common.IndexDocPath, quesma_api.IsHTTPMethod("POST"), func(ctx context.Context, req *quesma_api.Request, writer http.ResponseWriter) (*quesma_api.Result, error) {
		return es_to_ch_common.SetPathPattern(req, es_to_ch_common.IndexDocPath), nil
	})
	fc.AddRouter(router)
	return fc
}

func setContentType(w http.ResponseWriter) http.ResponseWriter {
	w.Header().Set("Content-Type", "application/json")
	return w
}
