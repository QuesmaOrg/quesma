// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package quesma

import (
	"context"
	"net/http"
	"quesma/clickhouse"
	"quesma/frontend_connectors"
	"quesma/quesma/config"
	"quesma/schema"
	quesma_api "quesma_v2/core"
	"quesma_v2/core/diag"
)

type ElasticHttpIngestFrontendConnector struct {
	*frontend_connectors.BasicHTTPFrontendConnector

	Config *config.QuesmaConfiguration

	phoneHomeClient diag.PhoneHomeClient
}

func NewElasticHttpIngestFrontendConnector(endpoint string,
	logManager *clickhouse.LogManager,
	registry schema.Registry,
	config *config.QuesmaConfiguration, router quesma_api.Router) *ElasticHttpIngestFrontendConnector {

	fc := &ElasticHttpIngestFrontendConnector{
		BasicHTTPFrontendConnector: frontend_connectors.NewBasicHTTPFrontendConnector(endpoint, config),
	}
	fallback := func(ctx context.Context, req *quesma_api.Request, writer http.ResponseWriter) (*quesma_api.Result, error) {
		fc.BasicHTTPFrontendConnector.GetRouterInstance().ElasticFallback(req.Decision, ctx, writer, req.OriginalRequest, []byte(req.Body), logManager, registry)
		return nil, nil
	}

	router.AddFallbackHandler(fallback)
	fc.AddRouter(router)

	return fc
}

func (h *ElasticHttpIngestFrontendConnector) GetChildComponents() []interface{} {
	components := make([]interface{}, 0)
	if h.BasicHTTPFrontendConnector != nil {
		components = append(components, h.BasicHTTPFrontendConnector)
	}

	return components
}

func (h *ElasticHttpIngestFrontendConnector) SetDependencies(deps quesma_api.Dependencies) {
	h.phoneHomeClient = deps.PhoneHomeAgent()
}

type ElasticHttpQueryFrontendConnector struct {
	*frontend_connectors.BasicHTTPFrontendConnector

	phoneHomeClient diag.PhoneHomeClient
}

func NewElasticHttpQueryFrontendConnector(endpoint string,
	logManager *clickhouse.LogManager,
	registry schema.Registry,
	config *config.QuesmaConfiguration, router quesma_api.Router) *ElasticHttpIngestFrontendConnector {

	fc := &ElasticHttpIngestFrontendConnector{
		BasicHTTPFrontendConnector: frontend_connectors.NewBasicHTTPFrontendConnector(endpoint, config),
	}
	fallback := func(ctx context.Context, req *quesma_api.Request, writer http.ResponseWriter) (*quesma_api.Result, error) {
		fc.BasicHTTPFrontendConnector.GetRouterInstance().ElasticFallback(req.Decision, ctx, writer, req.OriginalRequest, []byte(req.Body), logManager, registry)
		return nil, nil
	}
	router.AddFallbackHandler(fallback)
	fc.AddRouter(router)
	return fc
}

func (h *ElasticHttpQueryFrontendConnector) GetChildComponents() []interface{} {
	components := make([]interface{}, 0)
	if h.BasicHTTPFrontendConnector != nil {
		components = append(components, h.BasicHTTPFrontendConnector)
	}
	return components
}

func (h *ElasticHttpQueryFrontendConnector) SetDependencies(deps quesma_api.Dependencies) {
	h.phoneHomeClient = deps.PhoneHomeAgent()
}
