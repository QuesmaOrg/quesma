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
	Config     *config.QuesmaConfiguration
	diagnostic diag.Diagnostic
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

func (h *ElasticHttpIngestFrontendConnector) InjectDiagnostic(diagnostic diag.Diagnostic) {
	h.diagnostic = diagnostic
	// TODO this is a hack
	h.BasicHTTPFrontendConnector.InjectDiagnostic(diagnostic)
}

type ElasticHttpQueryFrontendConnector struct {
	*frontend_connectors.BasicHTTPFrontendConnector
	diagnostic diag.Diagnostic
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

func (h *ElasticHttpQueryFrontendConnector) InjectDiagnostic(diagnostic diag.Diagnostic) {
	h.diagnostic = diagnostic
	// TODO this is a hack
	h.BasicHTTPFrontendConnector.InjectDiagnostic(diagnostic)
}
