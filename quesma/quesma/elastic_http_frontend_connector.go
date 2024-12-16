// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package quesma

import (
	"context"
	"net/http"
	"quesma/clickhouse"
	"quesma/frontend_connectors"
	"quesma/quesma/config"
	"quesma/quesma/recovery"
	"quesma/schema"
	quesma_api "quesma_v2/core"
	"quesma_v2/core/diag"
)

type ElasticHttpIngestFrontendConnector struct {
	*frontend_connectors.BasicHTTPFrontendConnector
	routerInstance *frontend_connectors.RouterV2
	logManager     *clickhouse.LogManager
	registry       schema.Registry
	Config         *config.QuesmaConfiguration
	diagnostic     diag.Diagnostic
}

func NewElasticHttpIngestFrontendConnector(endpoint string,
	logManager *clickhouse.LogManager,
	registry schema.Registry,
	config *config.QuesmaConfiguration, router quesma_api.Router) *ElasticHttpIngestFrontendConnector {

	routerInstance := frontend_connectors.NewRouterV2(config)
	fallback := func(ctx context.Context, req *quesma_api.Request, writer http.ResponseWriter) (*quesma_api.Result, error) {
		routerInstance.ElasticFallback(req.Decision, ctx, writer, req.OriginalRequest, []byte(req.Body), logManager, registry)
		return nil, nil
	}
	fc := &ElasticHttpIngestFrontendConnector{
		BasicHTTPFrontendConnector: frontend_connectors.NewBasicHTTPFrontendConnector(endpoint, config),
		routerInstance:             routerInstance,
		logManager:                 logManager,
		registry:                   registry,
	}
	_ = fallback
	router.AddFallbackHandler(fallback)
	fc.AddRouter(router)

	return fc
}

func (h *ElasticHttpIngestFrontendConnector) InjectDiagnostic(diagnostic diag.Diagnostic) {
	h.diagnostic = diagnostic
	// TODO this is a hack
	h.BasicHTTPFrontendConnector.InjectDiagnostic(diagnostic)
	h.routerInstance.InjectDiagnostic(diagnostic)
}

func serveHTTPHelper(w http.ResponseWriter, req *http.Request,
	routerInstance *frontend_connectors.RouterV2,
	pathRouter quesma_api.Router,
	agent diag.PhoneHomeClient,
	logManager *clickhouse.LogManager,
	registry schema.Registry) {
	defer recovery.LogPanic()
	reqBody, err := frontend_connectors.PeekBodyV2(req)
	if err != nil {
		http.Error(w, "Error reading request body", http.StatusInternalServerError)
		return
	}

	ua := req.Header.Get("User-Agent")
	agent.UserAgentCounters().Add(ua, 1)

	routerInstance.Reroute(req.Context(), w, req, reqBody, pathRouter, logManager, registry)
}

func (h *ElasticHttpIngestFrontendConnector) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	serveHTTPHelper(w, req, h.routerInstance, h.GetRouter(), h.diagnostic.PhoneHomeAgent(), h.logManager, h.registry)
}

type ElasticHttpQueryFrontendConnector struct {
	*frontend_connectors.BasicHTTPFrontendConnector
	routerInstance *frontend_connectors.RouterV2
	logManager     *clickhouse.LogManager
	registry       schema.Registry
	diagnostic     diag.Diagnostic
}

func NewElasticHttpQueryFrontendConnector(endpoint string,
	logManager *clickhouse.LogManager,
	registry schema.Registry,
	config *config.QuesmaConfiguration, router quesma_api.Router) *ElasticHttpIngestFrontendConnector {
	routerInstance := frontend_connectors.NewRouterV2(config)
	fallback := func(ctx context.Context, req *quesma_api.Request, writer http.ResponseWriter) (*quesma_api.Result, error) {
		routerInstance.ElasticFallback(req.Decision, ctx, writer, req.OriginalRequest, []byte(req.Body), logManager, registry)
		return nil, nil
	}
	_ = fallback
	fc := &ElasticHttpIngestFrontendConnector{
		BasicHTTPFrontendConnector: frontend_connectors.NewBasicHTTPFrontendConnector(endpoint, config),
		routerInstance:             frontend_connectors.NewRouterV2(config),
		logManager:                 logManager,
		registry:                   registry,
	}
	router.AddFallbackHandler(fallback)
	fc.AddRouter(router)
	return fc
}

func (h *ElasticHttpQueryFrontendConnector) InjectDiagnostic(diagnostic diag.Diagnostic) {
	h.diagnostic = diagnostic
	// TODO this is a hack
	h.BasicHTTPFrontendConnector.InjectDiagnostic(diagnostic)
	h.routerInstance.InjectDiagnostic(diagnostic)
}

func (h *ElasticHttpQueryFrontendConnector) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	serveHTTPHelper(w, req, h.routerInstance, h.GetRouter(), h.diagnostic.PhoneHomeAgent(), h.logManager, h.registry)
}
