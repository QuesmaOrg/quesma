// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package quesma

import (
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

	phoneHomeClient diag.PhoneHomeClient
}

func NewElasticHttpIngestFrontendConnector(endpoint string,
	logManager *clickhouse.LogManager,
	registry schema.Registry,
	config *config.QuesmaConfiguration) *ElasticHttpIngestFrontendConnector {

	return &ElasticHttpIngestFrontendConnector{
		BasicHTTPFrontendConnector: frontend_connectors.NewBasicHTTPFrontendConnector(endpoint, config),
		routerInstance:             frontend_connectors.NewRouterV2(config),
		logManager:                 logManager,
		registry:                   registry,
	}
}

func (h *ElasticHttpIngestFrontendConnector) GetChildComponents() []interface{} {
	components := make([]interface{}, 0)

	if h.routerInstance != nil {
		components = append(components, h.routerInstance)
	}

	if h.BasicHTTPFrontendConnector != nil {
		components = append(components, h.BasicHTTPFrontendConnector)
	}

	return components
}

func (h *ElasticHttpIngestFrontendConnector) SetDependencies(deps quesma_api.Dependencies) {
	h.phoneHomeClient = deps.PhoneHomeAgent()
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
	serveHTTPHelper(w, req, h.routerInstance, h.GetRouter(), h.phoneHomeClient, h.logManager, h.registry)
}

type ElasticHttpQueryFrontendConnector struct {
	*frontend_connectors.BasicHTTPFrontendConnector
	routerInstance  *frontend_connectors.RouterV2
	logManager      *clickhouse.LogManager
	registry        schema.Registry
	phoneHomeClient diag.PhoneHomeClient
}

func NewElasticHttpQueryFrontendConnector(endpoint string,
	logManager *clickhouse.LogManager,
	registry schema.Registry,
	config *config.QuesmaConfiguration) *ElasticHttpIngestFrontendConnector {

	return &ElasticHttpIngestFrontendConnector{
		BasicHTTPFrontendConnector: frontend_connectors.NewBasicHTTPFrontendConnector(endpoint, config),
		routerInstance:             frontend_connectors.NewRouterV2(config),
		logManager:                 logManager,
		registry:                   registry,
	}
}

func (h *ElasticHttpQueryFrontendConnector) GetChildComponents() []interface{} {
	components := make([]interface{}, 0)

	if h.routerInstance != nil {
		components = append(components, h.routerInstance)
	}

	if h.BasicHTTPFrontendConnector != nil {
		components = append(components, h.BasicHTTPFrontendConnector)
	}
	return components
}

func (h *ElasticHttpQueryFrontendConnector) SetDependencies(deps quesma_api.Dependencies) {
	h.phoneHomeClient = deps.PhoneHomeAgent()
}

func (h *ElasticHttpQueryFrontendConnector) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	serveHTTPHelper(w, req, h.routerInstance, h.GetRouter(), h.phoneHomeClient, h.logManager, h.registry)
}
