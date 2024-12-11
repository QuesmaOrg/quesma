// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package quesma

import (
	"net/http"
	"quesma/clickhouse"
	"quesma/frontend_connectors"
	"quesma/quesma/recovery"
	"quesma/telemetry"
	"quesma_v2/core"
)

type ElasticHttpIngestFrontendConnector struct {
	*frontend_connectors.BasicHTTPFrontendConnector
	routerInstance *frontend_connectors.RouterV2
	router         quesma_api.Router
	logManager     *clickhouse.LogManager
	agent          telemetry.PhoneHomeAgent
}

func NewElasticHttpIngestFrontendConnector(endpoint string,
	routerInstance *frontend_connectors.RouterV2,
	router quesma_api.Router,
	logManager *clickhouse.LogManager,
	agent telemetry.PhoneHomeAgent) *ElasticHttpIngestFrontendConnector {

	return &ElasticHttpIngestFrontendConnector{
		BasicHTTPFrontendConnector: frontend_connectors.NewBasicHTTPFrontendConnector(endpoint),
		routerInstance:             routerInstance,
		router:                     router,

		logManager: logManager,
		agent:      agent,
	}
}

func (h *ElasticHttpIngestFrontendConnector) ServeHTTP(w http.ResponseWriter, req *http.Request) {

	defer recovery.LogPanic()
	reqBody, err := frontend_connectors.PeekBodyV2(req)
	if err != nil {
		http.Error(w, "Error reading request body", http.StatusInternalServerError)
		return
	}

	ua := req.Header.Get("User-Agent")
	h.agent.UserAgentCounters().Add(ua, 1)

	h.routerInstance.Reroute(req.Context(), w, req, reqBody, h.router, h.logManager)
}

type ElasticHttpQueryFrontendConnector struct {
	*frontend_connectors.BasicHTTPFrontendConnector
	routerInstance *frontend_connectors.RouterV2
	router         quesma_api.Router
	logManager     *clickhouse.LogManager
	agent          telemetry.PhoneHomeAgent
}

func NewElasticHttpQueryFrontendConnector(endpoint string,
	routerInstance *frontend_connectors.RouterV2,
	router quesma_api.Router,
	logManager *clickhouse.LogManager,
	agent telemetry.PhoneHomeAgent) *ElasticHttpIngestFrontendConnector {

	return &ElasticHttpIngestFrontendConnector{
		BasicHTTPFrontendConnector: frontend_connectors.NewBasicHTTPFrontendConnector(endpoint),
		routerInstance:             routerInstance,
		router:                     router,
		logManager:                 logManager,
		agent:                      agent,
	}
}

func (h *ElasticHttpQueryFrontendConnector) ServeHTTP(w http.ResponseWriter, req *http.Request) {

	defer recovery.LogPanic()
	reqBody, err := frontend_connectors.PeekBodyV2(req)
	if err != nil {
		http.Error(w, "Error reading request body", http.StatusInternalServerError)
		return
	}

	ua := req.Header.Get("User-Agent")
	h.agent.UserAgentCounters().Add(ua, 1)

	h.routerInstance.Reroute(req.Context(), w, req, reqBody, h.router, h.logManager)
}
