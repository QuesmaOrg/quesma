// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package quesma

import (
	"net/http"
	"quesma/clickhouse"
	"quesma/frontend_connectors"
	"quesma/quesma/recovery"
	"quesma/telemetry"
	quesma_api "quesma_v2/core"
)

type ElasticHttpIngestFrontendConnector struct {
	*frontend_connectors.BasicHTTPFrontendConnector
	routerInstance *frontend_connectors.RouterV2
	logManager     *clickhouse.LogManager
	agent          telemetry.PhoneHomeAgent
}

func NewElasticHttpIngestFrontendConnector(endpoint string,
	routerInstance *frontend_connectors.RouterV2,
	logManager *clickhouse.LogManager,
	agent telemetry.PhoneHomeAgent) *ElasticHttpIngestFrontendConnector {

	return &ElasticHttpIngestFrontendConnector{
		BasicHTTPFrontendConnector: frontend_connectors.NewBasicHTTPFrontendConnector(endpoint),
		routerInstance:             routerInstance,
		logManager:                 logManager,
		agent:                      agent,
	}
}

func serveHTTPHelper(w http.ResponseWriter, req *http.Request,
	routerInstance *frontend_connectors.RouterV2,
	pathRouter quesma_api.Router,
	agent telemetry.PhoneHomeAgent,
	logManager *clickhouse.LogManager) {
	defer recovery.LogPanic()
	reqBody, err := frontend_connectors.PeekBodyV2(req)
	if err != nil {
		http.Error(w, "Error reading request body", http.StatusInternalServerError)
		return
	}

	ua := req.Header.Get("User-Agent")
	agent.UserAgentCounters().Add(ua, 1)

	routerInstance.Reroute(req.Context(), w, req, reqBody, pathRouter, logManager)
}

func (h *ElasticHttpIngestFrontendConnector) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	serveHTTPHelper(w, req, h.routerInstance, h.GetRouter(), h.agent, h.logManager)
}

type ElasticHttpQueryFrontendConnector struct {
	*frontend_connectors.BasicHTTPFrontendConnector
	routerInstance *frontend_connectors.RouterV2
	logManager     *clickhouse.LogManager
	agent          telemetry.PhoneHomeAgent
}

func NewElasticHttpQueryFrontendConnector(endpoint string,
	routerInstance *frontend_connectors.RouterV2,
	logManager *clickhouse.LogManager,
	agent telemetry.PhoneHomeAgent) *ElasticHttpIngestFrontendConnector {

	return &ElasticHttpIngestFrontendConnector{
		BasicHTTPFrontendConnector: frontend_connectors.NewBasicHTTPFrontendConnector(endpoint),
		routerInstance:             routerInstance,
		logManager:                 logManager,
		agent:                      agent,
	}
}

func (h *ElasticHttpQueryFrontendConnector) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	serveHTTPHelper(w, req, h.routerInstance, h.GetRouter(), h.agent, h.logManager)
}
