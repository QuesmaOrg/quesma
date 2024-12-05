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

type ElasticHttpFrontendConnector struct {
	*frontend_connectors.BasicHTTPFrontendConnector
	routerInstance *routerV2
	searchRouter   *quesma_api.PathRouter
	ingestRouter   *quesma_api.PathRouter
	logManager     *clickhouse.LogManager
	agent          telemetry.PhoneHomeAgent
}

func NewElasticHttpFrontendConnector(endpoint string,
	routerInstance *routerV2,
	searchRouter *quesma_api.PathRouter,
	ingestRouter *quesma_api.PathRouter,
	logManager *clickhouse.LogManager,
	agent telemetry.PhoneHomeAgent) *ElasticHttpFrontendConnector {
	return &ElasticHttpFrontendConnector{
		BasicHTTPFrontendConnector: frontend_connectors.NewBasicHTTPFrontendConnector(endpoint),
		routerInstance:             routerInstance,
		searchRouter:               searchRouter,
		ingestRouter:               ingestRouter,
		logManager:                 logManager,
		agent:                      agent,
	}
}

func (h *ElasticHttpFrontendConnector) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	defer recovery.LogPanic()
	reqBody, err := peekBodyV2(req)
	if err != nil {
		http.Error(w, "Error reading request body", http.StatusInternalServerError)
		return
	}

	ua := req.Header.Get("User-Agent")
	h.agent.UserAgentCounters().Add(ua, 1)

	h.routerInstance.reroute(req.Context(), w, req, reqBody, h.searchRouter, h.ingestRouter, h.logManager)
}
