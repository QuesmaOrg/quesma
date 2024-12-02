// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package quesma

import (
	"net/http"
	"quesma/clickhouse"
	"quesma/frontend_connectors"
	"quesma/quesma/mux"
	"quesma/telemetry"
)

type ElasticHttpFrontendConnector struct {
	*frontend_connectors.BasicHTTPFrontendConnector
	routerInstance *routerV2
	searchRouter   *mux.PathRouter
	ingestRouter   *mux.PathRouter
	logManager     *clickhouse.LogManager
	agent          telemetry.PhoneHomeAgent
}

func NewElasticHttpFrontendConnector(endpoint string,
	routerInstance *routerV2,
	searchRouter *mux.PathRouter,
	ingestRouter *mux.PathRouter,
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
	handlerV2(w, req, h.routerInstance, h.searchRouter, h.ingestRouter, h.logManager, h.agent)
}
