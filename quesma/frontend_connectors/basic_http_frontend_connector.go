// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package frontend_connectors

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"quesma/clickhouse"
	"quesma/quesma/config"
	"quesma/schema"
	quesma_api "quesma_v2/core"
	"quesma_v2/core/diag"
	"sync"
)

type BasicHTTPFrontendConnector struct {
	listener        *http.Server
	router          quesma_api.Router
	mutex           sync.Mutex
	responseMutator func(w http.ResponseWriter) http.ResponseWriter
	endpoint        string
	routerInstance  *RouterV2
	logManager      *clickhouse.LogManager
	registry        schema.Registry
	config          *config.QuesmaConfiguration

	phoneHomeClient    diag.PhoneHomeClient
	debugInfoCollector diag.DebugInfoCollector
}

func (h *BasicHTTPFrontendConnector) GetChildComponents() []interface{} {
	components := make([]interface{}, 0)

	if h.router != nil {
		components = append(components, h.router)
	}

	if h.routerInstance != nil {
		components = append(components, h.routerInstance)
	}
	return components
}

func (h *BasicHTTPFrontendConnector) SetDependencies(deps quesma_api.Dependencies) {
	h.phoneHomeClient = deps.PhoneHomeAgent()
	h.debugInfoCollector = deps.DebugInfoCollector()
	deps.PhoneHomeAgent().FailedRequestsCollector(func() int64 {
		return h.routerInstance.FailedRequests.Load()
	})
}

func NewBasicHTTPFrontendConnector(endpoint string, config *config.QuesmaConfiguration) *BasicHTTPFrontendConnector {

	return &BasicHTTPFrontendConnector{
		endpoint:       endpoint,
		config:         config,
		routerInstance: NewRouterV2(config),
		logManager:     nil,
		registry:       nil,
		responseMutator: func(w http.ResponseWriter) http.ResponseWriter {
			return w
		},
	}
}

func (h *BasicHTTPFrontendConnector) AddRouter(router quesma_api.Router) {
	h.router = router
}

func (h *BasicHTTPFrontendConnector) GetRouter() quesma_api.Router {
	return h.router
}

func (h *BasicHTTPFrontendConnector) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	reqBody, err := PeekBodyV2(req)
	if err != nil {
		http.Error(w, "Error reading request body", http.StatusInternalServerError)
		return
	}

	ua := req.Header.Get("User-Agent")
	if h.phoneHomeClient != nil {
		h.phoneHomeClient.UserAgentCounters().Add(ua, 1)
	}

	h.routerInstance.Reroute(req.Context(), w, req, reqBody, h.router, h.logManager, h.registry)
}

func (h *BasicHTTPFrontendConnector) Listen() error {
	h.mutex.Lock()
	defer h.mutex.Unlock()
	if h.listener != nil {
		// TODO handle this gracefully and return correct error
		return nil
	}
	h.listener = &http.Server{}
	h.listener.Addr = h.endpoint
	h.listener.Handler = h
	go func() {
		err := h.listener.ListenAndServe()
		// TODO: Handle error
		_ = err
	}()

	return nil
}

func (h *BasicHTTPFrontendConnector) Stop(ctx context.Context) error {
	h.mutex.Lock()
	defer h.mutex.Unlock()
	if h.listener == nil {
		return nil
	}
	err := h.listener.Shutdown(ctx)
	if err != nil {
		return err
	}
	return h.listener.Close()
}

func (h *BasicHTTPFrontendConnector) GetEndpoint() string {
	return h.endpoint
}

func ReadRequestBody(request *http.Request) ([]byte, error) {
	reqBody, err := io.ReadAll(request.Body)
	if err != nil {
		return nil, err
	}
	request.Body = io.NopCloser(bytes.NewBuffer(reqBody))
	return reqBody, nil
}

func (h *BasicHTTPFrontendConnector) GetRouterInstance() *RouterV2 {
	return h.routerInstance
}
