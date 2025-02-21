// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package frontend_connectors

import (
	"bytes"
	"context"
	"github.com/QuesmaOrg/quesma/quesma/clickhouse"
	"github.com/QuesmaOrg/quesma/quesma/quesma/config"
	"github.com/QuesmaOrg/quesma/quesma/schema"
	quesma_api "github.com/QuesmaOrg/quesma/quesma/v2/core"
	"github.com/QuesmaOrg/quesma/quesma/v2/core/diag"
	"io"
	"net/http"
	"sync"
)

type BasicHTTPFrontendConnector struct {
	listener        *http.Server
	router          quesma_api.Router
	mutex           sync.Mutex
	responseMutator func(w http.ResponseWriter) http.ResponseWriter
	endpoint        string
	dispatcher      *Dispatcher
	logManager      *clickhouse.LogManager
	registry        schema.Registry
	config          *config.QuesmaConfiguration

	phoneHomeClient    diag.PhoneHomeClient
	debugInfoCollector diag.DebugInfoCollector
	logger             quesma_api.QuesmaLogger
	middlewares        []http.Handler
}

func (h *BasicHTTPFrontendConnector) GetChildComponents() []interface{} {
	components := make([]interface{}, 0)

	if h.router != nil {
		components = append(components, h.router)
	}

	if h.dispatcher != nil {
		components = append(components, h.dispatcher)
	}
	return components
}

func (h *BasicHTTPFrontendConnector) SetDependencies(deps quesma_api.Dependencies) {
	h.phoneHomeClient = deps.PhoneHomeAgent()
	h.debugInfoCollector = deps.DebugInfoCollector()
	h.logger = deps.Logger()

	deps.PhoneHomeAgent().FailedRequestsCollector(func() int64 {
		return h.dispatcher.FailedRequests.Load()
	})
}

func NewBasicHTTPFrontendConnector(endpoint string, config *config.QuesmaConfiguration) *BasicHTTPFrontendConnector {

	return &BasicHTTPFrontendConnector{
		endpoint:   endpoint,
		config:     config,
		dispatcher: NewDispatcher(config),
		logManager: nil,
		registry:   nil,
		responseMutator: func(w http.ResponseWriter) http.ResponseWriter {
			return w
		},
		middlewares: make([]http.Handler, 0),
	}
}

func (h *BasicHTTPFrontendConnector) InstanceName() string {
	return "BasicHTTPFrontendConnector" // TODO return name from config
}

func (h *BasicHTTPFrontendConnector) AddRouter(router quesma_api.Router) {
	h.router = router
}

func (h *BasicHTTPFrontendConnector) GetRouter() quesma_api.Router {
	return h.router
}

type ResponseWriterWithStatusCode struct {
	http.ResponseWriter
	statusCode int
}

func (w *ResponseWriterWithStatusCode) WriteHeader(statusCode int) {
	w.statusCode = statusCode
	w.ResponseWriter.WriteHeader(statusCode)
}

func (h *BasicHTTPFrontendConnector) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	index := 0
	var runMiddleware func()

	runMiddleware = func() {
		if index < len(h.middlewares) {
			middleware := h.middlewares[index]
			index++
			responseWriter := &ResponseWriterWithStatusCode{w, 0}
			middleware.ServeHTTP(responseWriter, req) // Automatically proceeds to the next middleware
			// Only if the middleware did not set a status code, we proceed to the next middleware
			if responseWriter.statusCode == 0 {
				runMiddleware()
			}

		} else {
			h.finalHandler(w, req)
		}
	}
	runMiddleware()
}

func (h *BasicHTTPFrontendConnector) finalHandler(w http.ResponseWriter, req *http.Request) {
	reqBody, err := PeekBodyV2(req)
	if err != nil {
		http.Error(w, "Error reading request body", http.StatusInternalServerError)
		return
	}

	ua := req.Header.Get("User-Agent")
	if h.phoneHomeClient != nil {
		h.phoneHomeClient.UserAgentCounters().Add(ua, 1)
	}

	h.dispatcher.Reroute(req.Context(), w, req, reqBody, h.router)
}

func (h *BasicHTTPFrontendConnector) Listen() error {
	h.mutex.Lock()
	defer h.mutex.Unlock()
	if h.listener != nil {
		// TODO handle this gracefully and return correct error
		return nil
	}
	h.listener = &http.Server{

	}
	h.listener.Addr = h.endpoint
	h.listener.Handler = h
	go func() {
		h.logger.Info().Msgf("HTTP server started on %s", h.endpoint)
		err := h.listener.ListenAndServe()
		h.logger.Error().Err(err).Msg("HTTP server stopped")
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

func (h *BasicHTTPFrontendConnector) GetDispatcherInstance() *Dispatcher {
	return h.dispatcher
}

func (h *BasicHTTPFrontendConnector) AddMiddleware(middleware http.Handler) {
	h.middlewares = append(h.middlewares, middleware)
}
