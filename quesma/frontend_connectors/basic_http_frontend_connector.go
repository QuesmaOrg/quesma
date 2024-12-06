// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package frontend_connectors

import (
	"bytes"
	"context"
	"fmt"
	"github.com/ucarion/urlpath"
	"io"
	"net/http"
	quesma_api "quesma_v2/core"
	"sync"
)

type HTTPRouter struct {
	mux             *http.ServeMux                     // Default HTTP multiplexer
	handlers        map[string]quesma_api.HandlersPipe // Map to store custom route handlers
	fallbackHandler quesma_api.HTTPFrontendHandler
	mutex           sync.RWMutex // Mutex for concurrent access to handlers
}

func NewHTTPRouter() *HTTPRouter {
	return &HTTPRouter{
		mux:      http.NewServeMux(),
		handlers: make(map[string]quesma_api.HandlersPipe),
	}
}

// AddRoute adds a new route to the router
func (router *HTTPRouter) AddRoute(path string, handler quesma_api.HTTPFrontendHandler) {
	router.mutex.Lock()
	defer router.mutex.Unlock()
	router.handlers[path] = quesma_api.HandlersPipe{Handler: handler}
	fmt.Printf("Added route: %s\n", path)
}

func (router *HTTPRouter) AddFallbackHandler(handler quesma_api.HTTPFrontendHandler) {
	router.mutex.Lock()
	defer router.mutex.Unlock()
	router.fallbackHandler = handler
}

func (router *HTTPRouter) GetFallbackHandler() quesma_api.HTTPFrontendHandler {
	router.mutex.RLock()
	defer router.mutex.RUnlock()
	return router.fallbackHandler
}

func (router *HTTPRouter) Clone() quesma_api.Cloner {
	newRouter := NewHTTPRouter()
	router.mutex.Lock()
	defer router.mutex.Unlock()
	for path, handler := range router.handlers {
		newRouter.handlers[path] = handler
	}
	newRouter.fallbackHandler = router.fallbackHandler
	return newRouter
}

func (router *HTTPRouter) GetHandlers() map[string]quesma_api.HandlersPipe {
	router.mutex.RLock()
	defer router.mutex.RUnlock()
	callInfos := make(map[string]quesma_api.HandlersPipe)
	for k, v := range router.handlers {
		callInfos[k] = v
	}
	return callInfos
}

func (router *HTTPRouter) SetHandlers(handlers map[string]quesma_api.HandlersPipe) {
	router.mutex.Lock()
	defer router.mutex.Unlock()
	for path, handler := range handlers {
		router.handlers[path] = handler
	}
}

func (router *HTTPRouter) Lock() {
	router.mutex.Lock()
}

func (router *HTTPRouter) Unlock() {
	router.mutex.Unlock()
}

func (router *HTTPRouter) Multiplexer() *http.ServeMux {
	return router.mux
}

func (router *HTTPRouter) Register(pattern string, predicate quesma_api.RequestMatcher, handler quesma_api.Handler) {
	panic("not implemented")
}

func (router *HTTPRouter) Matches(req *quesma_api.Request) (*quesma_api.HttpHandlersPipe, *quesma_api.Decision) {
	panic("not implemented")
}

type BasicHTTPFrontendConnector struct {
	listener *http.Server
	router   quesma_api.Router

	responseMutator func(w http.ResponseWriter) http.ResponseWriter
	endpoint        string
}

func NewBasicHTTPFrontendConnector(endpoint string) *BasicHTTPFrontendConnector {
	return &BasicHTTPFrontendConnector{
		endpoint: endpoint,
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
	handlers := h.router.GetHandlers()
	handlerWrapper := getMatchingHandler(req.URL.Path, handlers)
	dispatcher := &quesma_api.Dispatcher{}
	w = h.responseMutator(w)
	if handlerWrapper == nil {
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if h.router.GetFallbackHandler() != nil {
				fmt.Printf("No handler found for path: %s\n", req.URL.Path)
				handler := h.router.GetFallbackHandler()
				_, message, _ := handler(req)
				_, err := w.Write(message.([]byte))
				if err != nil {
					fmt.Printf("Error writing response: %s\n", err)
				}
			}
		}).ServeHTTP(w, req)
		return
	}
	http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		metadata, message, _ := handlerWrapper.Handler(req)

		_, message = dispatcher.Dispatch(handlerWrapper.Processors, metadata, message)
		_, err := w.Write(message.([]byte))
		if err != nil {
			fmt.Printf("Error writing response: %s\n", err)
		}
	}).ServeHTTP(w, req)
}

func getMatchingHandler(requestPath string, handlers map[string]quesma_api.HandlersPipe) *quesma_api.HandlersPipe {
	for path, handler := range handlers {
		urlPath := urlpath.New(path)
		_, matches := urlPath.Match(requestPath)
		if matches {
			return &handler
		}
	}
	return nil
}

func (h *BasicHTTPFrontendConnector) Listen() error {
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
