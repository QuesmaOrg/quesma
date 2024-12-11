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
)

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
				result, _ := handler(context.Background(), &quesma_api.Request{OriginalRequest: req})
				_, err := w.Write(result.GenericResult.([]byte))
				if err != nil {
					fmt.Printf("Error writing response: %s\n", err)
				}
			}
		}).ServeHTTP(w, req)
		return
	}
	http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		result, _ := handlerWrapper.Handler(context.Background(), &quesma_api.Request{OriginalRequest: req})

		_, message := dispatcher.Dispatch(handlerWrapper.Processors, result.Meta, result.GenericResult)
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
