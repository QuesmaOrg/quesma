// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package frontend_connectors

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"quesma/quesma/recovery"
	quesma_api "quesma_v2/core"
	"strings"
	"sync"
)

type BasicHTTPFrontendConnector struct {
	listener        *http.Server
	router          quesma_api.Router
	mutex           sync.Mutex
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
	defer recovery.LogPanic()
	reqBody, err := PeekBodyV2(req)
	if err != nil {
		http.Error(w, "Error reading request body", http.StatusInternalServerError)
		return
	}

	quesmaRequest := &quesma_api.Request{
		Method:      req.Method,
		Path:        strings.TrimSuffix(req.URL.Path, "/"),
		Params:      map[string]string{},
		Headers:     req.Header,
		QueryParams: req.URL.Query(),
		Body:        string(reqBody),
	}
	handlersPipe, decision := h.router.Matches(quesmaRequest)
	if decision != nil {
		w.Header().Set(QuesmaTableResolverHeader, decision.String())
	} else {
		w.Header().Set(QuesmaTableResolverHeader, "n/a")
	}
	dispatcher := &quesma_api.Dispatcher{}
	w = h.responseMutator(w)
	if handlersPipe == nil {
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
		result, _ := handlersPipe.Handler(context.Background(), &quesma_api.Request{OriginalRequest: req})

		metadata, message := dispatcher.Dispatch(handlersPipe.Processors, result.Meta, result.GenericResult)
		result = &quesma_api.Result{
			Body:          result.Body,
			Meta:          metadata,
			StatusCode:    result.StatusCode,
			GenericResult: message,
		}
		zip := strings.Contains(req.Header.Get("Accept-Encoding"), "gzip")
		_ = zip
		if err == nil {
			AddProductAndContentHeaders(req.Header, w.Header())
		} else {

		}
		_, err := w.Write(result.GenericResult.([]byte))
		if err != nil {
			fmt.Printf("Error writing response: %s\n", err)
		}
	}).ServeHTTP(w, req)
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
