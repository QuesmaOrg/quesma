// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package frontend_connectors

import (
	"context"
	"fmt"
	"github.com/ucarion/urlpath"
	"net/http"
	quesma_api "quesma_v2/core"
)

type ElasticsearchIngestFrontendConnector struct {
	BasicHTTPFrontendConnector
}

const (
	IndexDocPath  = "/:index/_doc"
	IndexBulkPath = "/:index/_bulk"

	// IngestAction and below are metadata items passed to processor.
	IngestAction    = "ingest_action"
	DocIndexAction  = "_doc"
	BulkIndexAction = "_bulk"
	IngestTargetKey = "ingest_target"
	// TODO: this actually should not be a dependency on processor
)

func NewElasticsearchIngestFrontendConnector(endpoint string) *ElasticsearchIngestFrontendConnector {
	fc := &ElasticsearchIngestFrontendConnector{
		BasicHTTPFrontendConnector: BasicHTTPFrontendConnector{
			endpoint: endpoint,
		},
	}
	router := NewHTTPRouter()
	router.AddRoute(IndexBulkPath, bulk)
	router.AddRoute(IndexDocPath, doc)
	fc.AddRouter(router)
	return fc
}

func (h *ElasticsearchIngestFrontendConnector) AddRouter(router quesma_api.Router) {
	h.router = router
}

func (h *ElasticsearchIngestFrontendConnector) GetRouter() quesma_api.Router {
	return h.router
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

func (h *ElasticsearchIngestFrontendConnector) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	handlers := h.router.GetHandlers()
	handlerWrapper := getMatchingHandler(req.URL.Path, handlers)
	if handlerWrapper == nil {
		h.router.Multiplexer().ServeHTTP(w, req)
		return
	}
	dispatcher := &quesma_api.Dispatcher{}

	// for the response out we are Elasticsearch-7 compliant
	w.Header().Set("Content-Type", "application/json")
	http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		metadata, message, _ := handlerWrapper.Handler(req)
		req.Header.Set("x-przemek", "blah")
		_, message = dispatcher.Dispatch(handlerWrapper.Processors, metadata, message)
		_, err := w.Write(message.([]byte))
		if err != nil {
			fmt.Printf("Error writing response: %s\n", err)
		}
	}).ServeHTTP(w, req)
}

func (h *ElasticsearchIngestFrontendConnector) Listen() error {
	h.listener = &http.Server{}
	h.listener.Addr = h.endpoint
	h.listener.Handler = h
	go func() {
		err := h.listener.ListenAndServe()
		_ = err
	}()

	return nil
}

func (h *ElasticsearchIngestFrontendConnector) Stop(ctx context.Context) error {
	if h.listener == nil {
		return nil
	}
	err := h.listener.Shutdown(ctx)
	if err != nil {
		return err
	}
	return h.listener.Close()
}

func (h *ElasticsearchIngestFrontendConnector) GetEndpoint() string {
	return h.endpoint
}

func bulk(request *http.Request) (map[string]interface{}, any, error) {
	//body, err := ReadRequestBody(request)
	//if err != nil {
	//	return nil, nil, err
	//}
	metadata := quesma_api.MakeNewMetadata()
	metadata[IngestAction] = BulkIndexAction
	metadata[IngestTargetKey] = getIndexFromRequest(request)
	return metadata, request, nil
}

func doc(request *http.Request) (map[string]interface{}, any, error) {
	//body, err := ReadRequestBody(request)
	//if err != nil {
	//	return nil, nil, err
	//}
	metadata := quesma_api.MakeNewMetadata()
	metadata[IngestAction] = DocIndexAction
	metadata[IngestTargetKey] = getIndexFromRequest(request)
	return metadata, request, nil
}

func getIndexFromRequest(request *http.Request) string {
	expectedUrl := urlpath.New("/:index/*")
	match, _ := expectedUrl.Match(request.URL.Path) // safe to call at this level
	return match.Params["index"]
}
