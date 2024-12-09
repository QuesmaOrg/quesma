// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package frontend_connectors

import (
	"context"
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
			endpoint:        endpoint,
			responseMutator: setContentType,
		},
	}
	router := NewHTTPRouter()
	router.AddRoute(IndexBulkPath, bulk)
	router.AddRoute(IndexDocPath, doc)
	fc.AddRouter(router)
	return fc
}

func setContentType(w http.ResponseWriter) http.ResponseWriter {
	w.Header().Set("Content-Type", "application/json")
	return w
}

func bulk(_ context.Context, request *http.Request) (map[string]interface{}, any, error) {
	metadata := quesma_api.MakeNewMetadata()
	metadata[IngestAction] = BulkIndexAction
	metadata[IngestTargetKey] = getIndexFromRequest(request)
	return metadata, request, nil
}

func doc(_ context.Context, request *http.Request) (map[string]interface{}, any, error) {
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
