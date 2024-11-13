// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package quesma

import (
	"net/http"
	"quesma/logger"
	"slices"
)

const (
	contentTypeHeaderKey             = "Content-Type"
	osdRequestHeaderKey              = "x-opensearch-product-origin"
	osdRequestHeaderValue            = "opensearch-dashboards"
	elasticSearchResponseHeaderKey   = "X-Elastic-Product"
	elasticSearchResponseHeaderValue = "Elasticsearch"
	opaqueIdHeaderKey                = "X-Opaque-Id"

	httpHeaderContentLength = "Content-Length"

	quesmaSourceHeader     = "X-Quesma-Source"
	quesmaSourceElastic    = "Elasticsearch"
	quesmaSourceClickhouse = "Clickhouse"

	quesmaTableResolverHeader = "X-Quesma-Table-Resolver"
)

// Certain Elasticsearch SaaS providers might add custom headers to the response,
// which should be ignored when comparing Quesma response with Elasticsearch response.
var ignoredElasticsearchHeaders = []string{
	"X-Cloud-Request-Id", "X-Found-Handling-Cluster", "X-Found-Handling-Instance", "Www-Authenticate", "Date", // Elastic Cloud
}

func addProductAndContentHeaders(request http.Header, response http.Header) {
	if request.Get(osdRequestHeaderKey) == osdRequestHeaderValue {
		response.Set(contentTypeHeaderKey, "application/json; charset=UTF-8")
	} else {
		response.Set(elasticSearchResponseHeaderKey, elasticSearchResponseHeaderValue)
		response.Set(contentTypeHeaderKey, "application/vnd.elasticsearch+json;compatible-with=8")
	}
	response.Set(opaqueIdHeaderKey, "unknownId")
}

func LogMissingEsHeaders(elasticsearchHeaders, quesmaHeaders http.Header, reqId string) {
	missingHeaders := findMissingElasticsearchHeaders(elasticsearchHeaders, quesmaHeaders)
	for _, headerName := range missingHeaders {
		logger.Warn().Str(logger.RID, reqId).Msgf("Header %s is missing in Quesma's response", headerName)
	}
}

func findMissingElasticsearchHeaders(elasticsearchHeaders, quesmaHeaders http.Header) []string {
	var missingHeaders []string
	for esHeaderName := range elasticsearchHeaders {
		if !slices.Contains(ignoredElasticsearchHeaders, esHeaderName) {
			if _, ok := quesmaHeaders[esHeaderName]; !ok {
				missingHeaders = append(missingHeaders, esHeaderName)
			}
		}
	}
	return missingHeaders
}
