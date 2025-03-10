// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package frontend_connectors

import (
	"net/http"
	"slices"
)

const (
	ContentTypeHeaderKey             = "Content-Type"
	osdRequestHeaderKey              = "x-opensearch-product-origin"
	osdRequestHeaderValue            = "opensearch-dashboards"
	elasticSearchResponseHeaderKey   = "X-Elastic-Product"
	elasticSearchResponseHeaderValue = "Elasticsearch"
	OpaqueIdHeaderKey                = "X-Opaque-Id"

	HttpHeaderContentLength = "Content-Length"

	QuesmaSourceHeader     = "X-Quesma-Source"
	QuesmaSourceElastic    = "Elasticsearch"
	QuesmaSourceClickhouse = "Clickhouse"

	QuesmaTableResolverHeader = "X-Quesma-Table-Resolver"
)

// Certain Elasticsearch SaaS providers might add custom headers to the response,
// which should be ignored when comparing Quesma response with Elasticsearch response.
var ignoredElasticsearchHeaders = []string{
	"X-Cloud-Request-Id", "X-Found-Handling-Cluster", "X-Found-Handling-Instance", "Www-Authenticate", "Date", // Elastic Cloud
}

func AddProductAndContentHeaders(request http.Header, response http.Header) {
	if request.Get(osdRequestHeaderKey) == osdRequestHeaderValue {
		response.Set(ContentTypeHeaderKey, "application/json; charset=UTF-8")
	} else {
		response.Set(elasticSearchResponseHeaderKey, elasticSearchResponseHeaderValue)
		//response.Set(contentTypeHeaderKey, "application/vnd.elasticsearch+json;compatible-with=8")
	}
	// WARNING:
	// Elasticsearch 8.x responds with `Content-Type: application/vnd.elasticsearch+json;compatible-with=8`
	// Elasticsearch 7.x responds with `Content-Type: application/json; charset=UTF-8`
	//     We decided to always use the 7.x response for now, but we might need to change it in the future.
	//     Specifically, we might need to change this behaviour by introducing Elasticsearch 8 and Elasticsearch 7-specific frontend connectors.
	// 	   More in: https://github.com/QuesmaOrg/quesma/issues/994
	response.Set(ContentTypeHeaderKey, "application/json; charset=UTF-8")
	response.Set(OpaqueIdHeaderKey, "unknownId")
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
