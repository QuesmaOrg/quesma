package quesma

import (
	"net/http"
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
)

func addProductAndContentHeaders(request http.Header, response http.Header) {
	if request.Get(osdRequestHeaderKey) == osdRequestHeaderValue {
		response.Set(contentTypeHeaderKey, "application/json; charset=UTF-8")
	} else {
		response.Set(elasticSearchResponseHeaderKey, elasticSearchResponseHeaderValue)
		response.Set(contentTypeHeaderKey, "application/vnd.elasticsearch+json;compatible-with=8")
	}
	response.Set(opaqueIdHeaderKey, "unknownId")
}
