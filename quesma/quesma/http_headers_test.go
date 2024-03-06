package quesma

import (
	"github.com/stretchr/testify/assert"
	"net/http"
	"testing"
)

func Test_OsdHeaders(t *testing.T) {
	var request, response http.Header
	request = make(http.Header)
	response = make(http.Header)
	request.Set(osdRequestHeaderKey, osdRequestHeaderValue)
	addProductAndContentHeaders(request, response)
	assert.NotContains(t, response, elasticSearchResponseHeaderKey)
	assert.Equal(t, "application/json; charset=UTF-8", response.Get(contentTypeHeaderKey), "Content-Type set correctly")
}

func Test_EsHeaders(t *testing.T) {
	var request, response http.Header
	request = make(http.Header)
	response = make(http.Header)
	addProductAndContentHeaders(request, response)
	assert.Equal(t, elasticSearchResponseHeaderValue, response.Get(elasticSearchResponseHeaderKey), "X-Elastic-Product set correctly")
	assert.Equalf(t, "application/vnd.elasticsearch+json;compatible-with=8", response.Get(contentTypeHeaderKey), "Content-Type set correctly")
}
