// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package frontend_connectors

import (
	"github.com/stretchr/testify/assert"
	"net/http"
	"reflect"
	"testing"
)

func Test_OsdHeaders(t *testing.T) {
	var request, response http.Header
	request = make(http.Header)
	response = make(http.Header)
	request.Set(osdRequestHeaderKey, osdRequestHeaderValue)
	AddProductAndContentHeaders(request, response)
	assert.NotContains(t, response, elasticSearchResponseHeaderKey)
	assert.Equal(t, "application/json; charset=UTF-8", response.Get(ContentTypeHeaderKey), "Content-Type set correctly")
}

func Test_EsHeaders(t *testing.T) {
	var request, response http.Header
	request = make(http.Header)
	response = make(http.Header)
	AddProductAndContentHeaders(request, response)
	assert.Equal(t, elasticSearchResponseHeaderValue, response.Get(elasticSearchResponseHeaderKey), "X-Elastic-Product set correctly")
	assert.Equalf(t, "application/json; charset=UTF-8", response.Get(ContentTypeHeaderKey), "Content-Type set correctly")
}

func TestFindMissingElasticsearchHeaders(t *testing.T) {
	tests := []struct {
		elasticsearchHeaders http.Header
		quesmaHeaders        http.Header
		expectedMissing      []string
	}{
		{
			elasticsearchHeaders: http.Header{"h1": []string{"v1"}, "h2": []string{"v2"}},
			quesmaHeaders:        http.Header{"h1": []string{"v1"}},
			expectedMissing:      []string{"h2"},
		},
		{
			elasticsearchHeaders: http.Header{"h1": []string{"v1"}, "h2": []string{"v2"}},
			quesmaHeaders:        http.Header{"h1": []string{"v1"}, "h2": []string{"v2"}},
			expectedMissing:      nil,
		},
		{
			elasticsearchHeaders: http.Header{"h1": []string{"v1"}, "h2": []string{"v2"}},
			quesmaHeaders:        http.Header{"h1": []string{"v1"}, "h3": []string{"v3"}},
			expectedMissing:      []string{"h2"},
		},
		{
			elasticsearchHeaders: http.Header{"h1": []string{"v1"}, "h2": []string{"v1"}, "X-Cloud-Request-Id": []string{"v2"}, "X-Found-Handling-Cluster": []string{"v3"}, "X-Found-Handling-Instance": []string{"v4"}},
			quesmaHeaders:        http.Header{"h1": []string{"v1"}, "h3": []string{"v3"}},
			expectedMissing:      []string{"h2"},
		},
		{
			elasticsearchHeaders: http.Header{},
			quesmaHeaders:        http.Header{},
			expectedMissing:      nil,
		},
	}

	for _, test := range tests {
		actualMissing := findMissingElasticsearchHeaders(test.elasticsearchHeaders, test.quesmaHeaders)
		if !reflect.DeepEqual(actualMissing, test.expectedMissing) {
			t.Errorf("For elasticsearchHeaders %v and quesmaHeaders %v, expected missing headers to be %v, but got %v",
				test.elasticsearchHeaders, test.quesmaHeaders, test.expectedMissing, actualMissing)
		}
	}
}
