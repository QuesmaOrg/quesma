// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package mux

import (
	"context"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestPathRouter_Matches_ShouldIgnoreTrailingSlash(t *testing.T) {
	router := NewPathRouter()
	router.Register("/:index/_bulk", IsHTTPMethod("POST"), mockHandler)
	router.Register("/:index/_doc", IsHTTPMethod("POST"), mockHandler)
	router.Register("/:index/_count", IsHTTPMethod("GET"), mockHandler)

	tests := []struct {
		path       string
		httpMethod string
		body       string
		want       bool
	}{
		{path: "/i1,i2/_count", httpMethod: "GET", body: "", want: true},
		{path: "/i1,i2/_count", httpMethod: "POST", body: "", want: false},
		{path: "/_all/_count/", httpMethod: "GET", body: "", want: true},
		{path: "/_all/_count/", httpMethod: "PUT", body: "", want: false},
		{path: "/index1/_doc", httpMethod: "POST", body: "", want: true},
		{path: "/index1/_doc", httpMethod: "GET", body: "", want: false},
		{path: "/index2/_doc/", httpMethod: "POST", body: "", want: true},
		{path: "/indexABC/_bulk", httpMethod: "GET", body: "", want: false},
		{path: "/indexABC/_bulk/", httpMethod: "POST", body: "", want: true},
	}
	for _, tt := range tests {
		t.Run(tt.httpMethod+" "+tt.path, func(t *testing.T) {

			req := toRequest(tt.path, tt.httpMethod, tt.body)
			_, found, _ := router.Matches(req)
			assert.Equalf(t, tt.want, found, "Matches(%v, %v, %v)", tt.path, tt.httpMethod, tt.body)
		})
	}

}

func TestShouldMatchMultipleHttpMethods(t *testing.T) {
	router := NewPathRouter()
	router.Register("/:index/_bulk", IsHTTPMethod("POST", "GET"), mockHandler)

	tests := []struct {
		path       string
		httpMethod string
		body       string
		want       bool
	}{
		{path: "/index1/_bulk", httpMethod: "POST", body: "", want: true},
		{path: "/index1/_bulk", httpMethod: "GET", body: "", want: true},
		{path: "/index1/_bulk", httpMethod: "PUT", body: "", want: false},
		{path: "/index1/_bulk", httpMethod: "DELETE", body: "", want: false},
	}
	for _, tt := range tests {
		t.Run(tt.httpMethod+" "+tt.path, func(t *testing.T) {

			req := toRequest(tt.path, tt.httpMethod, tt.body)

			_, found, _ := router.Matches(req)
			assert.Equalf(t, tt.want, found, "Matches(%v, %v, %v)", tt.path, tt.httpMethod, tt.body)
		})
	}
}

func toRequest(path, method string, body string) *Request {
	return &Request{
		Path:   path,
		Method: method,
		Body:   body,
	}
}

func mockHandler(_ context.Context, _ *Request) (*Result, error) {
	return &Result{}, nil
}
