package mux

import (
	"context"
	"github.com/stretchr/testify/assert"
	"net/http"
	"net/url"
	"testing"
)

func TestPathRouter_Matches_ShouldIgnoreTrailingSlash(t *testing.T) {
	router := NewPathRouter()
	router.RegisterPath("/:index/_bulk", "POST", mockHandler)
	router.RegisterPath("/:index/_doc", "POST", mockHandler)
	router.RegisterPath("/:index/_count", "GET", mockHandler)

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
			_, _, found := router.Matches(tt.path, tt.httpMethod, tt.body)
			assert.Equalf(t, tt.want, found, "Matches(%v, %v, %v)", tt.path, tt.httpMethod, tt.body)
		})
	}
}

func TestShouldMatchMultipleHttpMethods(t *testing.T) {
	router := NewPathRouter()
	router.RegisterPathMatcher("/:index/_bulk", []string{"POST", "GET"}, always, mockHandler)

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
			_, _, found := router.Matches(tt.path, tt.httpMethod, tt.body)
			assert.Equalf(t, tt.want, found, "Matches(%v, %v, %v)", tt.path, tt.httpMethod, tt.body)
		})
	}
}

func always(_ map[string]string, _ string) bool {
	return true
}

func mockHandler(_ context.Context, _, _ string, _ map[string]string, _ http.Header, _ url.Values) (*Result, error) {
	return &Result{}, nil
}
