package mux

import (
	"context"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestMatches_ShouldIgnoreTrailingSlash(t *testing.T) {
	router := NewPathRouter()
	router.RegisterPath("/:index/_bulk", "POST", mockHandler)
	router.RegisterPath("/:index/_doc", "POST", mockHandler)
	router.RegisterPath("/:index/_count", "GET", mockHandler)

	assert.True(t, router.Matches(toRequest("/i1,i2/_count", "GET", "")))
	assert.True(t, router.Matches(toRequest("/_all/_count/", "GET", "")))
	assert.True(t, router.Matches(toRequest("/index1/_doc", "POST", "")))
	assert.True(t, router.Matches(toRequest("/index2/_doc/", "POST", "")))
	assert.True(t, router.Matches(toRequest("/indexABC/_bulk", "POST", "")))
	assert.True(t, router.Matches(toRequest("/indexABC/_bulk/", "POST", "")))
}

func TestShouldMatchMultipleHttpMethods(t *testing.T) {
	router := NewPathRouter()
	router.Register("/:index/_bulk", IsHTTPMethod("POST", "GET"), mockHandler)

	assert.True(t, router.Matches(toRequest("/index1/_bulk", "POST", "")))
	assert.True(t, router.Matches(toRequest("/index1/_bulk", "GET", "")))
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
