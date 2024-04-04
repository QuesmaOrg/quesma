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

	assert.True(t, router.Matches("/i1,i2/_count", "GET", ""))
	assert.True(t, router.Matches("/_all/_count/", "GET", ""))

	assert.True(t, router.Matches("/index1/_doc", "POST", ""))
	assert.True(t, router.Matches("/index2/_doc/", "POST", ""))

	assert.True(t, router.Matches("/indexABC/_bulk", "POST", ""))
	assert.True(t, router.Matches("/indexABC/_bulk/", "POST", ""))

}

func mockHandler(ctx context.Context, body, uri string, params map[string]string) (*Result, error) {
	return &Result{}, nil
}
