package quesma

import (
	"context"
	"mitmproxy/quesma/quesma/mux"
	"net/http"
)

type (
	BypassChain struct {
		Bypasses []Bypass
	}
	Bypass interface {
		Applies(req HttpRequest) bool
		Execute(ctx context.Context, req HttpRequest, header http.Header) (*mux.Result, error)
	}
	HttpRequest struct {
		Body    string
		Path    string
		Method  string
		Headers http.Header
	}
)

func NewBypassChain(bypasses ...Bypass) *BypassChain {
	list := make([]Bypass, 0)
	list = append(list, bypasses...)
	return &BypassChain{Bypasses: list}
}
