// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package mux

import (
	"context"
	"github.com/ucarion/urlpath"
	"net/http"
	"net/url"
	"quesma/logger"
	"quesma/quesma/types"
	"strings"
)

type (
	PathRouter struct {
		mappings []mapping
	}
	mapping struct {
		pattern      string
		compiledPath urlpath.Path
		predicate    RequestMatcher
		handler      Handler
	}
	Result struct {
		Body       string
		Meta       map[string]string
		StatusCode int
	}

	Request struct {
		Method string
		Path   string
		Params map[string]string

		Headers     http.Header
		QueryParams url.Values

		Body       string
		ParsedBody types.RequestBody
	}

	Handler func(ctx context.Context, req *Request) (*Result, error)

	RequestMatcher interface {
		Matches(req *Request) bool
	}
)

type RequestMatcherFunc func(req *Request) bool

func ServerErrorResult() *Result {
	return &Result{
		StatusCode: http.StatusInternalServerError,
		Meta:       map[string]string{"Content-queryType": "text/plain"},
	}
}

func BadReqeustResult() *Result {
	return &Result{
		StatusCode: http.StatusBadRequest,
		Meta:       map[string]string{"Content-queryType": "text/plain"},
	}
}

func (f RequestMatcherFunc) Matches(req *Request) bool {
	return f(req)
}

// Url router where you can register multiple URL paths with handler.
// We need our own component as default libraries caused side-effects on requests or response.
// The pattern syntax is based on ucarion/urlpath project. e.g. "/shelves/:shelf/books/:book"
func NewPathRouter() *PathRouter {
	return &PathRouter{mappings: make([]mapping, 0)}
}

func (p *PathRouter) Register(pattern string, predicate RequestMatcher, handler Handler) {

	mapping := mapping{pattern, urlpath.New(pattern), predicate, handler}
	p.mappings = append(p.mappings, mapping)

}

func (p *PathRouter) Matches(req *Request) (Handler, bool) {
	handler, found := p.findHandler(req)
	if found {
		routerStatistics.addMatched(req.Path)
		logger.Debug().Msgf("Matched path: %s", req.Path)
		return handler, true
	} else {
		routerStatistics.addUnmatched(req.Path)
		logger.Debug().Msgf("Non-matched path: %s", req.Path)
		return handler, false
	}
}

func (p *PathRouter) findHandler(req *Request) (Handler, bool) {
	path := strings.TrimSuffix(req.Path, "/")
	for _, m := range p.mappings {
		meta, match := m.compiledPath.Match(path)

		if match {
			req.Params = meta.Params
			predicateResult := m.predicate.Matches(req)

			if predicateResult {
				return m.handler, true
			}
		}
	}
	return nil, false
}

type httpMethodPredicate struct {
	methods []string
}

func (p *httpMethodPredicate) Matches(req *Request) bool {

	for _, method := range p.methods {
		if method == req.Method {
			return true
		}
	}
	return false
}

func IsHTTPMethod(methods ...string) RequestMatcher {
	return &httpMethodPredicate{methods}
}

type predicateAnd struct {
	predicates []RequestMatcher
}

func (p *predicateAnd) Matches(req *Request) bool {
	for _, predicate := range p.predicates {
		if !predicate.Matches(req) {
			return false
		}
	}
	return true
}

func And(predicates ...RequestMatcher) RequestMatcher {
	return &predicateAnd{predicates}
}
