// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package quesma_api

import (
	"context"
	"github.com/ucarion/urlpath"
	"net/http"
	"net/url"
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
		ParsedBody RequestBody
	}

	Handler func(ctx context.Context, req *Request) (*Result, error)

	MatchResult struct {
		Matched  bool
		Decision *Decision
	}
	RequestMatcher interface {
		Matches(req *Request) MatchResult
	}
)

type RequestMatcherFunc func(req *Request) MatchResult

func ServerErrorResult() *Result {
	return &Result{
		StatusCode: http.StatusInternalServerError,
		Meta:       map[string]string{"Content-Type": "text/plain"},
	}
}

func BadReqeustResult() *Result {
	return &Result{
		StatusCode: http.StatusBadRequest,
		Meta:       map[string]string{"Content-Type": "text/plain"},
	}
}

func (f RequestMatcherFunc) Matches(req *Request) MatchResult {
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

func (p *PathRouter) Matches(req *Request) (Handler, *Decision) {
	handler, decision := p.findHandler(req)
	if handler != nil {
		routerStatistics.addMatched(req.Path)
		return handler, decision
	} else {
		routerStatistics.addUnmatched(req.Path)
		return handler, decision
	}
}

func (p *PathRouter) findHandler(req *Request) (Handler, *Decision) {
	path := strings.TrimSuffix(req.Path, "/")
	for _, m := range p.mappings {
		meta, match := m.compiledPath.Match(path)
		if match {
			req.Params = meta.Params
			predicateResult := m.predicate.Matches(req)
			if predicateResult.Matched {
				return m.handler, predicateResult.Decision
			} else {
				return nil, predicateResult.Decision
			}
		}
	}
	return nil, nil
}

type httpMethodPredicate struct {
	methods []string
}

func (p *httpMethodPredicate) Matches(req *Request) MatchResult {

	for _, method := range p.methods {
		if method == req.Method {
			return MatchResult{true, nil}
		}
	}
	return MatchResult{false, nil}
}

func IsHTTPMethod(methods ...string) RequestMatcher {
	return &httpMethodPredicate{methods}
}

type predicateAnd struct {
	predicates []RequestMatcher
}

func (p *predicateAnd) Matches(req *Request) MatchResult {
	var lastDecision *Decision

	for _, predicate := range p.predicates {
		res := predicate.Matches(req)
		lastDecision = res.Decision
		if !res.Matched {
			return MatchResult{false, res.Decision}
		}
	}
	return MatchResult{true, lastDecision}
}

func And(predicates ...RequestMatcher) RequestMatcher {
	return &predicateAnd{predicates}
}

type predicateNever struct{}

func (p *predicateNever) Matches(req *Request) MatchResult {
	return MatchResult{false, nil}
}

func Never() RequestMatcher {
	return &predicateNever{}
}

type predicateAlways struct{}

func (p *predicateAlways) Matches(req *Request) MatchResult {
	return MatchResult{true, nil}
}

func Always() RequestMatcher {
	return &predicateAlways{}
}
