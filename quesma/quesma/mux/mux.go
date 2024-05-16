package mux

import (
	"context"
	"github.com/ucarion/urlpath"
	"mitmproxy/quesma/logger"
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
		httpMethod   string
		predicate    MatchPredicate
		handler      handler
	}
	Result struct {
		Body       string
		Meta       map[string]string
		StatusCode int
	}
	handler        func(ctx context.Context, body, uri string, params map[string]string, headers http.Header, queryParams url.Values) (*Result, error)
	MatchPredicate func(params map[string]string, body string) bool
)

// Url router where you can register multiple URL paths with handler.
// We need our own component as default libraries caused side-effects on requests or response.
// The pattern syntax is based on ucarion/urlpath project. e.g. "/shelves/:shelf/books/:book"
func NewPathRouter() *PathRouter {
	return &PathRouter{mappings: make([]mapping, 0)}
}

func (p *PathRouter) RegisterPath(pattern, httpMethod string, handler handler) {
	mapping := mapping{pattern, urlpath.New(pattern), httpMethod, identity(), handler}
	p.mappings = append(p.mappings, mapping)
}

func (p *PathRouter) RegisterPathMatcher(pattern string, httpMethods []string, predicate MatchPredicate, handler handler) {
	for _, httpMethod := range httpMethods {
		mapping := mapping{pattern, urlpath.New(pattern), httpMethod, predicate, handler}
		p.mappings = append(p.mappings, mapping)
	}
}

func (p *PathRouter) Execute(ctx context.Context, path, body, httpMethod string, headers http.Header, queryParams url.Values) (*Result, error) {
	handler, meta, found := p.findHandler(path, httpMethod, body)
	if found {
		return handler(ctx, body, path, meta.Params, headers, queryParams)
	}
	return nil, nil
}

func (p *PathRouter) Matches(path, httpMethod, body string) bool {
	_, _, found := p.findHandler(path, httpMethod, body)
	if found {
		routerStatistics.addMatched(path)
		logger.Debug().Msgf("Matched path: %s", path)
		return true
	} else {
		routerStatistics.addUnmatched(path)
		logger.Debug().Msgf("Non-matched path: %s", path)
		return false
	}
}

func (p *PathRouter) findHandler(path, httpMethod, body string) (handler, urlpath.Match, bool) {
	path = strings.TrimSuffix(path, "/")
	for _, m := range p.mappings {
		meta, match := m.compiledPath.Match(path)
		if match && m.httpMethod == httpMethod && m.predicate(meta.Params, body) {
			return m.handler, meta, true
		}
	}
	return nil, urlpath.Match{}, false
}

func identity() MatchPredicate {
	return func(map[string]string, string) bool {
		return true
	}
}
