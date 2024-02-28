package mux

import (
	"context"
	"github.com/ucarion/urlpath"
	"mitmproxy/quesma/logger"
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
		Body string
		Meta map[string]string
	}
	handler        func(ctx context.Context, body string, uri string, params map[string]string) (*Result, error)
	MatchPredicate func(map[string]string) bool
)

func HeaderlessResult(body string) *Result {
	return &Result{Body: body, Meta: make(map[string]string)}
}

// Url router where you can register multiple URL paths with handler.
// We need our own component as default libraries caused side-effects on requests or response.
// The pattern syntax is based on ucarion/urlpath project. e.g. "/shelves/:shelf/books/:book"
func NewPathRouter() *PathRouter {
	return &PathRouter{mappings: make([]mapping, 0)}
}

func (p *PathRouter) RegisterPath(pattern string, httpMethod string, handler handler) {
	mapping := mapping{pattern, urlpath.New(pattern), httpMethod, identity(), handler}
	p.mappings = append(p.mappings, mapping)
}

func (p *PathRouter) RegisterPathMatcher(pattern string, httpMethod string, predicate MatchPredicate, handler handler) {
	mapping := mapping{pattern, urlpath.New(pattern), httpMethod, predicate, handler}
	p.mappings = append(p.mappings, mapping)
}

func (p *PathRouter) Execute(ctx context.Context, path string, body string, httpMethod string) (*Result, error) {
	handler, meta, found := p.findHandler(path, httpMethod)
	if found {
		return handler(ctx, body, path, meta.Params)
	}
	return nil, nil
}

func (p *PathRouter) Matches(path string, httpMethod string) bool {
	_, _, found := p.findHandler(path, httpMethod)
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

func (p *PathRouter) findHandler(path string, httpMethod string) (handler, urlpath.Match, bool) {
	for _, m := range p.mappings {
		meta, match := m.compiledPath.Match(path)
		if match && m.httpMethod == httpMethod && m.predicate(meta.Params) {
			return m.handler, meta, true
		}
	}
	return nil, urlpath.Match{}, false
}

func identity() MatchPredicate {
	return func(map[string]string) bool {
		return true
	}
}
