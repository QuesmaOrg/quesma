package mux

import (
	"context"
	"github.com/ucarion/urlpath"
	"mitmproxy/quesma/logger"
	"slices"
	"sync"
)

type (
	PathRouter struct {
		mappings []mapping
	}
	Statistics struct {
		Matched    []string
		Nonmatched []string
	}
	mapping struct {
		pattern      string
		compiledPath urlpath.Path
		httpMethod   string
		predicate    MatchPredicate
		handler      handler
	}
	handler                     func(ctx context.Context, body string, uri string, params map[string]string) (string, error)
	routerStatisticsAccumulator struct {
		mu         *sync.Mutex
		matched    map[string]bool
		nonmatched map[string]bool
	}
	MatchPredicate func(map[string]string) bool
)

// TODO make it bounded and use RWMutex
var routerStatistics = routerStatisticsAccumulator{
	mu:         &sync.Mutex{},
	matched:    make(map[string]bool),
	nonmatched: make(map[string]bool)}

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

func (p *PathRouter) Execute(ctx context.Context, path string, body string, httpMethod string) (string, error) {
	handler, meta, found := p.findHandler(path, httpMethod)
	if found {
		resp, err := handler(ctx, body, path, meta.Params)
		return resp, err
	}
	return "", nil
}

func (p *PathRouter) Matches(path string, httpMethod string) bool {
	_, _, found := p.findHandler(path, httpMethod)
	if found {
		routerStatistics.addMatched(path)
		logger.Debug().Msgf("Matched path: %s", path)
		return true
	} else {
		routerStatistics.addNonmatched(path)
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

func MatchStatistics() Statistics {
	return routerStatistics.snapshot()
}

func (a *routerStatisticsAccumulator) addMatched(url string) {
	a.withLock(func() {
		a.matched[url] = true
	})
}

func (a *routerStatisticsAccumulator) addNonmatched(url string) {
	a.withLock(func() {
		a.nonmatched[url] = true
	})
}

func (a *routerStatisticsAccumulator) snapshot() Statistics {
	var matched []string
	var nonmatched []string

	a.withLock(func() {
		for k := range a.nonmatched {
			nonmatched = append(nonmatched, k)
		}

		for k := range a.matched {
			matched = append(matched, k)
		}
	})

	slices.Sort(matched)
	slices.Sort(nonmatched)
	return Statistics{matched, nonmatched}
}

func (a *routerStatisticsAccumulator) withLock(action func()) {
	a.mu.Lock()
	action()
	a.mu.Unlock()
}

func identity() MatchPredicate {
	return func(map[string]string) bool {
		return true
	}
}
