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
		handler      handler
	}
	handler                     func(ctx context.Context, body string, uri string, params map[string]string) (string, error)
	routerStatisticsAccumulator struct {
		mu         *sync.Mutex
		matched    map[string]bool
		nonmatched map[string]bool
	}
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
	mapping := mapping{pattern, urlpath.New(pattern), httpMethod, handler}
	p.mappings = append(p.mappings, mapping)
}

func (p *PathRouter) Execute(ctx context.Context, path string, body string, httpMethod string) (string, bool, error) {
	for _, m := range p.mappings {
		meta, match := m.compiledPath.Match(path)
		if match && m.httpMethod == httpMethod {
			logger.Debug().Str("path", path).Str("pattern", m.pattern).Msg("matched")
			routerStatistics.addMatched(path)
			resp, err := m.handler(ctx, body, path, meta.Params)
			return resp, true, err
		}
	}
	routerStatistics.addNonmatched(path)
	return "", false, nil
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
