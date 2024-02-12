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
		handler      handler
	}
	handler func(ctx context.Context, body string, uri string, params map[string]string) (string, error)
)

// Url router where you can register multiple URL paths with handler.
// We need our own component as default libraries caused side-effects on requests or response.
// The pattern syntax is based on ucarion/urlpath project. e.g. "/shelves/:shelf/books/:book"
func NewPathRouter() *PathRouter {
	return &PathRouter{mappings: make([]mapping, 0)}
}

func (receiver *PathRouter) RegisterPath(pattern string, httpMethod string, handler handler) {
	mapping := mapping{pattern, urlpath.New(pattern), httpMethod, handler}
	receiver.mappings = append(receiver.mappings, mapping)
}

func (receiver *PathRouter) Execute(ctx context.Context, path string, body string, httpMethod string) (string, bool, error) {
	for _, m := range receiver.mappings {
		meta, match := m.compiledPath.Match(path)
		if match && m.httpMethod == httpMethod {
			logger.Debug().Str("path", path).Str("pattern", m.pattern).Msg("matched")
			resp, err := m.handler(ctx, body, path, meta.Params)
			return resp, true, err
		}
	}
	return "", false, nil
}
