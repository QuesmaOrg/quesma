package quesma

import (
	"context"
	"errors"
	"mitmproxy/quesma/clickhouse"
	"mitmproxy/quesma/quesma/config"
	"mitmproxy/quesma/quesma/mux"
	"mitmproxy/quesma/quesma/routes"
	"mitmproxy/quesma/quesma/ui"
	"strings"
)

func configureRouter(config config.QuesmaConfiguration, lm *clickhouse.LogManager, console *ui.QuesmaManagementConsole) *mux.PathRouter {
	router := mux.NewPathRouter()
	router.RegisterPath(routes.ClusterHealthPath, "GET", func(_ context.Context, body string, _ string, params map[string]string) (string, error) {
		return `{"cluster_name": "quesma"}`, nil
	})
	router.RegisterPath(routes.BulkPath, "POST", func(ctx context.Context, body string, _ string, params map[string]string) (string, error) {
		dualWriteBulk(ctx, "", body, lm, config)
		return "", nil
	})
	router.RegisterPathMatcher(routes.IndexDocPath, "POST", withIndexEnabled(config), func(ctx context.Context, body string, _ string, params map[string]string) (string, error) {
		dualWrite(ctx, params["index"], body, lm, config)
		return "", nil
	})
	router.RegisterPathMatcher(routes.IndexBulkPath, "POST", withIndexEnabled(config), func(ctx context.Context, body string, _ string, params map[string]string) (string, error) {
		dualWriteBulk(ctx, params["index"], body, lm, config)
		return "", nil
	})
	router.RegisterPathMatcher(routes.IndexSearchPath, "POST", withIndexEnabled(config), func(ctx context.Context, body string, _ string, params map[string]string) (string, error) {
		if strings.Contains(params["index"], ",") {
			return "", errors.New("multi index search is not yet supported")
		} else {
			responseBody, err := handleSearch(ctx, params["index"], []byte(body), lm, console)
			if err != nil {
				return "", err
			}
			return string(responseBody), nil
		}
	})
	router.RegisterPathMatcher(routes.IndexAsyncSearchPath, "POST", withIndexEnabled(config), func(ctx context.Context, body string, _ string, params map[string]string) (string, error) {
		if strings.Contains(params["index"], ",") {
			return "", errors.New("multi index search is not yet supported")
		} else {
			responseBody, err := handleAsyncSearch(ctx, params["index"], []byte(body), lm, console)
			if err != nil {
				return "", err
			}
			return string(responseBody), nil
		}
	})
	return router
}

func withIndexEnabled(config config.QuesmaConfiguration) mux.MatchPredicate {
	return func(m map[string]string) bool {
		indexConfig, exists := config.GetIndexConfig(m["index"])
		return exists && indexConfig.Enabled
	}
}
