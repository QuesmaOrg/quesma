package quesma

import (
	"context"
	"mitmproxy/quesma/clickhouse"
	"mitmproxy/quesma/quesma/config"
	"mitmproxy/quesma/quesma/mux"
	"mitmproxy/quesma/quesma/ui"
)

func configureRouter(config config.QuesmaConfiguration, lm *clickhouse.LogManager, console *ui.QuesmaManagementConsole) *mux.PathRouter {
	router := mux.NewPathRouter()
	router.RegisterPath("/_cluster/health", "GET", func(_ context.Context, body string, _ string, params map[string]string) (string, error) {
		return `{"cluster_name": "quesma"}`, nil
	})
	router.RegisterPath("/_bulk", "POST", func(ctx context.Context, body string, _ string, params map[string]string) (string, error) {
		dualWriteBulk(ctx, "", body, lm, config)
		return "", nil
	})
	router.RegisterPath("/_search", "POST", func(ctx context.Context, body string, _ string, params map[string]string) (string, error) {
		// TODO Just for now, hardcoding the index to "logs-generic-default"
		TableName := `logs-generic-default`
		responseBody, err := handleSearch(ctx, TableName, []byte(body), lm, console)
		if err != nil {
			return "", err
		}
		return string(responseBody), nil
	})
	router.RegisterPath("/:index/_doc", "POST", func(ctx context.Context, body string, _ string, params map[string]string) (string, error) {
		dualWrite(ctx, params["index"], body, lm, config)
		return "", nil
	})
	router.RegisterPath("/:index/_bulk", "POST", func(ctx context.Context, body string, _ string, params map[string]string) (string, error) {
		dualWriteBulk(ctx, params["index"], body, lm, config)
		return "", nil
	})
	router.RegisterPath("/:index/_search", "POST", func(ctx context.Context, body string, _ string, params map[string]string) (string, error) {
		responseBody, err := handleSearch(ctx, params["index"], []byte(body), lm, console)
		if err != nil {
			return "", err
		}
		return string(responseBody), nil
	})
	router.RegisterPath("/:index/_async_search", "POST", func(ctx context.Context, body string, _ string, params map[string]string) (string, error) {
		responseBody, err := handleAsyncSearch(ctx, params["index"], []byte(body), lm, console)
		if err != nil {
			return "", err
		}
		return string(responseBody), nil
	})
	return router
}
