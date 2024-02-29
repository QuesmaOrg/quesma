package quesma

import (
	"context"
	"encoding/json"
	"errors"
	"mitmproxy/quesma/clickhouse"
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/quesma/config"
	"mitmproxy/quesma/quesma/mux"
	"mitmproxy/quesma/quesma/routes"
	"mitmproxy/quesma/quesma/ui"
	"mitmproxy/quesma/stats/errorstats"
	"strings"
)

const httpOk = 200

func configureRouter(config config.QuesmaConfiguration, lm *clickhouse.LogManager, console *ui.QuesmaManagementConsole) *mux.PathRouter {
	router := mux.NewPathRouter()
	router.RegisterPath(routes.ClusterHealthPath, "GET", func(_ context.Context, body string, _ string, params map[string]string) (*mux.Result, error) {
		return elasticsearchQueryResult(`{"cluster_name": "quesma"}`, httpOk), nil
	})

	router.RegisterPath(routes.BulkPath, "POST", func(ctx context.Context, body string, _ string, params map[string]string) (*mux.Result, error) {
		dualWriteBulk(ctx, "", body, lm, config)
		return nil, nil
	})

	router.RegisterPathMatcher(routes.IndexDocPath, "POST", matchedAgainstConfig(config), func(ctx context.Context, body string, _ string, params map[string]string) (*mux.Result, error) {
		dualWrite(ctx, params["index"], body, lm, config)
		return indexDocResult(params["index"], httpOk), nil
	})

	router.RegisterPathMatcher(routes.IndexBulkPath, "POST", matchedAgainstConfig(config), func(ctx context.Context, body string, _ string, params map[string]string) (*mux.Result, error) {
		dualWriteBulk(ctx, params["index"], body, lm, config)
		return nil, nil
	})

	router.RegisterPathMatcher(routes.IndexSearchPath, "POST", matchedAgainstPattern(config, fromClickhouse()), func(ctx context.Context, body string, _ string, params map[string]string) (*mux.Result, error) {
		if strings.Contains(params["index"], ",") {
			errorstats.GlobalErrorStatistics.RecordKnownError("Multi index search is not supported", nil,
				"Multi index search is not yet supported: "+params["index"])
			return nil, errors.New("multi index search is not yet supported")
		} else {
			responseBody, err := handleSearch(ctx, params["index"], []byte(body), lm, console)
			if err != nil {
				return nil, err
			}
			return elasticsearchQueryResult(string(responseBody), httpOk), nil
		}
	})
	router.RegisterPathMatcher(routes.IndexAsyncSearchPath, "POST", matchedAgainstPattern(config, fromClickhouse()), func(ctx context.Context, body string, _ string, params map[string]string) (*mux.Result, error) {
		if strings.Contains(params["index"], ",") {
			errorstats.GlobalErrorStatistics.RecordKnownError("Multi index search is not supported", nil,
				"Multi index search is not yet supported: "+params["index"])
			return nil, errors.New("multi index search is not yet supported")
		} else {
			responseBody, err := handleAsyncSearch(ctx, params["index"], []byte(body), lm, console)
			if err != nil {
				return nil, err
			}
			return elasticsearchQueryResult(string(responseBody), httpOk), nil
		}
	})
	router.RegisterPathMatcher(routes.FieldCapsPath, "POST", matchedAgainstPattern(config, fromClickhouse()), func(ctx context.Context, body string, _ string, params map[string]string) (*mux.Result, error) {
		if strings.Contains(params["index"], ",") {
			return nil, errors.New("multi index search is not yet supported")
		} else {
			responseBody, err := hanndleFieldCaps(ctx, params["index"], []byte(body), lm)
			if err != nil {
				return nil, err
			}
			return elasticsearchQueryResult(string(responseBody), httpOk), nil
		}
	})
	router.RegisterPathMatcher(routes.TermsEnumPath, "POST", matchedAgainstPattern(config, fromClickhouse()), func(ctx context.Context, body string, _ string, params map[string]string) (*mux.Result, error) {
		if strings.Contains(params["index"], ",") {
			return nil, errors.New("multi index terms enum is not yet supported")
		} else {
			if responseBody, err := handleTermsEnum(ctx, params["index"], []byte(body), lm); err != nil {
				return nil, err
			} else {
				return elasticsearchQueryResult(string(responseBody), httpOk), nil
			}
		}
	})
	return router
}

func fromClickhouse() func() []string {
	return func() []string {
		return clickhouse.Tables()
	}
}

func matchedAgainstConfig(config config.QuesmaConfiguration) mux.MatchPredicate {
	return func(m map[string]string) bool {
		indexConfig, exists := config.GetIndexConfig(m["index"])
		return exists && indexConfig.Enabled
	}
}

func matchedAgainstPattern(configuration config.QuesmaConfiguration, tables func() []string) mux.MatchPredicate {
	return func(m map[string]string) bool {
		if strings.HasPrefix(m["index"], ".") {
			logger.Debug().Msgf("index %s is an internal Elasticsearch index, skipping", m["index"])
			return false
		}

		var candidates []string
		for _, tableName := range tables() {
			if config.MatchName(m["index"], tableName) {
				candidates = append(candidates, tableName)
			}
		}

		if len(candidates) > 0 {
			// TODO multi-index support
			indexConfig, exists := configuration.GetIndexConfig(candidates[0])
			return exists && indexConfig.Enabled
		} else {
			logger.Debug().Msgf("no index found for pattern %s", m["index"])
			return false
		}
	}
}

func elasticsearchQueryResult(body string, statusCode int) *mux.Result {
	return &mux.Result{Body: body, Meta: map[string]string{
		"X-Elastic-Product": "Elasticsearch",
		// TODO copy paste from the original request
		"X-Opaque-Id":             "unknownId",
		"Content-Type":            "application/vnd.elasticsearch+json;compatible-with=8",
		"Location":                "/.clickhouse",
		"X-Quesma-Headers-Source": "Quesma",
	}, StatusCode: statusCode}
}

func elasticsearchInsertResult(body string, statusCode int) *mux.Result {
	return &mux.Result{Body: body, Meta: map[string]string{
		"X-Elastic-Product": "Elasticsearch",
		// TODO copy paste from the original request
		"X-Opaque-Id":             "unknownId",
		"Content-Type":            "application/json",
		"Location":                "/.clickhouse",
		"X-Quesma-Headers-Source": "Quesma",
	}, StatusCode: statusCode}
}

func indexDocResult(index string, statusCode int) *mux.Result {
	body, err := json.Marshal(indexDocResponse{
		Id:          "fakeId",
		Index:       index,
		PrimaryTerm: 1,
		SeqNo:       0,
		Shards: shardsResponse{
			Failed:     0,
			Successful: 1,
			Total:      1,
		},
		Version: 1,
		Result:  "created",
	})
	if err != nil {
		panic(err)
	}
	return elasticsearchInsertResult(string(body), statusCode)
}

type indexDocResponse struct {
	Id          string         `json:"_id"`
	Index       string         `json:"_index"`
	PrimaryTerm int            `json:"_primary_term"`
	SeqNo       int            `json:"_seq_no"`
	Shards      shardsResponse `json:"_shards"`
	Version     int            `json:"_version"`
	Result      string         `json:"result"`
}

type shardsResponse struct {
	Failed     int `json:"failed"`
	Successful int `json:"successful"`
	Total      int `json:"total"`
}
