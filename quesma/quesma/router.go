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
	"mitmproxy/quesma/quesma/termsenum"
	"mitmproxy/quesma/quesma/ui"
	"mitmproxy/quesma/stats/errorstats"
	"mitmproxy/quesma/telemetry"
	"regexp"
	"slices"
	"strings"
	"time"
)

const httpOk = 200
const elasticIndexPrefix = "."

func configureRouter(config config.QuesmaConfiguration, lm *clickhouse.LogManager, console *ui.QuesmaManagementConsole, phoneHomeAgent telemetry.PhoneHomeAgent, queryRunner *QueryRunner) *mux.PathRouter {
	router := mux.NewPathRouter()
	router.RegisterPath(routes.ClusterHealthPath, "GET", func(_ context.Context, body string, _ string, params map[string]string) (*mux.Result, error) {
		return elasticsearchQueryResult(`{"cluster_name": "quesma"}`, httpOk), nil
	})

	router.RegisterPathMatcher(routes.BulkPath, "POST", matchedAgainstBulkBody(config), func(ctx context.Context, body string, _ string, params map[string]string) (*mux.Result, error) {
		results := dualWriteBulk(ctx, body, lm, config, phoneHomeAgent)
		return bulkInsertResult(results), nil
	})

	router.RegisterPathMatcher(routes.IndexDocPath, "POST", matchedExact(config), func(ctx context.Context, body string, _ string, params map[string]string) (*mux.Result, error) {
		dualWrite(ctx, params["index"], body, lm, config)
		return indexDocResult(params["index"], httpOk), nil
	})

	router.RegisterPathMatcher(routes.IndexBulkPath, "POST", matchedExact(config), func(ctx context.Context, body string, _ string, params map[string]string) (*mux.Result, error) {
		dualWriteBulk(ctx, body, lm, config, phoneHomeAgent)
		return nil, nil
	})

	router.RegisterPathMatcher(routes.IndexCountPath, "GET", matchedAgainstPattern(config, fromClickhouse(lm)), func(ctx context.Context, _ string, _ string, params map[string]string) (*mux.Result, error) {
		cnt, err := queryRunner.handleCount(ctx, params["index"], lm)
		if err != nil {
			return nil, err
		}

		return elasticsearchCountResult(cnt, httpOk), nil
	})

	router.RegisterPathMatcher(routes.IndexSearchPath, "POST", matchedAgainstPattern(config, fromClickhouse(lm)), func(ctx context.Context, body string, _ string, params map[string]string) (*mux.Result, error) {
		responseBody, err := queryRunner.handleSearch(ctx, params["index"], []byte(body), lm, console)
		if err != nil {
			return nil, err
		}
		return elasticsearchQueryResult(string(responseBody), httpOk), nil
	})
	router.RegisterPathMatcher(routes.IndexAsyncSearchPath, "POST", matchedAgainstPattern(config, fromClickhouse(lm)), func(ctx context.Context, body string, _ string, params map[string]string) (*mux.Result, error) {
		if strings.Contains(params["index"], ",") {
			errorstats.GlobalErrorStatistics.RecordKnownError("Multi index search is not supported", nil,
				"Multi index search is not yet supported: "+params["index"])
			return nil, errors.New("multi index search is not yet supported")
		} else {
			waitForResultsMs := 1000 // Defaults to 1 second as in docs
			if v, ok := params["wait_for_completion_timeout"]; ok {
				if w, err := time.ParseDuration(v); err == nil {
					waitForResultsMs = int(w.Milliseconds())
				} else {
					logger.Warn().Msgf("Can't parse wait_for_completion_timeout value: %s", v)
				}
			}
			keepOnCompletion := false
			if v, ok := params["keep_on_completion"]; ok {
				if v == "true" {
					keepOnCompletion = true
				}
			}
			responseBody, err := queryRunner.handleAsyncSearch(ctx, params["index"], []byte(body), lm, console, waitForResultsMs, keepOnCompletion)
			if err != nil {
				return nil, err
			}
			return elasticsearchQueryResult(string(responseBody), httpOk), nil
		}
	})
	router.RegisterPath(routes.AsyncSearchIdPath, "GET", func(ctx context.Context, body string, _ string, params map[string]string) (*mux.Result, error) {
		responseBody, err := queryRunner.handlePartialAsyncSearch(params["id"], console)
		if err != nil {
			return nil, err
		}
		return elasticsearchQueryResult(string(responseBody), httpOk), nil
	})

	router.RegisterPath(routes.AsyncSearchIdPath, "POST", func(ctx context.Context, body string, _ string, params map[string]string) (*mux.Result, error) {
		responseBody, err := queryRunner.handlePartialAsyncSearch(params["id"], console)
		if err != nil {
			return nil, err
		}
		return elasticsearchQueryResult(string(responseBody), httpOk), nil
	})

	router.RegisterPath(routes.AsyncSearchIdPath, "DELETE", func(ctx context.Context, body string, _ string, params map[string]string) (*mux.Result, error) {
		responseBody, err := queryRunner.deleteAsyncSeach(params["id"])
		if err != nil {
			return nil, err
		}
		return elasticsearchQueryResult(string(responseBody), httpOk), nil
	})

	router.RegisterPathMatcher(routes.FieldCapsPath, "POST", matchedAgainstPattern(config, fromClickhouse(lm)), func(ctx context.Context, body string, _ string, params map[string]string) (*mux.Result, error) {
		responseBody, err := handleFieldCaps(ctx, params["index"], []byte(body), lm)
		if err != nil {
			return nil, err
		}
		return elasticsearchQueryResult(string(responseBody), httpOk), nil
	})
	router.RegisterPathMatcher(routes.TermsEnumPath, "POST", matchedAgainstPattern(config, fromClickhouse(lm)), func(ctx context.Context, body string, _ string, params map[string]string) (*mux.Result, error) {
		if strings.Contains(params["index"], ",") {
			return nil, errors.New("multi index terms enum is not yet supported")
		} else {
			if responseBody, err := termsenum.HandleTermsEnum(ctx, params["index"], []byte(body), lm, console); err != nil {
				return nil, err
			} else {
				return elasticsearchQueryResult(string(responseBody), httpOk), nil
			}
		}
	})
	return router
}

func matchedAgainstBulkBody(configuration config.QuesmaConfiguration) func(m map[string]string, body string) bool {
	return func(m map[string]string, body string) bool {
		for idx, s := range strings.Split(body, "\n") {
			if idx%2 == 0 && len(s) > 0 {
				indexConfig, found := configuration.GetIndexConfig(extractIndexName(s))
				if !found || !indexConfig.Enabled {
					return false
				}
			}
		}
		return true
	}
}

func fromClickhouse(lm *clickhouse.LogManager) func() []string {
	return func() []string {
		definitions := lm.GetTableDefinitions()
		return definitions.Keys()
	}
}

// check whether exact index name is enabled
func matchedExact(config config.QuesmaConfiguration) mux.MatchPredicate {
	return func(m map[string]string, _ string) bool {
		if strings.HasPrefix(m["index"], elasticIndexPrefix) {
			logger.Debug().Msgf("index %s is an internal Elasticsearch index, skipping", m["index"])
			return false
		}
		indexConfig, exists := config.GetIndexConfig(m["index"])
		return exists && indexConfig.Enabled
	}
}

func matchedAgainstPattern(configuration config.QuesmaConfiguration, tables func() []string) mux.MatchPredicate {
	return func(m map[string]string, _ string) bool {
		if strings.HasPrefix(m["index"], elasticIndexPrefix) {
			logger.Debug().Msgf("index %s is an internal Elasticsearch index, skipping", m["index"])
			return false
		}

		var candidates []string

		if strings.ContainsAny(m["index"], "*,") {
			for _, pattern := range strings.Split(m["index"], ",") {
				for _, tableName := range tables() {
					if config.MatchName(pattern, tableName) {
						candidates = append(candidates, tableName)
					}
				}
			}

			slices.Sort(candidates)
			candidates = slices.Compact(candidates)

			for _, candidate := range candidates {
				indexConfig, exists := configuration.GetIndexConfig(candidate)
				if !exists || !indexConfig.Enabled {
					return false
				}
			}
			return true
		} else {
			for _, tableName := range tables() {
				if config.MatchName(m["index"], tableName) {
					candidates = append(candidates, tableName)
				}
			}

			for _, candidate := range candidates {
				indexConfig, exists := configuration.GetIndexConfig(candidate)
				if exists && indexConfig.Enabled {
					return true
				}
			}
			logger.Debug().Msgf("no index found for pattern %s", m["index"])
			return false
		}
	}
}

func elasticsearchCountResult(body int64, statusCode int) *mux.Result {
	var result = countResult{
		Shards: struct {
			Failed     int `json:"failed"`
			Skipped    int `json:"skipped"`
			Successful int `json:"successful"`
			Total      int `json:"total"`
		}{
			Failed:     0,
			Skipped:    0,
			Successful: 1,
			Total:      1,
		},
		Count: body,
	}
	serialized, err := json.Marshal(result)
	if err != nil {
		panic(err)
	}
	return &mux.Result{Body: string(serialized), Meta: map[string]string{
		"Location":                "/.clickhouse",
		"Content-Type":            "application/json",
		"X-Quesma-Headers-Source": "Quesma",
	}, StatusCode: statusCode}
}

type countResult struct {
	Shards struct {
		Failed     int `json:"failed"`
		Skipped    int `json:"skipped"`
		Successful int `json:"successful"`
		Total      int `json:"total"`
	} `json:"_shards"`
	Count int64 `json:"count"`
}

func elasticsearchQueryResult(body string, statusCode int) *mux.Result {
	return &mux.Result{Body: body, Meta: map[string]string{
		// TODO copy paste from the original request
		"Location":                "/.clickhouse",
		"X-Quesma-Headers-Source": "Quesma",
	}, StatusCode: statusCode}
}

func bulkInsertResult(ops []WriteResult) *mux.Result {
	body, err := json.Marshal(bulkResponse{
		Errors: false,
		Items:  toBulkItems(ops),
		Took:   42,
	})
	if err != nil {
		panic(err)
	}

	return elasticsearchInsertResult(string(body), httpOk)
}

func elasticsearchInsertResult(body string, statusCode int) *mux.Result {
	return &mux.Result{Body: body, Meta: map[string]string{
		// TODO copy paste from the original request
		contentTypeHeaderKey:      "application/json",
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

func bulkSingleResult(opName string, index string) any {
	response := bulkSingleResponse{
		ID:          "fakeId",
		Index:       index,
		PrimaryTerm: 1,
		SeqNo:       0,
		Shards: shardsResponse{
			Failed:     0,
			Successful: 1,
			Total:      1,
		},
		Version: 0,
		Result:  "created",
		Status:  201,
	}
	if opName == "create" {
		return struct {
			Create bulkSingleResponse `json:"create"`
		}{Create: response}
	} else if opName == "index" {
		return struct {
			Index bulkSingleResponse `json:"index"`
		}{Index: response}
	} else {
		panic("unsupported operation name: " + opName)
	}
}

type (
	indexDocResponse struct {
		Id          string         `json:"_id"`
		Index       string         `json:"_index"`
		PrimaryTerm int            `json:"_primary_term"`
		SeqNo       int            `json:"_seq_no"`
		Shards      shardsResponse `json:"_shards"`
		Version     int            `json:"_version"`
		Result      string         `json:"result"`
	}
	bulkSingleResponse struct {
		ID          string         `json:"_id"`
		Index       string         `json:"_index"`
		PrimaryTerm int            `json:"_primary_term"`
		SeqNo       int            `json:"_seq_no"`
		Shards      shardsResponse `json:"_shards"`
		Version     int            `json:"_version"`
		Result      string         `json:"result"`
		Status      int            `json:"status"`
	}
	bulkResponse struct {
		Errors bool  `json:"errors"`
		Items  []any `json:"items"`
		Took   int   `json:"took"`
	}
	shardsResponse struct {
		Failed     int `json:"failed"`
		Successful int `json:"successful"`
		Total      int `json:"total"`
	}
)

func toBulkItems(ops []WriteResult) []any {
	var items []any
	for _, op := range ops {
		items = append(items, bulkSingleResult(op.Operation, op.Index))
	}
	return items
}

var indexNamePattern = regexp.MustCompile(`"_index"\s*:\s*"([^"]+)"`)

func extractIndexName(input string) string {
	results := indexNamePattern.FindStringSubmatch(input)

	if len(results) < 2 {
		return ""
	}

	return results[1]
}
