package quesma

import (
	"context"
	"encoding/json"
	"errors"
	"mitmproxy/quesma/clickhouse"
	"mitmproxy/quesma/elasticsearch"
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/quesma/config"
	"mitmproxy/quesma/quesma/mux"
	"mitmproxy/quesma/quesma/routes"
	"mitmproxy/quesma/quesma/termsenum"
	"mitmproxy/quesma/quesma/ui"
	"mitmproxy/quesma/stats/errorstats"
	"mitmproxy/quesma/telemetry"
	"mitmproxy/quesma/tracing"
	"regexp"
	"slices"
	"strings"
	"time"
)

const (
	httpOk              = 200
	quesmaAsyncIdPrefix = "quesma_async_search_id_"
)

func configureRouter(cfg config.QuesmaConfiguration, lm *clickhouse.LogManager, im elasticsearch.IndexManagement, console *ui.QuesmaManagementConsole, phoneHomeAgent telemetry.PhoneHomeAgent, queryRunner *QueryRunner) *mux.PathRouter {
	router := mux.NewPathRouter()
	router.RegisterPath(routes.ClusterHealthPath, "GET", func(_ context.Context, body string, _ string, params map[string]string) (*mux.Result, error) {
		return elasticsearchQueryResult(`{"cluster_name": "quesma"}`, httpOk), nil
	})

	router.RegisterPathMatcher(routes.BulkPath, "POST", matchedAgainstBulkBody(cfg), func(ctx context.Context, body string, _ string, params map[string]string) (*mux.Result, error) {
		results := dualWriteBulk(ctx, nil, body, lm, cfg, phoneHomeAgent)
		return bulkInsertResult(results), nil
	})

	router.RegisterPathMatcher(routes.IndexRefreshPath, "POST", matchedExact(cfg), func(ctx context.Context, body string, _ string, params map[string]string) (*mux.Result, error) {
		return elasticsearchInsertResult(`{"_shards":{"total":1,"successful":1,"failed":0}}`, httpOk), nil
	})

	router.RegisterPathMatcher(routes.IndexDocPath, "POST", matchedExact(cfg), func(ctx context.Context, body string, _ string, params map[string]string) (*mux.Result, error) {
		dualWrite(ctx, params["index"], body, lm, cfg)
		return indexDocResult(params["index"], httpOk), nil
	})

	router.RegisterPathMatcher(routes.IndexBulkPath, "POST", matchedExact(cfg), func(ctx context.Context, body string, _ string, params map[string]string) (*mux.Result, error) {
		index := params["index"]
		results := dualWriteBulk(ctx, &index, body, lm, cfg, phoneHomeAgent)
		return bulkInsertResult(results), nil
	})

	router.RegisterPathMatcher(routes.IndexBulkPath, "PUT", matchedExact(cfg), func(ctx context.Context, body string, _ string, params map[string]string) (*mux.Result, error) {
		index := params["index"]
		results := dualWriteBulk(ctx, &index, body, lm, cfg, phoneHomeAgent)
		return bulkInsertResult(results), nil
	})

	router.RegisterPathMatcher(routes.ResolveIndexPath, "GET", always(), func(ctx context.Context, body string, _ string, params map[string]string) (*mux.Result, error) {
		pattern := params["index"]
		if elasticsearch.IsIndexPattern(pattern) {
			// todo avoid creating new instances all the time
			sources, found, err := elasticsearch.NewIndexResolver(cfg.Elasticsearch.Url.String()).Resolve(pattern)
			if err != nil {
				return nil, err
			}
			if !found {
				return &mux.Result{StatusCode: 404}, nil
			}

			definitions := lm.GetTableDefinitions()
			sources.Indices = slices.DeleteFunc(sources.Indices, func(i elasticsearch.Index) bool {
				return definitions.Has(i.Name)
			})
			sources.DataStreams = slices.DeleteFunc(sources.DataStreams, func(i elasticsearch.DataStream) bool {
				return definitions.Has(i.Name)
			})
			definitions.Range(
				func(name string, table *clickhouse.Table) bool {
					if config.MatchName(preprocessPattern(pattern), name) {
						sources.DataStreams = append(sources.DataStreams, elasticsearch.DataStream{
							Name:           name,
							BackingIndices: []string{name},
							TimestampField: `@timestamp`,
						})
					}

					return true
				})

			return resolveIndexResult(sources), nil
		} else {
			if config.MatchName(preprocessPattern(pattern), pattern) {
				definitions := lm.GetTableDefinitions()
				if definitions.Has(pattern) {
					return resolveIndexResult(elasticsearch.Sources{
						Indices: []elasticsearch.Index{},
						Aliases: []elasticsearch.Alias{},
						DataStreams: []elasticsearch.DataStream{
							{
								Name:           pattern,
								BackingIndices: []string{pattern},
								TimestampField: `@timestamp`,
							},
						},
					}), nil
				} else {
					return &mux.Result{StatusCode: 404}, nil
				}
			} else {
				sources, found, err := elasticsearch.NewIndexResolver(cfg.Elasticsearch.Url.String()).Resolve(pattern)
				if err != nil {
					return nil, err
				}
				if !found {
					return &mux.Result{StatusCode: 404}, nil
				}

				return resolveIndexResult(sources), nil
			}
		}
	})

	router.RegisterPathMatcher(routes.IndexCountPath, "GET", matchedAgainstPattern(cfg), func(ctx context.Context, _ string, _ string, params map[string]string) (*mux.Result, error) {
		cnt, err := queryRunner.handleCount(ctx, params["index"], lm)
		if err != nil {
			if errors.Is(errIndexNotExists, err) {
				return &mux.Result{StatusCode: 404}, nil
			} else {
				return nil, err
			}
		}

		if cnt == -1 {
			return &mux.Result{StatusCode: 404}, nil
		} else {
			return elasticsearchCountResult(cnt, httpOk), nil
		}
	})

	router.RegisterPathMatcher(routes.GlobalSearchPath, "POST", func(_ map[string]string, _ string) bool {
		return true // for now, always route to Quesma, in the near future: combine results from both sources
	}, func(ctx context.Context, body string, _ string, params map[string]string) (*mux.Result, error) {
		responseBody, err := queryRunner.handleSearch(ctx, "*", []byte(body), cfg, lm, im, console)
		if err != nil {
			if errors.Is(errIndexNotExists, err) {
				return &mux.Result{StatusCode: 404}, nil
			} else {
				return nil, err
			}
		}
		return elasticsearchQueryResult(string(responseBody), httpOk), nil
	})

	router.RegisterPathMatcher(routes.IndexSearchPath, "POST", matchedAgainstPattern(cfg), func(ctx context.Context, body string, _ string, params map[string]string) (*mux.Result, error) {
		responseBody, err := queryRunner.handleSearch(ctx, params["index"], []byte(body), cfg, lm, im, console)
		if err != nil {
			if errors.Is(errIndexNotExists, err) {
				return &mux.Result{StatusCode: 404}, nil
			} else {
				return nil, err
			}
		}
		return elasticsearchQueryResult(string(responseBody), httpOk), nil
	})
	router.RegisterPathMatcher(routes.IndexAsyncSearchPath, "POST", matchedAgainstPattern(cfg), func(ctx context.Context, body string, _ string, params map[string]string) (*mux.Result, error) {
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
			responseBody, err := queryRunner.handleAsyncSearch(ctx, cfg, params["index"], []byte(body), lm, im, console, waitForResultsMs, keepOnCompletion)
			if err != nil {
				if errors.Is(errIndexNotExists, err) {
					return &mux.Result{StatusCode: 404}, nil
				} else {
					return nil, err
				}
			}
			return elasticsearchQueryResult(string(responseBody), httpOk), nil
		}
	})
	router.RegisterPathMatcher(routes.AsyncSearchIdPath, "GET", matchedAgainstAsyncId(), func(ctx context.Context, body string, _ string, params map[string]string) (*mux.Result, error) {
		ctx = context.WithValue(ctx, tracing.AsyncIdCtxKey, params["id"])
		responseBody, err := queryRunner.handlePartialAsyncSearch(ctx, params["id"])
		if err != nil {
			return nil, err
		}
		return elasticsearchQueryResult(string(responseBody), httpOk), nil
	})

	router.RegisterPathMatcher(routes.AsyncSearchIdPath, "POST", matchedAgainstAsyncId(), func(ctx context.Context, body string, _ string, params map[string]string) (*mux.Result, error) {
		ctx = context.WithValue(ctx, tracing.AsyncIdCtxKey, params["id"])
		responseBody, err := queryRunner.handlePartialAsyncSearch(ctx, params["id"])
		if err != nil {
			return nil, err
		}
		return elasticsearchQueryResult(string(responseBody), httpOk), nil
	})

	router.RegisterPathMatcher(routes.AsyncSearchIdPath, "DELETE", matchedAgainstAsyncId(), func(ctx context.Context, body string, _ string, params map[string]string) (*mux.Result, error) {
		responseBody, err := queryRunner.deleteAsyncSeach(params["id"])
		if err != nil {
			return nil, err
		}
		return elasticsearchQueryResult(string(responseBody), httpOk), nil
	})

	router.RegisterPathMatcher(routes.FieldCapsPath, "POST", matchedAgainstPattern(cfg), func(ctx context.Context, body string, _ string, params map[string]string) (*mux.Result, error) {
		responseBody, err := handleFieldCaps(ctx, params["index"], []byte(body), lm)
		if err != nil {
			if errors.Is(errIndexNotExists, err) {
				return &mux.Result{StatusCode: 404}, nil
			} else {
				return nil, err
			}
		}
		return elasticsearchQueryResult(string(responseBody), httpOk), nil
	})
	router.RegisterPathMatcher(routes.TermsEnumPath, "POST", matchedAgainstPattern(cfg), func(ctx context.Context, body string, _ string, params map[string]string) (*mux.Result, error) {
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

	router.RegisterPathMatcher(routes.EQLSearch, "GET", matchedAgainstPattern(cfg), func(ctx context.Context, body string, _ string, params map[string]string) (*mux.Result, error) {
		responseBody, err := queryRunner.handleEQLSearch(ctx, params["index"], []byte(body), cfg, lm, im, console)
		if err != nil {
			if errors.Is(errIndexNotExists, err) {
				return &mux.Result{StatusCode: 404}, nil
			} else {
				return nil, err
			}
		}
		return elasticsearchQueryResult(string(responseBody), httpOk), nil
	})

	return router
}

func always() func(params map[string]string, body string) bool {
	return func(params map[string]string, body string) bool {
		return true
	}
}

// check whether exact index name is enabled
func matchedExact(config config.QuesmaConfiguration) mux.MatchPredicate {
	return func(m map[string]string, _ string) bool {
		if elasticsearch.IsInternalIndex(m["index"]) {
			logger.Debug().Msgf("index %s is an internal Elasticsearch index, skipping", m["index"])
			return false
		}
		indexConfig, exists := config.IndexConfig[m["index"]]
		return exists && indexConfig.Enabled
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
		"X-Quesma-Headers-Source": "Quesma",
	}, StatusCode: statusCode}
}

func resolveIndexResult(sources elasticsearch.Sources) *mux.Result {
	body, err := json.Marshal(sources)
	if err != nil {
		panic(err)
	}

	return &mux.Result{
		Body:       string(body),
		Meta:       map[string]string{},
		StatusCode: httpOk}
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
