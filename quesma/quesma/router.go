package quesma

import (
	"context"
	"encoding/json"
	"errors"
	"mitmproxy/quesma/clickhouse"
	"mitmproxy/quesma/elasticsearch"
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/queryparser"
	"mitmproxy/quesma/quesma/config"
	"mitmproxy/quesma/quesma/mux"
	"mitmproxy/quesma/quesma/routes"
	"mitmproxy/quesma/quesma/termsenum"
	"mitmproxy/quesma/quesma/types"
	"mitmproxy/quesma/quesma/ui"
	"mitmproxy/quesma/schema"
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

func configureRouter(cfg config.QuesmaConfiguration, schemaRegistry schema.Registry, lm *clickhouse.LogManager, console *ui.QuesmaManagementConsole, phoneHomeAgent telemetry.PhoneHomeAgent, queryRunner *QueryRunner) *mux.PathRouter {

	// some syntactic sugar
	method := mux.IsHTTPMethod
	and := mux.And

	router := mux.NewPathRouter()
	router.Register(routes.ClusterHealthPath, method("GET"), func(_ context.Context, req *mux.Request) (*mux.Result, error) {
		return elasticsearchQueryResult(`{"cluster_name": "quesma"}`, httpOk), nil
	})

	router.Register(routes.BulkPath, and(method("POST"), matchedAgainstBulkBody(cfg)), func(ctx context.Context, req *mux.Request) (*mux.Result, error) {

		body, err := types.ExpectNDJSON(req.ParsedBody)
		if err != nil {
			return nil, err
		}

		results := dualWriteBulk(ctx, nil, body, lm, cfg, phoneHomeAgent)
		return bulkInsertResult(results), nil
	})

	router.Register(routes.IndexRefreshPath, and(method("POST"), matchedExact(cfg)), func(ctx context.Context, req *mux.Request) (*mux.Result, error) {
		return elasticsearchInsertResult(`{"_shards":{"total":1,"successful":1,"failed":0}}`, httpOk), nil
	})

	router.Register(routes.IndexDocPath, and(method("POST"), matchedExact(cfg)), func(ctx context.Context, req *mux.Request) (*mux.Result, error) {

		body, err := types.ExpectJSON(req.ParsedBody)
		if err != nil {
			return nil, err
		}

		err = dualWrite(ctx, req.Params["index"], body, lm, cfg)
		return indexDocResult(req.Params["index"], httpOk), err
	})

	router.Register(routes.IndexBulkPath, and(method("POST", "PUT"), matchedExact(cfg)), func(ctx context.Context, req *mux.Request) (*mux.Result, error) {
		index := req.Params["index"]

		body, err := types.ExpectNDJSON(req.ParsedBody)
		if err != nil {
			return nil, err
		}

		results := dualWriteBulk(ctx, &index, body, lm, cfg, phoneHomeAgent)
		return bulkInsertResult(results), nil
	})

	router.Register(routes.ResolveIndexPath, method("GET"), func(ctx context.Context, req *mux.Request) (*mux.Result, error) {
		pattern := elasticsearch.NormalizePattern(req.Params["index"])
		if elasticsearch.IsIndexPattern(pattern) {
			// todo avoid creating new instances all the time
			sources, found, err := elasticsearch.NewIndexResolver(cfg.Elasticsearch.Url.String()).Resolve(pattern)
			if err != nil {
				return nil, err
			}
			if !found {
				return &mux.Result{StatusCode: 404}, nil
			}

			definitions, err := lm.GetTableDefinitions()
			if err != nil {
				return nil, err
			}
			sources.Indices = slices.DeleteFunc(sources.Indices, func(i elasticsearch.Index) bool {
				return definitions.Has(i.Name)
			})
			sources.DataStreams = slices.DeleteFunc(sources.DataStreams, func(i elasticsearch.DataStream) bool {
				return definitions.Has(i.Name)
			})
			definitions.Range(
				func(name string, table *clickhouse.Table) bool {
					if config.MatchName(elasticsearch.NormalizePattern(pattern), name) {
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
			if config.MatchName(elasticsearch.NormalizePattern(pattern), pattern) {
				definitions, err := lm.GetTableDefinitions()
				if err != nil {
					return nil, err
				}

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

	router.Register(routes.IndexCountPath, and(method("GET"), matchedAgainstPattern(cfg)), func(ctx context.Context, req *mux.Request) (*mux.Result, error) {
		cnt, err := queryRunner.handleCount(ctx, req.Params["index"])
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

	router.Register(routes.GlobalSearchPath, and(method("GET", "POST"), matchAgainstKibanaAlerts()), func(ctx context.Context, req *mux.Request) (*mux.Result, error) {

		body, err := types.ExpectJSON(req.ParsedBody)
		if err != nil {
			return nil, err
		}

		// TODO we should pass JSON here instead of []byte
		responseBody, err := queryRunner.handleSearch(ctx, "*", body)
		if err != nil {
			if errors.Is(errIndexNotExists, err) {
				return &mux.Result{StatusCode: 404}, nil
			} else {
				return nil, err
			}
		}
		return elasticsearchQueryResult(string(responseBody), httpOk), nil
	})

	router.Register(routes.IndexSearchPath, and(method("GET", "POST"), matchedAgainstPattern(cfg)), func(ctx context.Context, req *mux.Request) (*mux.Result, error) {

		body, err := types.ExpectJSON(req.ParsedBody)
		if err != nil {
			return nil, err
		}

		responseBody, err := queryRunner.handleSearch(ctx, req.Params["index"], body)
		if err != nil {
			if errors.Is(errIndexNotExists, err) {
				return &mux.Result{StatusCode: 404}, nil
			} else if errors.Is(err, errCouldNotParseRequest) {
				return &mux.Result{
					Body:       string(queryparser.BadRequestParseError(err)),
					StatusCode: 400,
				}, nil
			} else {
				return nil, err
			}
		}
		return elasticsearchQueryResult(string(responseBody), httpOk), nil
	})
	router.Register(routes.IndexAsyncSearchPath, and(method("POST"), matchedAgainstPattern(cfg)), func(ctx context.Context, req *mux.Request) (*mux.Result, error) {
		waitForResultsMs := 1000 // Defaults to 1 second as in docs
		if v, ok := req.Params["wait_for_completion_timeout"]; ok {
			if w, err := time.ParseDuration(v); err == nil {
				waitForResultsMs = int(w.Milliseconds())
			} else {
				logger.Warn().Msgf("Can't parse wait_for_completion_timeout value: %s", v)
			}
		}
		keepOnCompletion := false
		if v, ok := req.Params["keep_on_completion"]; ok {
			if v == "true" {
				keepOnCompletion = true
			}
		}

		body, err := types.ExpectJSON(req.ParsedBody)
		if err != nil {
			return nil, err
		}

		responseBody, err := queryRunner.handleAsyncSearch(ctx, req.Params["index"], body, waitForResultsMs, keepOnCompletion)
		if err != nil {
			if errors.Is(errIndexNotExists, err) {
				return &mux.Result{StatusCode: 404}, nil
			} else if errors.Is(err, errCouldNotParseRequest) {
				return &mux.Result{
					Body:       string(queryparser.BadRequestParseError(err)),
					StatusCode: 400,
				}, nil
			} else {
				return nil, err
			}
		}
		return elasticsearchQueryResult(string(responseBody), httpOk), nil
	})

	router.Register(routes.AsyncSearchIdPath, and(method("GET"), matchedAgainstAsyncId()), func(ctx context.Context, req *mux.Request) (*mux.Result, error) {
		ctx = context.WithValue(ctx, tracing.AsyncIdCtxKey, req.Params["id"])
		responseBody, err := queryRunner.handlePartialAsyncSearch(ctx, req.Params["id"])
		if err != nil {
			return nil, err
		}
		return elasticsearchQueryResult(string(responseBody), httpOk), nil
	})

	router.Register(routes.AsyncSearchIdPath, and(method("DELETE"), matchedAgainstAsyncId()), func(ctx context.Context, req *mux.Request) (*mux.Result, error) {
		responseBody, err := queryRunner.deleteAsyncSeach(req.Params["id"])
		if err != nil {
			return nil, err
		}
		return elasticsearchQueryResult(string(responseBody), httpOk), nil
	})

	router.Register(routes.FieldCapsPath, and(method("GET", "POST"), matchedAgainstPattern(cfg)), func(ctx context.Context, req *mux.Request) (*mux.Result, error) {
		responseBody, err := handleFieldCaps(ctx, cfg, schemaRegistry, req.Params["index"], lm)
		if err != nil {
			if errors.Is(errIndexNotExists, err) {
				if req.QueryParams.Get("allow_no_indices") == "true" || req.QueryParams.Get("ignore_unavailable") == "true" {
					return elasticsearchQueryResult(string(EmptyFieldCapsResponse()), httpOk), nil
				}
				return &mux.Result{StatusCode: 404}, nil
			} else {
				return nil, err
			}
		}
		return elasticsearchQueryResult(string(responseBody), httpOk), nil
	})
	router.Register(routes.TermsEnumPath, and(method("POST"), matchedAgainstPattern(cfg)), func(ctx context.Context, req *mux.Request) (*mux.Result, error) {
		if strings.Contains(req.Params["index"], ",") {
			return nil, errors.New("multi index terms enum is not yet supported")
		} else {

			var body types.JSON
			switch b := req.ParsedBody.(type) {
			case types.JSON:
				body = b
			default:
				return nil, errors.New("invalid request body, expecting JSON")
			}

			if responseBody, err := termsenum.HandleTermsEnum(ctx, req.Params["index"], body, lm, console); err != nil {
				return nil, err
			} else {
				return elasticsearchQueryResult(string(responseBody), httpOk), nil
			}
		}
	})

	eqlHandler := func(ctx context.Context, req *mux.Request) (*mux.Result, error) {

		body, err := types.ExpectJSON(req.ParsedBody)
		if err != nil {
			return nil, err
		}

		responseBody, err := queryRunner.handleEQLSearch(ctx, req.Params["index"], body)
		if err != nil {
			if errors.Is(errIndexNotExists, err) {
				return &mux.Result{StatusCode: 404}, nil
			} else {
				return nil, err
			}
		}
		return elasticsearchQueryResult(string(responseBody), httpOk), nil
	}

	router.Register(routes.EQLSearch, and(method("GET", "POST"), matchedAgainstPattern(cfg)), eqlHandler)

	return router
}

// check whether exact index name is enabled
func matchedExact(config config.QuesmaConfiguration) mux.RequestMatcher {
	return mux.RequestMatcherFunc(func(req *mux.Request) bool {
		if elasticsearch.IsInternalIndex(req.Params["index"]) {
			logger.Debug().Msgf("index %s is an internal Elasticsearch index, skipping", req.Params["index"])
			return false
		}
		indexConfig, exists := config.IndexConfig[req.Params["index"]]
		return exists && indexConfig.Enabled
	})
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
