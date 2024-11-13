// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package quesma

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"quesma/clickhouse"
	"quesma/elasticsearch"
	"quesma/end_user_errors"
	"quesma/ingest"
	"quesma/logger"
	"quesma/queryparser"
	"quesma/quesma/config"
	"quesma/quesma/errors"
	"quesma/quesma/functionality/bulk"
	"quesma/quesma/functionality/doc"
	"quesma/quesma/functionality/field_capabilities"
	"quesma/quesma/functionality/resolve"
	"quesma/quesma/functionality/terms_enum"
	"quesma/quesma/mux"
	"quesma/quesma/routes"
	"quesma/quesma/types"
	"quesma/quesma/ui"
	"quesma/schema"
	"quesma/table_resolver"
	"quesma/telemetry"
	"quesma/tracing"
	"regexp"
	"strings"
	"sync"
	"time"
)

func configureRouter(cfg *config.QuesmaConfiguration, sr schema.Registry, lm *clickhouse.LogManager, ip *ingest.IngestProcessor, console *ui.QuesmaManagementConsole, phoneHomeAgent telemetry.PhoneHomeAgent, queryRunner *QueryRunner, tableResolver table_resolver.TableResolver) *mux.PathRouter {

	// some syntactic sugar
	method := mux.IsHTTPMethod
	and := mux.And

	router := mux.NewPathRouter()
	router.Register(routes.ClusterHealthPath, method("GET"), func(_ context.Context, req *mux.Request) (*mux.Result, error) {
		return elasticsearchQueryResult(`{"cluster_name": "quesma"}`, http.StatusOK), nil
	})

	router.Register(routes.BulkPath, and(method("POST"), matchedAgainstBulkBody(cfg, tableResolver)), func(ctx context.Context, req *mux.Request) (*mux.Result, error) {

		body, err := types.ExpectNDJSON(req.ParsedBody)
		if err != nil {
			return nil, err
		}

		results, err := bulk.Write(ctx, nil, body, ip, cfg, phoneHomeAgent, tableResolver)
		return bulkInsertResult(ctx, results, err)
	})

	router.Register(routes.IndexRefreshPath, and(method("POST"), matchedExactQueryPath(tableResolver)), func(ctx context.Context, req *mux.Request) (*mux.Result, error) {
		return elasticsearchInsertResult(`{"_shards":{"total":1,"successful":1,"failed":0}}`, http.StatusOK), nil
	})

	router.Register(routes.IndexDocPath, and(method("POST"), matchedExactIngestPath(tableResolver)), func(ctx context.Context, req *mux.Request) (*mux.Result, error) {
		index := req.Params["index"]

		body, err := types.ExpectJSON(req.ParsedBody)
		if err != nil {
			return &mux.Result{
				Body:       string(queryparser.BadRequestParseError(err)),
				StatusCode: http.StatusBadRequest,
			}, nil
		}

		result, err := doc.Write(ctx, &index, body, ip, cfg, phoneHomeAgent, tableResolver)
		if err != nil {
			return &mux.Result{
				Body:       string(queryparser.BadRequestParseError(err)),
				StatusCode: http.StatusBadRequest,
			}, nil
		}

		return indexDocResult(result)
	})

	router.Register(routes.IndexBulkPath, and(method("POST", "PUT"), matchedExactIngestPath(tableResolver)), func(ctx context.Context, req *mux.Request) (*mux.Result, error) {
		index := req.Params["index"]

		body, err := types.ExpectNDJSON(req.ParsedBody)
		if err != nil {
			return nil, err
		}

		results, err := bulk.Write(ctx, &index, body, ip, cfg, phoneHomeAgent, tableResolver)
		return bulkInsertResult(ctx, results, err)
	})

	router.Register(routes.ResolveIndexPath, method("GET"), func(ctx context.Context, req *mux.Request) (*mux.Result, error) {
		sources, err := resolve.HandleResolve(req.Params["index"], sr, cfg)
		if err != nil {
			return nil, err
		}
		return resolveIndexResult(sources)
	})

	router.Register(routes.IndexCountPath, and(method("GET"), matchedAgainstPattern(tableResolver)), func(ctx context.Context, req *mux.Request) (*mux.Result, error) {
		cnt, err := queryRunner.handleCount(ctx, req.Params["index"])
		if err != nil {
			if errors.Is(quesma_errors.ErrIndexNotExists(), err) {
				return &mux.Result{StatusCode: http.StatusNotFound}, nil
			} else {
				return nil, err
			}
		}

		if cnt == -1 {
			return &mux.Result{StatusCode: http.StatusNotFound}, nil
		} else {
			return elasticsearchCountResult(cnt, http.StatusOK)
		}
	})

	// TODO: This endpoint is currently disabled (mux.Never()) as it's pretty much used only by internal Kibana requests,
	// it's error-prone to detect them in matchAgainstKibanaInternal() and Quesma can't handle well the cases of wildcard
	// matching many indices either way.
	router.Register(routes.GlobalSearchPath, and(mux.Never(), method("GET", "POST"), matchAgainstKibanaInternal()), func(ctx context.Context, req *mux.Request) (*mux.Result, error) {

		body, err := types.ExpectJSON(req.ParsedBody)
		if err != nil {
			return nil, err
		}

		// TODO we should pass JSON here instead of []byte
		responseBody, err := queryRunner.handleSearch(ctx, "*", body)
		if err != nil {
			if errors.Is(quesma_errors.ErrIndexNotExists(), err) {
				return &mux.Result{StatusCode: http.StatusNotFound}, nil
			} else {
				return nil, err
			}
		}
		return elasticsearchQueryResult(string(responseBody), http.StatusOK), nil
	})

	router.Register(routes.IndexSearchPath, and(method("GET", "POST"), matchedAgainstPattern(tableResolver)), func(ctx context.Context, req *mux.Request) (*mux.Result, error) {

		body, err := types.ExpectJSON(req.ParsedBody)
		if err != nil {
			return nil, err
		}

		responseBody, err := queryRunner.handleSearch(ctx, req.Params["index"], body)
		if err != nil {
			if errors.Is(quesma_errors.ErrIndexNotExists(), err) {
				return &mux.Result{StatusCode: http.StatusNotFound}, nil
			} else if errors.Is(err, quesma_errors.ErrCouldNotParseRequest()) {
				return &mux.Result{
					Body:       string(queryparser.BadRequestParseError(err)),
					StatusCode: http.StatusBadRequest,
				}, nil
			} else {
				return nil, err
			}
		}
		return elasticsearchQueryResult(string(responseBody), http.StatusOK), nil
	})
	router.Register(routes.IndexAsyncSearchPath, and(method("POST"), matchedAgainstPattern(tableResolver)), func(ctx context.Context, req *mux.Request) (*mux.Result, error) {
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
			if errors.Is(quesma_errors.ErrIndexNotExists(), err) {
				return &mux.Result{StatusCode: http.StatusNotFound}, nil
			} else if errors.Is(err, quesma_errors.ErrCouldNotParseRequest()) {
				return &mux.Result{
					Body:       string(queryparser.BadRequestParseError(err)),
					StatusCode: http.StatusBadRequest,
				}, nil
			} else {
				return nil, err
			}
		}
		return elasticsearchQueryResult(string(responseBody), http.StatusOK), nil
	})

	router.Register(routes.IndexMappingPath, and(method("PUT"), matchedAgainstPattern(tableResolver)), func(ctx context.Context, req *mux.Request) (*mux.Result, error) {
		index := req.Params["index"]

		body, err := types.ExpectJSON(req.ParsedBody)
		if err != nil {
			return nil, err
		}

		columns := elasticsearch.ParseMappings("", body)

		sr.UpdateDynamicConfiguration(schema.TableName(index), schema.Table{Columns: columns})

		return putIndexResult(index)
	})

	router.Register(routes.IndexMappingPath, and(method("GET"), matchedAgainstPattern(tableResolver)), func(ctx context.Context, req *mux.Request) (*mux.Result, error) {
		index := req.Params["index"]

		foundSchema, found := sr.FindSchema(schema.TableName(index))
		if !found {
			return &mux.Result{StatusCode: http.StatusNotFound}, nil
		}

		hierarchicalSchema := schema.SchemaToHierarchicalSchema(&foundSchema)
		mappings := elasticsearch.GenerateMappings(hierarchicalSchema)

		return getIndexMappingResult(index, mappings)
	})

	router.Register(routes.AsyncSearchIdPath, and(method("GET"), matchedAgainstAsyncId()), func(ctx context.Context, req *mux.Request) (*mux.Result, error) {
		ctx = context.WithValue(ctx, tracing.AsyncIdCtxKey, req.Params["id"])
		responseBody, err := queryRunner.handlePartialAsyncSearch(ctx, req.Params["id"])
		if err != nil {
			return nil, err
		}
		return elasticsearchQueryResult(string(responseBody), http.StatusOK), nil
	})

	router.Register(routes.AsyncSearchIdPath, and(method("DELETE"), matchedAgainstAsyncId()), func(ctx context.Context, req *mux.Request) (*mux.Result, error) {
		responseBody, err := queryRunner.deleteAsyncSearch(req.Params["id"])
		if err != nil {
			return nil, err
		}
		return elasticsearchQueryResult(string(responseBody), http.StatusOK), nil
	})

	router.Register(routes.FieldCapsPath, and(method("GET", "POST"), matchedAgainstPattern(tableResolver)), func(ctx context.Context, req *mux.Request) (*mux.Result, error) {

		responseBody, err := field_capabilities.HandleFieldCaps(ctx, cfg, sr, req.Params["index"], lm)
		if err != nil {
			if errors.Is(quesma_errors.ErrIndexNotExists(), err) {
				if req.QueryParams.Get("allow_no_indices") == "true" || req.QueryParams.Get("ignore_unavailable") == "true" {
					return elasticsearchQueryResult(string(field_capabilities.EmptyFieldCapsResponse()), http.StatusOK), nil
				}
				return &mux.Result{StatusCode: http.StatusNotFound}, nil
			} else {
				return nil, err
			}
		}
		return elasticsearchQueryResult(string(responseBody), http.StatusOK), nil
	})
	router.Register(routes.TermsEnumPath, and(method("POST"), matchedAgainstPattern(tableResolver)), func(ctx context.Context, req *mux.Request) (*mux.Result, error) {
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

			if responseBody, err := terms_enum.HandleTermsEnum(ctx, req.Params["index"], body, lm, sr, console); err != nil {
				return nil, err
			} else {
				return elasticsearchQueryResult(string(responseBody), http.StatusOK), nil
			}
		}
	})

	router.Register(routes.EQLSearch, and(method("GET", "POST"), matchedAgainstPattern(tableResolver)), func(ctx context.Context, req *mux.Request) (*mux.Result, error) {
		body, err := types.ExpectJSON(req.ParsedBody)
		if err != nil {
			return nil, err
		}

		responseBody, err := queryRunner.handleEQLSearch(ctx, req.Params["index"], body)
		if err != nil {
			if errors.Is(quesma_errors.ErrIndexNotExists(), err) {
				return &mux.Result{StatusCode: http.StatusNotFound}, nil
			} else {
				return nil, err
			}
		}
		return elasticsearchQueryResult(string(responseBody), http.StatusOK), nil
	})

	router.Register(routes.IndexPath, and(method("PUT"), matchedAgainstPattern(tableResolver)), func(ctx context.Context, req *mux.Request) (*mux.Result, error) {
		index := req.Params["index"]
		if req.Body == "" {
			logger.Warn().Msgf("empty body in PUT /%s request, Quesma is not doing anything", index)
			return putIndexResult(index)
		}

		body, err := types.ExpectJSON(req.ParsedBody)
		if err != nil {
			return nil, err
		}

		mappings, ok := body["mappings"]
		if !ok {
			logger.Warn().Msgf("no mappings found in PUT /%s request, ignoring that request. Full content: %s", index, req.Body)
			return putIndexResult(index)
		}
		columns := elasticsearch.ParseMappings("", mappings.(map[string]interface{}))

		sr.UpdateDynamicConfiguration(schema.TableName(index), schema.Table{Columns: columns})

		return putIndexResult(index)
	})

	router.Register(routes.IndexPath, and(method("GET"), matchedAgainstPattern(tableResolver)), func(ctx context.Context, req *mux.Request) (*mux.Result, error) {
		index := req.Params["index"]

		foundSchema, found := sr.FindSchema(schema.TableName(index))
		if !found {
			return &mux.Result{StatusCode: http.StatusNotFound}, nil
		}

		hierarchicalSchema := schema.SchemaToHierarchicalSchema(&foundSchema)
		mappings := elasticsearch.GenerateMappings(hierarchicalSchema)

		return getIndexResult(index, mappings)
	})

	router.Register(routes.QuesmaTableResolverPath, mux.Always(), func(ctx context.Context, req *mux.Request) (*mux.Result, error) {

		indexPattern := req.Params["index"]

		decisions := make(map[string]*table_resolver.Decision)
		humanReadable := make(map[string]string)
		for _, pipeline := range tableResolver.Pipelines() {
			decision := tableResolver.Resolve(pipeline, indexPattern)
			decisions[pipeline] = decision
			humanReadable[pipeline] = decision.String()
		}

		resp := struct {
			IndexPattern  string                              `json:"index_pattern"`
			Decisions     map[string]*table_resolver.Decision `json:"decisions"`
			HumanReadable map[string]string                   `json:"human_readable"`
		}{
			IndexPattern:  indexPattern,
			Decisions:     decisions,
			HumanReadable: humanReadable,
		}

		body, err := json.MarshalIndent(resp, "", "  ")
		if err != nil {
			return nil, err
		}

		return &mux.Result{Body: string(body), StatusCode: http.StatusOK}, nil
	})

	return router
}

func elasticsearchCountResult(body int64, statusCode int) (*mux.Result, error) {
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
		return nil, err
	}
	return &mux.Result{Body: string(serialized), Meta: map[string]string{
		"Content-Type":            "application/json",
		"X-Quesma-Headers-Source": "Quesma",
	}, StatusCode: statusCode}, nil
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

var ingestWarning sync.Once

func noIngestEnabledButThereIngestRequest() {
	logger.Error().Msgf("Ingest is disabled by configuration, but the request is trying to ingest data. ")
}

func bulkInsertResult(ctx context.Context, ops []bulk.BulkItem, err error) (*mux.Result, error) {

	if err != nil {
		var msg string
		var reason string
		var httpCode int

		var endUserError *end_user_errors.EndUserError
		if errors.As(err, &endUserError) {
			msg = string(queryparser.InternalQuesmaError(endUserError.EndUserErrorMessage()))
			reason = endUserError.Reason()
			httpCode = http.StatusInternalServerError

			if endUserError.ErrorType().Number == end_user_errors.ErrNoIngest.Number {
				// agents have no mercy, they will try again, and again
				// we should log this error once
				ingestWarning.Do(noIngestEnabledButThereIngestRequest)
			}

		} else {
			msg = string(queryparser.BadRequestParseError(err))
			reason = err.Error()
			httpCode = http.StatusBadRequest
		}

		// ingest can be noisy, so we can enable debug logs here
		var logEveryIngestError bool

		if logEveryIngestError {
			logger.ErrorWithCtxAndReason(ctx, reason).Msgf("Bulk insert error: %v", err)
		}

		return &mux.Result{
			Body:       msg,
			StatusCode: httpCode,
		}, nil
	}

	if err != nil {
		return &mux.Result{
			Body:       string(queryparser.BadRequestParseError(err)),
			StatusCode: http.StatusBadRequest,
		}, nil
	}

	body, err := json.Marshal(bulk.BulkResponse{
		Errors: false,
		Items:  ops,
		Took:   42,
	})
	if err != nil {
		return nil, err
	}

	return elasticsearchInsertResult(string(body), http.StatusOK), nil
}

func elasticsearchInsertResult(body string, statusCode int) *mux.Result {
	return &mux.Result{Body: body, Meta: map[string]string{
		// TODO copy paste from the original request
		contentTypeHeaderKey:      "application/json",
		"X-Quesma-Headers-Source": "Quesma",
	}, StatusCode: statusCode}
}

func resolveIndexResult(sources elasticsearch.Sources) (*mux.Result, error) {
	if len(sources.Aliases) == 0 && len(sources.DataStreams) == 0 && len(sources.Indices) == 0 {
		return &mux.Result{StatusCode: http.StatusNotFound}, nil
	}

	body, err := json.Marshal(sources)
	if err != nil {
		return nil, err
	}

	return &mux.Result{
		Body:       string(body),
		Meta:       map[string]string{},
		StatusCode: http.StatusOK}, nil
}

func indexDocResult(bulkItem bulk.BulkItem) (*mux.Result, error) {
	body, err := json.Marshal(bulkItem.Index)
	if err != nil {
		return nil, err
	}
	return elasticsearchInsertResult(string(body), http.StatusOK), nil
}

func putIndexResult(index string) (*mux.Result, error) {
	result := putIndexResponse{
		Acknowledged:       true,
		ShardsAcknowledged: true,
		Index:              index,
	}
	serialized, err := json.Marshal(result)
	if err != nil {
		return nil, err
	}

	return &mux.Result{StatusCode: http.StatusOK, Body: string(serialized)}, nil
}

func getIndexMappingResult(index string, mappings map[string]any) (*mux.Result, error) {
	result := map[string]any{
		index: map[string]any{
			"mappings": mappings,
		},
	}
	serialized, err := json.Marshal(result)
	if err != nil {
		return nil, err
	}

	return &mux.Result{StatusCode: http.StatusOK, Body: string(serialized)}, nil
}

func getIndexResult(index string, mappings map[string]any) (*mux.Result, error) {
	// For now return the same as getIndexMappingResult,
	// but "GET /:index" can also contain "settings" and "aliases" (in the future)
	return getIndexMappingResult(index, mappings)
}

type (
	putIndexResponse struct {
		Acknowledged       bool   `json:"acknowledged"`
		ShardsAcknowledged bool   `json:"shards_acknowledged"`
		Index              string `json:"index"`
	}
)

var indexNamePattern = regexp.MustCompile(`"_index"\s*:\s*"([^"]+)"`)

func extractIndexName(input string) string {
	results := indexNamePattern.FindStringSubmatch(input)

	if len(results) < 2 {
		return ""
	}

	return results[1]
}
