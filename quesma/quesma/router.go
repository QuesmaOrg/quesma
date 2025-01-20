// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package quesma

import (
	"context"
	"errors"
	"github.com/QuesmaOrg/quesma/quesma/backend_connectors"
	"github.com/QuesmaOrg/quesma/quesma/clickhouse"
	"github.com/QuesmaOrg/quesma/quesma/elasticsearch"
	"github.com/QuesmaOrg/quesma/quesma/end_user_errors"
	"github.com/QuesmaOrg/quesma/quesma/frontend_connectors"
	"github.com/QuesmaOrg/quesma/quesma/ingest"
	"github.com/QuesmaOrg/quesma/quesma/logger"
	"github.com/QuesmaOrg/quesma/quesma/painful"
	"github.com/QuesmaOrg/quesma/quesma/queryparser"
	"github.com/QuesmaOrg/quesma/quesma/quesma/config"
	quesma_errors "github.com/QuesmaOrg/quesma/quesma/quesma/errors"
	"github.com/QuesmaOrg/quesma/quesma/quesma/functionality/bulk"
	"github.com/QuesmaOrg/quesma/quesma/quesma/functionality/doc"
	"github.com/QuesmaOrg/quesma/quesma/quesma/functionality/field_capabilities"
	"github.com/QuesmaOrg/quesma/quesma/quesma/functionality/resolve"
	"github.com/QuesmaOrg/quesma/quesma/quesma/functionality/terms_enum"
	"github.com/QuesmaOrg/quesma/quesma/quesma/types"
	"github.com/QuesmaOrg/quesma/quesma/quesma/ui"
	"github.com/QuesmaOrg/quesma/quesma/schema"
	"github.com/QuesmaOrg/quesma/quesma/table_resolver"
	"github.com/QuesmaOrg/quesma/quesma/telemetry"
	quesma_api "github.com/QuesmaOrg/quesma/quesma/v2/core"
	"github.com/QuesmaOrg/quesma/quesma/v2/core/routes"
	"github.com/QuesmaOrg/quesma/quesma/v2/core/tracing"
	"github.com/goccy/go-json"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"time"
)

func ConfigureRouter(cfg *config.QuesmaConfiguration, sr schema.Registry, lm *clickhouse.LogManager, ip *ingest.IngestProcessor, console *ui.QuesmaManagementConsole, phoneHomeAgent telemetry.PhoneHomeAgent, queryRunner *QueryRunner, tableResolver table_resolver.TableResolver, elasticsearchConnector *backend_connectors.ElasticsearchBackendConnector) *quesma_api.PathRouter {

	// some syntactic sugar
	method := quesma_api.IsHTTPMethod
	and := quesma_api.And

	router := quesma_api.NewPathRouter()

	// These are the endpoints that are not supported by Quesma
	// These will redirect to the elastic cluster.
	for _, path := range elasticsearch.InternalPaths {
		router.Register(path, quesma_api.Never(), func(ctx context.Context, req *quesma_api.Request, _ http.ResponseWriter) (*quesma_api.Result, error) {
			return nil, nil
		})
	}

	// These are the endpoints that are supported by Quesma

	// Warning:
	// The first handler that matches the path will be considered to use.
	// If the predicate returns false it will be redirected to the elastic cluster.
	// If the predicate returns true, the handler will be used.
	//
	// So, if you add multiple handlers with the same path, the first one will be used, the rest will be redirected to the elastic cluster.
	// This is current limitation of the router.

	router.Register(routes.ExecutePainlessScriptPath, and(method("POST"), matchAgainstIndexNameInScriptRequestBody(tableResolver)), func(ctx context.Context, req *quesma_api.Request, _ http.ResponseWriter) (*quesma_api.Result, error) {

		var scriptRequest painful.ScriptRequest

		err := json.Unmarshal([]byte(req.Body), &scriptRequest)
		if err != nil {
			return nil, err
		}

		scriptResponse, err := scriptRequest.Eval()

		if err != nil {
			errorResponse := painful.RenderErrorResponse(scriptRequest.Script.Source, err)
			responseBytes, err := json.Marshal(errorResponse)
			if err != nil {
				return nil, err
			}

			return &quesma_api.Result{
				Body:          string(responseBytes),
				StatusCode:    errorResponse.Status,
				GenericResult: responseBytes,
			}, nil
		}

		responseBytes, err := json.Marshal(scriptResponse)
		if err != nil {
			return nil, err
		}

		return &quesma_api.Result{
			Body:          string(responseBytes),
			StatusCode:    http.StatusOK,
			GenericResult: responseBytes,
		}, nil
	})

	router.Register(routes.ClusterHealthPath, method("GET"), func(_ context.Context, req *quesma_api.Request, _ http.ResponseWriter) (*quesma_api.Result, error) {

		return elasticsearchQueryResult(`{"cluster_name": "quesma"}`, http.StatusOK), nil
	})

	router.Register(routes.BulkPath, and(method("POST", "PUT"), matchedAgainstBulkBody(cfg, tableResolver)), func(ctx context.Context, req *quesma_api.Request, _ http.ResponseWriter) (*quesma_api.Result, error) {

		body, err := types.ExpectNDJSON(req.ParsedBody)
		if err != nil {
			return nil, err
		}

		results, err := bulk.Write(ctx, nil, body, ip, cfg.IngestStatistics, elasticsearchConnector, phoneHomeAgent, tableResolver)
		return bulkInsertResult(ctx, results, err)
	})

	router.Register(routes.IndexRefreshPath, and(method("POST"), matchedExactQueryPath(tableResolver)), func(ctx context.Context, req *quesma_api.Request, _ http.ResponseWriter) (*quesma_api.Result, error) {
		return elasticsearchInsertResult(`{"_shards":{"total":1,"successful":1,"failed":0}}`, http.StatusOK), nil
	})

	router.Register(routes.IndexDocPath, and(method("POST"), matchedExactIngestPath(tableResolver)), func(ctx context.Context, req *quesma_api.Request, _ http.ResponseWriter) (*quesma_api.Result, error) {
		index := req.Params["index"]

		body, err := types.ExpectJSON(req.ParsedBody)
		if err != nil {
			return &quesma_api.Result{
				Body:          string(queryparser.BadRequestParseError(err)),
				StatusCode:    http.StatusBadRequest,
				GenericResult: queryparser.BadRequestParseError(err),
			}, nil
		}

		result, err := doc.Write(ctx, &index, body, ip, cfg.IngestStatistics, phoneHomeAgent, tableResolver, elasticsearchConnector)
		if err != nil {
			return &quesma_api.Result{
				Body:          string(queryparser.BadRequestParseError(err)),
				StatusCode:    http.StatusBadRequest,
				GenericResult: queryparser.BadRequestParseError(err),
			}, nil
		}

		return indexDocResult(result)
	})

	router.Register(routes.IndexBulkPath, and(method("POST", "PUT"), matchedExactIngestPath(tableResolver)), func(ctx context.Context, req *quesma_api.Request, _ http.ResponseWriter) (*quesma_api.Result, error) {
		index := req.Params["index"]

		body, err := types.ExpectNDJSON(req.ParsedBody)
		if err != nil {
			return nil, err
		}

		results, err := bulk.Write(ctx, &index, body, ip, cfg.IngestStatistics, elasticsearchConnector, phoneHomeAgent, tableResolver)
		return bulkInsertResult(ctx, results, err)
	})

	router.Register(routes.ResolveIndexPath, method("GET"), func(ctx context.Context, req *quesma_api.Request, _ http.ResponseWriter) (*quesma_api.Result, error) {
		sources, err := resolve.HandleResolve(req.Params["index"], sr, queryRunner.im)
		if err != nil {
			return nil, err
		}
		return resolveIndexResult(sources)
	})

	router.Register(routes.IndexCountPath, and(method("GET"), matchedAgainstPattern(tableResolver)), func(ctx context.Context, req *quesma_api.Request, _ http.ResponseWriter) (*quesma_api.Result, error) {
		cnt, err := queryRunner.HandleCount(ctx, req.Params["index"])
		if err != nil {
			if errors.Is(quesma_errors.ErrIndexNotExists(), err) {
				return &quesma_api.Result{StatusCode: http.StatusNotFound}, nil
			} else {
				return nil, err
			}
		}

		if cnt == -1 {
			return &quesma_api.Result{StatusCode: http.StatusNotFound}, nil
		} else {
			return elasticsearchCountResult(cnt, http.StatusOK)
		}
	})

	// TODO: This endpoint is currently disabled (mux.Never()) as it's pretty much used only by internal Kibana requests,
	// it's error-prone to detect them in matchAgainstKibanaInternal() and Quesma can't handle well the cases of wildcard
	// matching many indices either way.
	router.Register(routes.GlobalSearchPath, and(quesma_api.Never(), method("GET", "POST"), matchAgainstKibanaInternal()), func(ctx context.Context, req *quesma_api.Request, _ http.ResponseWriter) (*quesma_api.Result, error) {

		body, err := types.ExpectJSON(req.ParsedBody)
		if err != nil {
			return nil, err
		}

		// TODO we should pass JSON here instead of []byte
		responseBody, err := queryRunner.HandleSearch(ctx, "*", body)
		if err != nil {
			if errors.Is(quesma_errors.ErrIndexNotExists(), err) {
				return &quesma_api.Result{StatusCode: http.StatusNotFound}, nil
			} else {
				return nil, err
			}
		}
		return elasticsearchQueryResult(string(responseBody), http.StatusOK), nil
	})

	router.Register(routes.IndexSearchPath, and(method("GET", "POST"), matchedAgainstPattern(tableResolver)), func(ctx context.Context, req *quesma_api.Request, _ http.ResponseWriter) (*quesma_api.Result, error) {

		body, err := types.ExpectJSON(req.ParsedBody)
		if err != nil {
			return nil, err
		}

		responseBody, err := queryRunner.HandleSearch(ctx, req.Params["index"], body)
		if err != nil {
			if errors.Is(quesma_errors.ErrIndexNotExists(), err) {
				return &quesma_api.Result{StatusCode: http.StatusNotFound}, nil
			} else if errors.Is(err, quesma_errors.ErrCouldNotParseRequest()) {
				return &quesma_api.Result{
					Body:          string(queryparser.BadRequestParseError(err)),
					StatusCode:    http.StatusBadRequest,
					GenericResult: queryparser.BadRequestParseError(err),
				}, nil
			} else {
				return nil, err
			}
		}
		return elasticsearchQueryResult(string(responseBody), http.StatusOK), nil
	})
	router.Register(routes.IndexAsyncSearchPath, and(method("POST"), matchedAgainstPattern(tableResolver)), func(ctx context.Context, req *quesma_api.Request, _ http.ResponseWriter) (*quesma_api.Result, error) {
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

		responseBody, err := queryRunner.HandleAsyncSearch(ctx, req.Params["index"], body, waitForResultsMs, keepOnCompletion)
		if err != nil {
			if errors.Is(quesma_errors.ErrIndexNotExists(), err) {
				return &quesma_api.Result{StatusCode: http.StatusNotFound}, nil
			} else if errors.Is(err, quesma_errors.ErrCouldNotParseRequest()) {
				return &quesma_api.Result{
					Body:          string(queryparser.BadRequestParseError(err)),
					StatusCode:    http.StatusBadRequest,
					GenericResult: queryparser.BadRequestParseError(err),
				}, nil
			} else {
				return nil, err
			}
		}
		return elasticsearchQueryResult(string(responseBody), http.StatusOK), nil
	})

	handleMultiSearch := func(ctx context.Context, req *quesma_api.Request, defaultIndexName string, _ http.ResponseWriter) (*quesma_api.Result, error) {

		body, err := types.ExpectNDJSON(req.ParsedBody)
		if err != nil {
			return nil, err
		}

		responseBody, err := queryRunner.HandleMultiSearch(ctx, defaultIndexName, body)

		if err != nil {
			if errors.Is(quesma_errors.ErrIndexNotExists(), err) {
				return &quesma_api.Result{StatusCode: http.StatusNotFound}, nil
			} else if errors.Is(err, quesma_errors.ErrCouldNotParseRequest()) {
				return &quesma_api.Result{
					Body:          string(queryparser.BadRequestParseError(err)),
					StatusCode:    http.StatusBadRequest,
					GenericResult: queryparser.BadRequestParseError(err),
				}, nil
			} else {
				return nil, err
			}
		}

		return elasticsearchQueryResult(string(responseBody), http.StatusOK), nil
	}

	router.Register(routes.IndexMsearchPath, and(method("GET", "POST"), quesma_api.Always()), func(ctx context.Context, req *quesma_api.Request, _ http.ResponseWriter) (*quesma_api.Result, error) {
		return handleMultiSearch(ctx, req, req.Params["index"], nil)
	})

	router.Register(routes.GlobalMsearchPath, and(method("GET", "POST"), quesma_api.Always()), func(ctx context.Context, req *quesma_api.Request, _ http.ResponseWriter) (*quesma_api.Result, error) {
		return handleMultiSearch(ctx, req, "", nil)
	})

	router.Register(routes.IndexMappingPath, and(method("GET", "PUT"), matchedAgainstPattern(tableResolver)), func(ctx context.Context, req *quesma_api.Request, _ http.ResponseWriter) (*quesma_api.Result, error) {

		switch req.Method {

		case "GET":
			index := req.Params["index"]

			foundSchema, found := sr.FindSchema(schema.IndexName(index))
			if !found {
				return &quesma_api.Result{StatusCode: http.StatusNotFound}, nil
			}

			hierarchicalSchema := schema.SchemaToHierarchicalSchema(&foundSchema)
			mappings := elasticsearch.GenerateMappings(hierarchicalSchema)

			return getIndexMappingResult(index, mappings)

		case "PUT":
			index := req.Params["index"]

			err := elasticsearch.IsValidIndexName(index)
			if err != nil {
				return nil, err
			}

			body, err := types.ExpectJSON(req.ParsedBody)
			if err != nil {
				return nil, err
			}

			columns := elasticsearch.ParseMappings("", body)
			sr.UpdateDynamicConfiguration(schema.IndexName(index), schema.Table{Columns: columns})
			return putIndexResult(index)
		}

		return nil, errors.New("unsupported method")

	})

	router.Register(routes.AsyncSearchStatusPath, and(method("GET"), matchedAgainstAsyncId()), func(ctx context.Context, req *quesma_api.Request, _ http.ResponseWriter) (*quesma_api.Result, error) {
		responseBody, err := queryRunner.HandleAsyncSearchStatus(ctx, req.Params["id"])
		if err != nil {
			return nil, err
		}
		return elasticsearchQueryResult(string(responseBody), http.StatusOK), nil
	})

	router.Register(routes.AsyncSearchIdPath, and(method("GET", "DELETE"), matchedAgainstAsyncId()), func(ctx context.Context, req *quesma_api.Request, _ http.ResponseWriter) (*quesma_api.Result, error) {

		switch req.Method {

		case "GET":
			ctx = context.WithValue(ctx, tracing.AsyncIdCtxKey, req.Params["id"])
			responseBody, err := queryRunner.HandlePartialAsyncSearch(ctx, req.Params["id"])
			if err != nil {
				return nil, err
			}
			return elasticsearchQueryResult(string(responseBody), http.StatusOK), nil

		case "DELETE":
			responseBody, err := queryRunner.DeleteAsyncSearch(req.Params["id"])
			if err != nil {
				return nil, err
			}
			return elasticsearchQueryResult(string(responseBody), http.StatusOK), nil
		}

		return nil, errors.New("unsupported method")
	})

	router.Register(routes.FieldCapsPath, and(method("GET", "POST"), matchedAgainstPattern(tableResolver)), func(ctx context.Context, req *quesma_api.Request, _ http.ResponseWriter) (*quesma_api.Result, error) {

		responseBody, err := field_capabilities.HandleFieldCaps(ctx, cfg.IndexConfig, sr, req.Params["index"], lm)
		if err != nil {
			if errors.Is(quesma_errors.ErrIndexNotExists(), err) {
				if req.QueryParams.Get("allow_no_indices") == "true" || req.QueryParams.Get("ignore_unavailable") == "true" {
					return elasticsearchQueryResult(string(field_capabilities.EmptyFieldCapsResponse()), http.StatusOK), nil
				}
				return &quesma_api.Result{StatusCode: http.StatusNotFound}, nil
			} else {
				return nil, err
			}
		}
		return elasticsearchQueryResult(string(responseBody), http.StatusOK), nil
	})
	router.Register(routes.TermsEnumPath, and(method("POST"), matchedAgainstPattern(tableResolver)), func(ctx context.Context, req *quesma_api.Request, _ http.ResponseWriter) (*quesma_api.Result, error) {
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

	router.Register(routes.EQLSearch, and(method("GET", "POST"), matchedAgainstPattern(tableResolver)), func(ctx context.Context, req *quesma_api.Request, _ http.ResponseWriter) (*quesma_api.Result, error) {
		body, err := types.ExpectJSON(req.ParsedBody)
		if err != nil {
			return nil, err
		}

		responseBody, err := queryRunner.handleEQLSearch(ctx, req.Params["index"], body)
		if err != nil {
			if errors.Is(quesma_errors.ErrIndexNotExists(), err) {
				return &quesma_api.Result{StatusCode: http.StatusNotFound}, nil
			} else {
				return nil, err
			}
		}
		return elasticsearchQueryResult(string(responseBody), http.StatusOK), nil
	})

	router.Register(routes.IndexPath, and(method("GET", "PUT"), matchedAgainstPattern(tableResolver)), func(ctx context.Context, req *quesma_api.Request, _ http.ResponseWriter) (*quesma_api.Result, error) {

		switch req.Method {

		case "GET":
			index := req.Params["index"]

			foundSchema, found := sr.FindSchema(schema.IndexName(index))
			if !found {
				return &quesma_api.Result{StatusCode: http.StatusNotFound}, nil
			}

			hierarchicalSchema := schema.SchemaToHierarchicalSchema(&foundSchema)
			mappings := elasticsearch.GenerateMappings(hierarchicalSchema)

			return getIndexResult(index, mappings)

		case "PUT":

			index := req.Params["index"]

			err := elasticsearch.IsValidIndexName(index)
			if err != nil {
				return nil, err
			}

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

			sr.UpdateDynamicConfiguration(schema.IndexName(index), schema.Table{Columns: columns})

			return putIndexResult(index)
		}

		return nil, errors.New("unsupported method")
	})

	router.Register(routes.QuesmaTableResolverPath, method("GET"), func(ctx context.Context, req *quesma_api.Request, _ http.ResponseWriter) (*quesma_api.Result, error) {
		indexPattern := req.Params["index"]

		decisions := make(map[string]*quesma_api.Decision)
		humanReadable := make(map[string]string)
		for _, pipeline := range tableResolver.Pipelines() {
			decision := tableResolver.Resolve(pipeline, indexPattern)
			decisions[pipeline] = decision
			humanReadable[pipeline] = decision.String()
		}

		resp := struct {
			IndexPattern  string                          `json:"index_pattern"`
			Decisions     map[string]*quesma_api.Decision `json:"decisions"`
			HumanReadable map[string]string               `json:"human_readable"`
		}{
			IndexPattern:  indexPattern,
			Decisions:     decisions,
			HumanReadable: humanReadable,
		}

		body, err := json.MarshalIndent(resp, "", "  ")
		if err != nil {
			return nil, err
		}

		return &quesma_api.Result{Body: string(body), StatusCode: http.StatusOK, GenericResult: body}, nil
	})

	return router
}

func elasticsearchCountResult(body int64, statusCode int) (*quesma_api.Result, error) {
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
	return &quesma_api.Result{Body: string(serialized), Meta: map[string]any{
		"Content-Type":            "application/json",
		"X-Quesma-Headers-Source": "Quesma",
	}, StatusCode: statusCode,
		GenericResult: serialized}, nil
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

// ElasticsearchQueryResult is a low-effort way to export widely used func without too much refactoring
func ElasticsearchQueryResult(body string, statusCode int) *quesma_api.Result {
	return elasticsearchQueryResult(body, statusCode)
}

func elasticsearchQueryResult(body string, statusCode int) *quesma_api.Result {
	return &quesma_api.Result{Body: body, Meta: map[string]any{
		// TODO copy paste from the original request
		"X-Quesma-Headers-Source": "Quesma",
	}, StatusCode: statusCode,
		GenericResult: []byte(body)}
}

var ingestWarning sync.Once

func noIngestEnabledButThereIngestRequest() {
	logger.Error().Msgf("Ingest is disabled by configuration, but the request is trying to ingest data. ")
}

func bulkInsertResult(ctx context.Context, ops []bulk.BulkItem, err error) (*quesma_api.Result, error) {

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

		return &quesma_api.Result{
			Body:          msg,
			StatusCode:    httpCode,
			GenericResult: []byte(msg),
		}, nil
	}

	if err != nil {
		return &quesma_api.Result{
			Body:          string(queryparser.BadRequestParseError(err)),
			StatusCode:    http.StatusBadRequest,
			GenericResult: queryparser.BadRequestParseError(err),
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

// ElasticsearchInsertResult is a low-effort way to export widely used func without too much refactoring
func ElasticsearchInsertResult(body string, statusCode int) *quesma_api.Result {
	return elasticsearchInsertResult(body, statusCode)
}

func elasticsearchInsertResult(body string, statusCode int) *quesma_api.Result {
	return &quesma_api.Result{Body: body, Meta: map[string]any{
		// TODO copy paste from the original request
		frontend_connectors.ContentTypeHeaderKey: "application/json",
		"X-Quesma-Headers-Source":                "Quesma",
	}, StatusCode: statusCode,
		GenericResult: []byte(body)}
}

func resolveIndexResult(sources elasticsearch.Sources) (*quesma_api.Result, error) {
	if len(sources.Aliases) == 0 && len(sources.DataStreams) == 0 && len(sources.Indices) == 0 {
		return &quesma_api.Result{StatusCode: http.StatusNotFound}, nil
	}

	body, err := json.Marshal(sources)
	if err != nil {
		return nil, err
	}

	return &quesma_api.Result{
		Body:          string(body),
		Meta:          map[string]any{},
		StatusCode:    http.StatusOK,
		GenericResult: body}, nil
}

func indexDocResult(bulkItem bulk.BulkItem) (*quesma_api.Result, error) {
	body, err := json.Marshal(bulkItem.Index)
	if err != nil {
		return nil, err
	}
	return elasticsearchInsertResult(string(body), http.StatusOK), nil
}

func putIndexResult(index string) (*quesma_api.Result, error) {
	result := putIndexResponse{
		Acknowledged:       true,
		ShardsAcknowledged: true,
		Index:              index,
	}
	serialized, err := json.Marshal(result)
	if err != nil {
		return nil, err
	}

	return &quesma_api.Result{StatusCode: http.StatusOK, Body: string(serialized), GenericResult: serialized}, nil
}

func getIndexMappingResult(index string, mappings map[string]any) (*quesma_api.Result, error) {
	result := map[string]any{
		index: map[string]any{
			"mappings": mappings,
		},
	}
	serialized, err := json.Marshal(result)
	if err != nil {
		return nil, err
	}

	return &quesma_api.Result{StatusCode: http.StatusOK, Body: string(serialized), GenericResult: serialized}, nil
}

func getIndexResult(index string, mappings map[string]any) (*quesma_api.Result, error) {
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
