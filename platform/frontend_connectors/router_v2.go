// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package frontend_connectors

import (
	"context"
	"errors"
	"github.com/QuesmaOrg/quesma/platform/backend_connectors"
	"github.com/QuesmaOrg/quesma/platform/clickhouse"
	"github.com/QuesmaOrg/quesma/platform/config"
	"github.com/QuesmaOrg/quesma/platform/errors"
	"github.com/QuesmaOrg/quesma/platform/ingest"
	"github.com/QuesmaOrg/quesma/platform/logger"
	"github.com/QuesmaOrg/quesma/platform/parsers/elastic_query_dsl"
	"github.com/QuesmaOrg/quesma/platform/parsers/painful"
	"github.com/QuesmaOrg/quesma/platform/schema"
	"github.com/QuesmaOrg/quesma/platform/table_resolver"
	quesma_api "github.com/QuesmaOrg/quesma/platform/v2/core"
	"github.com/QuesmaOrg/quesma/platform/v2/core/routes"
	"github.com/QuesmaOrg/quesma/platform/v2/core/types"
	"github.com/goccy/go-json"
	"net/http"
	"strings"
	"time"
)

func ConfigureIngestRouterV2(cfg *config.QuesmaConfiguration, dependencies quesma_api.Dependencies, ip *ingest.IngestProcessor, tableResolver table_resolver.TableResolver, esConn *backend_connectors.ElasticsearchBackendConnector) quesma_api.Router {
	// some syntactic sugar
	method := quesma_api.IsHTTPMethod
	and := quesma_api.And

	router := quesma_api.NewPathRouter()

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

	router.Register(routes.BulkPath, method("POST", "PUT"), func(ctx context.Context, req *quesma_api.Request, _ http.ResponseWriter) (*quesma_api.Result, error) {
		body, err := types.ExpectNDJSON(req.ParsedBody)
		if err != nil {
			return nil, err
		}
		return HandleBulk(ctx, body, ip, cfg.IngestStatistics, esConn, dependencies, tableResolver)
	})
	router.Register(routes.IndexDocPath, and(method("POST"), matchedExactIngestPath(tableResolver)), func(ctx context.Context, req *quesma_api.Request, _ http.ResponseWriter) (*quesma_api.Result, error) {
		index := req.Params["index"]

		body, err := types.ExpectJSON(req.ParsedBody)
		if err != nil {
			return &quesma_api.Result{
				Body:          string(elastic_query_dsl.BadRequestParseError(err)),
				StatusCode:    http.StatusBadRequest,
				GenericResult: elastic_query_dsl.BadRequestParseError(err),
			}, nil
		}

		return HandleIndexDoc(ctx, index, body, ip, cfg.IngestStatistics, esConn, dependencies, tableResolver)
	})

	router.Register(routes.IndexBulkPath, and(method("POST", "PUT"), matchedExactIngestPath(tableResolver)), func(ctx context.Context, req *quesma_api.Request, _ http.ResponseWriter) (*quesma_api.Result, error) {
		index := req.Params["index"]

		body, err := types.ExpectNDJSON(req.ParsedBody)
		if err != nil {
			return nil, err
		}

		return HandleBulkIndex(ctx, index, body, ip, cfg.IngestStatistics, esConn, dependencies, tableResolver)
	})
	return router
}

func ConfigureSearchRouterV2(cfg *config.QuesmaConfiguration, dependencies quesma_api.Dependencies, sr schema.Registry, lm *clickhouse.LogManager, queryRunner *QueryRunner, tableResolver table_resolver.TableResolver) quesma_api.Router {

	// some syntactic sugar
	method := quesma_api.IsHTTPMethod
	and := quesma_api.And

	router := quesma_api.NewPathRouter()

	// These are the endpoints that are supported by Quesma

	// Warning:
	// The first handler that matches the path will be considered to use.
	// If the predicate returns false it will be redirected to the elastic cluster.
	// If the predicate returns true, the handler will be used.
	//
	// So, if you add multiple handlers with the same path, the first one will be used, the rest will be redirected to the elastic cluster.
	// This is current limitation of the router.

	router.Register(routes.ClusterHealthPath, method("GET"), func(_ context.Context, req *quesma_api.Request, _ http.ResponseWriter) (*quesma_api.Result, error) {
		return HandleClusterHealth()
	})

	router.Register(routes.IndexRefreshPath, and(method("POST"), matchedExactQueryPath(tableResolver)), func(ctx context.Context, req *quesma_api.Request, _ http.ResponseWriter) (*quesma_api.Result, error) {
		return HandleIndexRefresh()
	})

	router.Register(routes.ResolveIndexPath, method("GET"), func(ctx context.Context, req *quesma_api.Request, _ http.ResponseWriter) (*quesma_api.Result, error) {
		return HandleResolveIndex(ctx, req.Params["index"], sr, cfg.Elasticsearch)
	})

	router.Register(routes.IndexPatternPitPath, and(method("POST"), matchedAgainstPattern(tableResolver)), func(ctx context.Context, req *quesma_api.Request, _ http.ResponseWriter) (*quesma_api.Result, error) {
		indexPattern := req.Params["index"]
		logger.Debug().Msgf("Quesma-managed PIT request, targeting indexPattern=%s", indexPattern)
		return HandlePitStore(indexPattern)
	})

	router.Register(routes.PitPath, and(method("DELETE"), hasQuesmaPitId()), func(ctx context.Context, req *quesma_api.Request, _ http.ResponseWriter) (*quesma_api.Result, error) {
		return PitDeletedResponse()
	})

	router.Register(routes.IndexCountPath, and(method("GET"), matchedAgainstPattern(tableResolver)), func(ctx context.Context, req *quesma_api.Request, _ http.ResponseWriter) (*quesma_api.Result, error) {
		return HandleIndexCount(ctx, req.Params["index"], queryRunner)
	})

	router.Register(routes.GlobalSearchPath, and(method("GET", "POST"), isSearchRequestWithQuesmaPit()), func(ctx context.Context, req *quesma_api.Request, _ http.ResponseWriter) (*quesma_api.Result, error) {
		pitId := getPitIdFromRequest(req, false)
		indexPattern := strings.TrimPrefix(pitId, quesmaPitPrefix)

		body, err := types.ExpectJSON(req.ParsedBody)
		if err != nil {
			return nil, err
		}

		// TODO we should pass JSON here instead of []byte
		responseBody, err := queryRunner.HandleSearch(ctx, indexPattern, body)
		if err != nil {
			if errors.Is(quesma_errors.ErrIndexNotExists(), err) {
				return &quesma_api.Result{StatusCode: http.StatusNotFound, GenericResult: make([]byte, 0)}, nil
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
		return HandleIndexSearch(ctx, req.Params["index"], body, queryRunner)
	})

	router.Register(routes.IndexAsyncSearchPath, and(method("POST"), matchedAgainstPattern(tableResolver)), func(ctx context.Context, req *quesma_api.Request, _ http.ResponseWriter) (*quesma_api.Result, error) {
		query, err := types.ExpectJSON(req.ParsedBody)
		if err != nil {
			return nil, err
		}
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

		return HandleIndexAsyncSearch(ctx, req.Params["index"], query, waitForResultsMs, keepOnCompletion, queryRunner)
	})

	router.Register(routes.IndexMsearchPath, and(method("GET", "POST"), matchedAgainstPattern(tableResolver)), func(ctx context.Context, req *quesma_api.Request, _ http.ResponseWriter) (*quesma_api.Result, error) {
		return HandleMultiSearch(ctx, req, req.Params["index"], queryRunner)
	})

	router.Register(routes.GlobalMsearchPath, and(method("GET", "POST"), quesma_api.Always()), func(ctx context.Context, req *quesma_api.Request, _ http.ResponseWriter) (*quesma_api.Result, error) {
		return HandleMultiSearch(ctx, req, "", queryRunner)
	})

	router.Register(routes.IndexMappingPath, and(method("GET", "PUT"), matchAgainstTableResolver(tableResolver, quesma_api.MetaPipeline)), func(ctx context.Context, req *quesma_api.Request, _ http.ResponseWriter) (*quesma_api.Result, error) {
		index := req.Params["index"]
		switch req.Method {
		case "GET":
			return HandleGetIndexMapping(ctx, sr, lm, index)
		case "PUT":
			if body, err := types.ExpectJSON(req.ParsedBody); err != nil {
				return nil, err
			} else {
				return HandlePutIndex(index, body, sr)
			}
		}
		return nil, errors.New("unsupported method")
	})

	router.Register(routes.AsyncSearchStatusPath, and(method("GET"), matchedAgainstAsyncId()), func(ctx context.Context, req *quesma_api.Request, _ http.ResponseWriter) (*quesma_api.Result, error) {
		return HandleAsyncSearchStatus(ctx, req.Params["id"], queryRunner)
	})

	router.Register(routes.AsyncSearchIdPath, and(method("GET", "DELETE"), matchedAgainstAsyncId()), func(ctx context.Context, req *quesma_api.Request, _ http.ResponseWriter) (*quesma_api.Result, error) {
		switch req.Method {
		case "GET":
			return HandleGettingAsyncSearchById(ctx, req.Params["id"], queryRunner)
		case "DELETE":
			return HandleDeletingAsyncSearchById(queryRunner, req.Params["id"])
		}
		return nil, errors.New("unsupported method")
	})

	router.Register(routes.FieldCapsPath, and(method("GET", "POST"), matchAgainstTableResolver(tableResolver, quesma_api.MetaPipeline)), func(ctx context.Context, req *quesma_api.Request, _ http.ResponseWriter) (*quesma_api.Result, error) {
		return HandleFieldCaps(ctx, req.Params["index"],
			req.QueryParams.Get("allow_no_indices") == "true",
			req.QueryParams.Get("ignore_unavailable") == "true",
			cfg.IndexConfig, sr, lm)
	})
	router.Register(routes.TermsEnumPath, and(method("POST"), matchedAgainstPattern(tableResolver)), func(ctx context.Context, req *quesma_api.Request, _ http.ResponseWriter) (*quesma_api.Result, error) {
		indexPattern := req.Params["index"]
		if strings.Contains(indexPattern, ",") {
			return nil, errors.New("multi index terms enum is not yet supported")
		}
		body, err := types.ExpectJSON(req.ParsedBody)
		if err != nil {
			return nil, errors.New("invalid request body, expecting JSON")
		}

		var isFieldMapSyntaxEnabled bool
		if indexCfg, exists := cfg.IndexConfig[indexPattern]; exists {
			isFieldMapSyntaxEnabled = indexCfg.EnableFieldMapSyntax
		}
		return HandleTermsEnum(ctx, indexPattern, body, isFieldMapSyntaxEnabled, queryRunner)
	})

	router.Register(routes.EQLSearch, and(method("GET", "POST"), matchedAgainstPattern(tableResolver)), func(ctx context.Context, req *quesma_api.Request, _ http.ResponseWriter) (*quesma_api.Result, error) {
		return nil, errors.New("EQL is not supported")
	})

	router.Register(routes.IndexPath, and(method("GET", "PUT"), matchedAgainstPattern(tableResolver)), func(ctx context.Context, req *quesma_api.Request, _ http.ResponseWriter) (*quesma_api.Result, error) {
		index := req.Params["index"]
		switch req.Method {
		case "GET":
			return HandleGetIndex(sr, index)
		case "PUT":
			if req.Body == "" {
				return HandlePutIndex(index, types.JSON{}, sr)
			}
			if body, err := types.ExpectJSON(req.ParsedBody); err != nil {
				return nil, err
			} else {
				return HandlePutIndex(index, body, sr)
			}
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

	router.Register(routes.QuesmaReloadTablsPath, method("POST"), func(ctx context.Context, req *quesma_api.Request, _ http.ResponseWriter) (*quesma_api.Result, error) {

		lm.ReloadTables()

		return &quesma_api.Result{
			Body:          "Table reloaded successfully",
			StatusCode:    http.StatusOK,
			GenericResult: []byte("Table reloaded successfully"),
		}, nil
	})

	return router
}
