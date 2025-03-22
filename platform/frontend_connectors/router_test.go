// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package frontend_connectors

import (
	"context"
	"errors"
	"fmt"
	"github.com/QuesmaOrg/quesma/platform/backend_connectors"
	"github.com/QuesmaOrg/quesma/platform/clickhouse"
	"github.com/QuesmaOrg/quesma/platform/config"
	"github.com/QuesmaOrg/quesma/platform/elasticsearch"
	quesma_errors "github.com/QuesmaOrg/quesma/platform/errors"
	"github.com/QuesmaOrg/quesma/platform/functionality/bulk"
	"github.com/QuesmaOrg/quesma/platform/functionality/doc"
	"github.com/QuesmaOrg/quesma/platform/functionality/field_capabilities"
	"github.com/QuesmaOrg/quesma/platform/functionality/resolve"
	"github.com/QuesmaOrg/quesma/platform/functionality/terms_enum"
	"github.com/QuesmaOrg/quesma/platform/ingest"
	"github.com/QuesmaOrg/quesma/platform/logger"
	"github.com/QuesmaOrg/quesma/platform/parsers/elastic_query_dsl"
	"github.com/QuesmaOrg/quesma/platform/parsers/painful"
	"github.com/QuesmaOrg/quesma/platform/schema"
	"github.com/QuesmaOrg/quesma/platform/table_resolver"
	"github.com/QuesmaOrg/quesma/platform/telemetry"
	"github.com/QuesmaOrg/quesma/platform/types"
	"github.com/QuesmaOrg/quesma/platform/ui"
	"github.com/QuesmaOrg/quesma/platform/util"
	quesma_api "github.com/QuesmaOrg/quesma/platform/v2/core"
	"github.com/QuesmaOrg/quesma/platform/v2/core/routes"
	"github.com/QuesmaOrg/quesma/platform/v2/core/tracing"
	"github.com/goccy/go-json"
	"github.com/stretchr/testify/assert"
	"net/http"
	"strings"
	"testing"
	"time"
)

var skipMessage = "Skipping test. These will be replaced with table resolver tests."

func configureRouter(cfg *config.QuesmaConfiguration, sr schema.Registry, lm *clickhouse.LogManager, ip *ingest.IngestProcessor, console *ui.QuesmaManagementConsole, phoneHomeAgent telemetry.PhoneHomeAgent, queryRunner *QueryRunner, tableResolver table_resolver.TableResolver, elasticsearchConnector *backend_connectors.ElasticsearchBackendConnector) *quesma_api.PathRouter {

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

	router.Register(routes.BulkPath, method("POST", "PUT"), func(ctx context.Context, req *quesma_api.Request, _ http.ResponseWriter) (*quesma_api.Result, error) {

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
				Body:          string(elastic_query_dsl.BadRequestParseError(err)),
				StatusCode:    http.StatusBadRequest,
				GenericResult: elastic_query_dsl.BadRequestParseError(err),
			}, nil
		}

		result, err := doc.Write(ctx, &index, body, ip, cfg.IngestStatistics, phoneHomeAgent, tableResolver, elasticsearchConnector)
		if err != nil {
			return &quesma_api.Result{
				Body:          string(elastic_query_dsl.BadRequestParseError(err)),
				StatusCode:    http.StatusBadRequest,
				GenericResult: elastic_query_dsl.BadRequestParseError(err),
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
		ir := elasticsearch.NewIndexResolver(cfg.Elasticsearch)
		sources, err := resolve.HandleResolve(req.Params["index"], sr, ir)
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
					Body:          string(elastic_query_dsl.BadRequestParseError(err)),
					StatusCode:    http.StatusBadRequest,
					GenericResult: elastic_query_dsl.BadRequestParseError(err),
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
					Body:          string(elastic_query_dsl.BadRequestParseError(err)),
					StatusCode:    http.StatusBadRequest,
					GenericResult: elastic_query_dsl.BadRequestParseError(err),
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
					Body:          string(elastic_query_dsl.BadRequestParseError(err)),
					StatusCode:    http.StatusBadRequest,
					GenericResult: elastic_query_dsl.BadRequestParseError(err),
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
		return nil, errors.New("EQL is not supported yet")
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

func Test_matchedAgainstConfig(t *testing.T) {

	t.Skip(skipMessage)

	tests := []struct {
		name   string
		index  string
		body   string
		config config.QuesmaConfiguration
		want   bool
	}{
		{
			name:   "index enabled",
			index:  "index",
			config: indexConfig("index", false),
			want:   true,
		},
		{
			name:   "index disabled",
			index:  "index",
			config: indexConfig("index", true),
			want:   false,
		},
		{
			name:   "index not configured",
			index:  "index",
			config: indexConfig("logs", true),
			want:   false,
		},
	}

	resolver := table_resolver.NewEmptyTableResolver()

	for i, tt := range tests {
		t.Run(util.PrettyTestName(tt.name, i), func(t *testing.T) {

			req := &quesma_api.Request{Params: map[string]string{"index": tt.index}, Body: tt.body}
			res := matchedExactQueryPath(resolver).Matches(req)

			assert.Equalf(t, tt.want, res.Matched, "matchedExactQueryPath(%v), index: %s, desision %s", tt.config, tt.index, res.Decision)
		})
	}
}

func Test_matchedAgainstPattern(t *testing.T) {

	t.Skip(skipMessage)

	tests := []struct {
		name          string
		pattern       string
		body          string
		configuration config.QuesmaConfiguration
		registry      schema.Registry
		want          bool
	}{
		{
			name:          "multiple indexes, one non-wildcard matches configuration",
			pattern:       "logs-1,logs-2,foo-*,index",
			configuration: indexConfig("index", false),
			registry:      &schema.StaticRegistry{},
			want:          true,
		},
		{
			name:          "multiple indexes, one wildcard matches configuration",
			pattern:       "logs-1,logs-2,foo-*,index",
			configuration: indexConfig("foo-5", false),
			registry:      &schema.StaticRegistry{},
			want:          true,
		},
		{
			name:          "multiple indexes, one internal",
			pattern:       "index,.kibana",
			configuration: indexConfig("index", false),
			registry:      &schema.StaticRegistry{},
			want:          false,
		},
		{
			name:          "index explicitly enabled",
			pattern:       "index",
			configuration: indexConfig("index", false),
			registry:      &schema.StaticRegistry{},
			want:          true,
		},
		{
			name:          "index explicitly disabled",
			pattern:       "index",
			configuration: indexConfig("index", true),
			registry:      &schema.StaticRegistry{},
			want:          false,
		},
		{
			name:          "index enabled, * pattern",
			pattern:       "*",
			configuration: indexConfig("logs-generic-default", false),
			registry:      &schema.StaticRegistry{},
			want:          true,
		},
		{
			name:          "index enabled, _all pattern",
			pattern:       "_all",
			configuration: indexConfig("logs-generic-default", false),
			registry:      &schema.StaticRegistry{},
			want:          true,
		},
		{
			name:          "index enabled, multiple patterns",
			pattern:       "logs-*-*, logs-*",
			configuration: indexConfig("logs-generic-default", false),
			registry:      &schema.StaticRegistry{},
			want:          true,
		},
		{
			name:          "index enabled, multiple patterns",
			pattern:       "logs-*-*, logs-generic-default",
			configuration: indexConfig("logs-generic-default", false),
			registry:      &schema.StaticRegistry{},
			want:          true,
		},
		{
			name:          "index disabled, wide pattern",
			pattern:       "logs-*-*",
			configuration: indexConfig("logs-generic-default", true),
			registry:      &schema.StaticRegistry{},
			want:          false,
		},
		{
			name:          "index enabled, narrow pattern",
			pattern:       "logs-generic-*",
			configuration: indexConfig("logs-generic-default", false),
			registry:      &schema.StaticRegistry{},
			want:          true,
		},
		{
			name:          "logs-elastic_agent-*",
			pattern:       "logs-elastic_agent-*",
			configuration: indexConfig("logs-generic-default", false),
			registry:      &schema.StaticRegistry{},
			want:          false,
		},
		{
			name:          "traces-apm*, not configured",
			pattern:       "traces-apm*",
			configuration: indexConfig("logs-generic-default", false),
			registry:      &schema.StaticRegistry{},
			want:          false,
		},
		{
			name:          "index autodiscovery (non-wildcard)",
			pattern:       "my_index",
			configuration: withAutodiscovery(indexConfig("another-index", false)),
			registry: &schema.StaticRegistry{
				Tables: map[schema.IndexName]schema.Schema{
					"my_index": {ExistsInDataSource: true},
				},
			},
			want: true,
		},
		{
			name:          "index autodiscovery (wildcard)",
			pattern:       "my_index*",
			configuration: withAutodiscovery(indexConfig("another-index", false)),
			registry: &schema.StaticRegistry{
				Tables: map[schema.IndexName]schema.Schema{
					"my_index8": {ExistsInDataSource: true},
				},
			},
			want: true,
		},
	}

	resolver := table_resolver.NewEmptyTableResolver()

	for i, tt := range tests {
		t.Run(util.PrettyTestName(tt.name, i), func(t *testing.T) {

			req := &quesma_api.Request{Params: map[string]string{"index": tt.pattern}, Body: tt.body}
			assert.Equalf(t, tt.want, matchedAgainstPattern(resolver).Matches(req).Matched, "matchedAgainstPattern(%v)", tt.configuration)
		})
	}
}

func indexConfig(name string, elastic bool) config.QuesmaConfiguration {
	var targets []string
	if elastic {
		targets = []string{config.ElasticsearchTarget}
	} else {
		targets = []string{config.ClickhouseTarget}
	}
	return config.QuesmaConfiguration{IndexConfig: map[string]config.IndexConfiguration{name: {QueryTarget: targets, IngestTarget: targets}}}
}

func withAutodiscovery(cfg config.QuesmaConfiguration) config.QuesmaConfiguration {
	cfg.AutodiscoveryEnabled = true
	return cfg
}

const testIndexName = "indexName"

func TestConfigureRouter(t *testing.T) {
	cfg := &config.QuesmaConfiguration{
		IndexConfig: map[string]config.IndexConfiguration{
			testIndexName: {},
		},
	}
	tr := TestTableResolver{}

	schemaRegistry := schema.NewSchemaRegistry(fixedTableProvider{}, cfg, clickhouse.SchemaTypeAdapter{})
	schemaRegistry.Start()
	defer schemaRegistry.Stop()

	testRouter := configureRouter(cfg, schemaRegistry, &clickhouse.LogManager{}, &ingest.IngestProcessor{}, &ui.QuesmaManagementConsole{}, telemetry.NewPhoneHomeAgent(cfg, nil, ""), &QueryRunner{}, tr, nil)
	tests := []struct {
		path                string
		method              string
		shouldReturnHandler bool
	}{
		// Routes explicitly registered in the router code
		{routes.ClusterHealthPath, "GET", true},
		// {routes.BulkPath, "POST", true}, // TODO later on, it requires body parsing
		{routes.IndexRefreshPath, "POST", true},
		{routes.IndexDocPath, "POST", true},
		{routes.IndexBulkPath, "POST", true},
		{routes.IndexBulkPath, "PUT", true},
		{routes.ResolveIndexPath, "GET", true},
		{routes.IndexCountPath, "GET", true},
		{routes.GlobalSearchPath, "GET", false},
		{routes.GlobalSearchPath, "POST", false},
		{routes.GlobalSearchPath, "PUT", false},
		{routes.IndexSearchPath, "GET", true},
		{routes.IndexSearchPath, "POST", true},
		{routes.IndexAsyncSearchPath, "POST", true},
		{routes.IndexMappingPath, "PUT", true},
		{routes.IndexMappingPath, "GET", true},
		{routes.AsyncSearchStatusPath, "GET", true},
		{routes.AsyncSearchIdPath, "GET", true},
		{routes.AsyncSearchIdPath, "DELETE", true},
		{routes.FieldCapsPath, "GET", true},
		{routes.FieldCapsPath, "POST", true},
		{routes.TermsEnumPath, "POST", true},
		{routes.EQLSearch, "GET", true},
		{routes.EQLSearch, "POST", true},
		{routes.IndexPath, "PUT", true},
		{routes.IndexPath, "GET", true},
		{routes.QuesmaTableResolverPath, "GET", true},
		// Few cases where the router should not match
		{"/invalid/path", "GET", false},
		{routes.ClusterHealthPath, "POST", false},
		//{routes.BulkPath, "GET", false}, // TODO later on, it requires body parsing
		{routes.IndexRefreshPath, "GET", false},
		{routes.IndexDocPath, "GET", false},
		{routes.IndexBulkPath, "DELETE", false},
		{routes.ResolveIndexPath, "POST", false},
		{routes.IndexCountPath, "POST", false},
		{routes.IndexSearchPath, "DELETE", false},
		{routes.IndexAsyncSearchPath, "GET", false},
		{routes.IndexMappingPath, "POST", false},
		{routes.AsyncSearchStatusPath, "POST", false},
		{routes.AsyncSearchIdPath, "PUT", false},
		{routes.FieldCapsPath, "DELETE", false},
		{routes.TermsEnumPath, "GET", false},
		{routes.EQLSearch, "DELETE", false},
		{routes.IndexPath, "POST", false},
		{routes.QuesmaTableResolverPath, "POST", false},
		{routes.QuesmaTableResolverPath, "PUT", false},
		{routes.QuesmaTableResolverPath, "DELETE", false},
	}

	for i, tt := range tests {
		tt.path = strings.Replace(tt.path, ":id", "quesma_async_absurd_test_id", -1)
		tt.path = strings.Replace(tt.path, ":index", testIndexName, -1)
		t.Run(util.PrettyTestName(fmt.Sprintf("%s-at-%s", tt.method, tt.path), i), func(t *testing.T) {
			req := &quesma_api.Request{Path: tt.path, Method: tt.method}
			reqHandler, _ := testRouter.Matches(req)
			assert.Equal(t, tt.shouldReturnHandler, reqHandler != nil, "Expected route match result for path: %s and method: %s", tt.path, tt.method)
		})
	}
}

// TestTableResolver should be used only within tests
type TestTableResolver struct{}

func (t TestTableResolver) Start() {}

func (t TestTableResolver) Stop() {}

func (t TestTableResolver) Resolve(_ string, indexPattern string) *quesma_api.Decision {
	if indexPattern == testIndexName {
		return &quesma_api.Decision{
			UseConnectors: []quesma_api.ConnectorDecision{
				&quesma_api.ConnectorDecisionClickhouse{},
			},
		}
	} else {
		return &quesma_api.Decision{
			Err:          fmt.Errorf("TestTableResolver err"),
			Reason:       "TestTableResolver reason",
			ResolverName: "TestTableResolver",
		}
	}
}

func (t TestTableResolver) Pipelines() []string { return []string{} }

func (t TestTableResolver) RecentDecisions() []quesma_api.PatternDecisions {
	return []quesma_api.PatternDecisions{}
}
