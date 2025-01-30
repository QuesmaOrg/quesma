// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package quesma

import (
	"context"
	"errors"
	"fmt"
	"github.com/QuesmaOrg/quesma/quesma/ab_testing"
	"github.com/QuesmaOrg/quesma/quesma/clickhouse"
	"github.com/QuesmaOrg/quesma/quesma/common_table"
	"github.com/QuesmaOrg/quesma/quesma/elasticsearch"
	"github.com/QuesmaOrg/quesma/quesma/end_user_errors"
	"github.com/QuesmaOrg/quesma/quesma/logger"
	"github.com/QuesmaOrg/quesma/quesma/model"
	"github.com/QuesmaOrg/quesma/quesma/optimize"
	"github.com/QuesmaOrg/quesma/quesma/painful"
	"github.com/QuesmaOrg/quesma/quesma/queryparser"
	"github.com/QuesmaOrg/quesma/quesma/quesma/async_search_storage"
	"github.com/QuesmaOrg/quesma/quesma/quesma/config"
	"github.com/QuesmaOrg/quesma/quesma/quesma/errors"
	"github.com/QuesmaOrg/quesma/quesma/quesma/recovery"
	"github.com/QuesmaOrg/quesma/quesma/quesma/types"
	"github.com/QuesmaOrg/quesma/quesma/quesma/ui"
	"github.com/QuesmaOrg/quesma/quesma/schema"
	"github.com/QuesmaOrg/quesma/quesma/table_resolver"
	"github.com/QuesmaOrg/quesma/quesma/util"
	"github.com/QuesmaOrg/quesma/quesma/v2/core"
	"github.com/QuesmaOrg/quesma/quesma/v2/core/diag"
	"github.com/QuesmaOrg/quesma/quesma/v2/core/tracing"
	"github.com/goccy/go-json"
	"net/http"
	"strings"
	"sync/atomic"
	"time"
)

const (
	asyncQueriesLimit      = 10000
	asyncQueriesLimitBytes = 1024 * 1024 * 500 // 500MB

	maxParallelQueries = 25 // maximum of parallel queries we can, this is arbitrary value and should be adjusted
)

type QueryRunner struct {
	executionCtx         context.Context
	cancel               context.CancelFunc
	AsyncRequestStorage  async_search_storage.AsyncRequestResultStorage
	AsyncQueriesContexts async_search_storage.AsyncQueryContextStorage
	logManager           clickhouse.LogManagerIFace
	cfg                  *config.QuesmaConfiguration
	debugInfoCollector   diag.DebugInfoCollector

	tableDiscovery clickhouse.TableDiscovery
	// configuration

	// this is passed to the QueryTranslator to render date math expressions
	DateMathRenderer         string // "clickhouse_interval" or "literal"  if not set, we use "clickhouse_interval"
	currentParallelQueryJobs atomic.Int64
	transformationPipeline   TransformationPipeline
	schemaRegistry           schema.Registry
	ABResultsSender          ab_testing.Sender
	tableResolver            table_resolver.TableResolver

	maxParallelQueries int // if set to 0, we run queries in sequence, it's fine for testing purposes
}

// QueryRunnerIFace is a temporary interface to bridge gap between QueryRunner and QueryRunner2 in `router_v2.go`.
// moving forwards as we remove two implementations we might look at making all these methods private again.
type QueryRunnerIFace interface {
	HandleSearch(ctx context.Context, indexPattern string, body types.JSON) ([]byte, error)
	HandleAsyncSearch(ctx context.Context, indexPattern string, body types.JSON, waitForResultsMs int, keepOnCompletion bool) ([]byte, error)
	HandleAsyncSearchStatus(_ context.Context, id string) ([]byte, error)
	HandleCount(ctx context.Context, indexPattern string) (int64, error)
	// Todo: consider removing this getters for these two below, this was required for temporary Field Caps impl in v2 api
	GetSchemaRegistry() schema.Registry
	GetLogManager() clickhouse.LogManagerIFace
	DeleteAsyncSearch(id string) ([]byte, error)
	HandlePartialAsyncSearch(ctx context.Context, id string) ([]byte, error)
}

func (q *QueryRunner) EnableQueryOptimization(cfg *config.QuesmaConfiguration) {
	q.transformationPipeline.transformers = append(q.transformationPipeline.transformers, optimize.NewOptimizePipeline(cfg))
}

func NewQueryRunner(lm clickhouse.LogManagerIFace,
	cfg *config.QuesmaConfiguration,
	qmc diag.DebugInfoCollector,
	schemaRegistry schema.Registry,
	abResultsRepository ab_testing.Sender,
	resolver table_resolver.TableResolver,
	tableDiscovery clickhouse.TableDiscovery) *QueryRunner {

	ctx, cancel := context.WithCancel(context.Background())

	return &QueryRunner{logManager: lm, cfg: cfg, debugInfoCollector: qmc,
		executionCtx: ctx, cancel: cancel,
		AsyncRequestStorage:  async_search_storage.NewAsyncSearchStorageInMemory(),
		AsyncQueriesContexts: async_search_storage.NewAsyncQueryContextStorageInMemory(),
		transformationPipeline: TransformationPipeline{
			transformers: []model.QueryTransformer{NewSchemaCheckPass(cfg, tableDiscovery, defaultSearchAfterStrategy)},
		},
		schemaRegistry:     schemaRegistry,
		ABResultsSender:    abResultsRepository,
		tableResolver:      resolver,
		tableDiscovery:     tableDiscovery,
		maxParallelQueries: maxParallelQueries,
	}
}

func (q *QueryRunner) GetSchemaRegistry() schema.Registry {
	return q.schemaRegistry
}

func (q *QueryRunner) GetLogManager() clickhouse.LogManagerIFace {
	return q.logManager
}

func NewQueryRunnerDefaultForTests(db quesma_api.BackendConnector, cfg *config.QuesmaConfiguration,
	tableName string, tables *clickhouse.TableMap, staticRegistry *schema.StaticRegistry) *QueryRunner {

	lm := clickhouse.NewLogManagerWithConnection(db, tables)
	logChan := logger.InitOnlyChannelLoggerForTests()

	resolver := table_resolver.NewEmptyTableResolver()
	resolver.Decisions[tableName] = &quesma_api.Decision{
		UseConnectors: []quesma_api.ConnectorDecision{
			&quesma_api.ConnectorDecisionClickhouse{
				ClickhouseTableName: tableName,
				ClickhouseIndexes:   []string{tableName},
			},
		},
	}

	tableDiscovery := clickhouse.NewEmptyTableDiscovery()
	tableDiscovery.TableMap = tables

	managementConsole := ui.NewQuesmaManagementConsole(cfg, nil, logChan, diag.EmptyPhoneHomeRecentStatsProvider(), nil, resolver)

	go managementConsole.RunOnlyChannelProcessor()

	return NewQueryRunner(lm, cfg, managementConsole, staticRegistry, ab_testing.NewEmptySender(), resolver, tableDiscovery)
}

// HandleCount returns -1 when table name could not be resolved
func (q *QueryRunner) HandleCount(ctx context.Context, indexPattern string) (int64, error) {
	indexes, err := q.logManager.ResolveIndexPattern(ctx, q.schemaRegistry, indexPattern)
	if err != nil {
		return 0, err
	}
	if len(indexes) == 0 {
		if elasticsearch.IsIndexPattern(indexPattern) {
			return 0, nil
		} else {
			logger.WarnWithCtx(ctx).Msgf("could not resolve table name for [%s]", indexPattern)
			return -1, quesma_errors.ErrIndexNotExists()
		}
	}

	if len(indexes) == 1 {
		return q.logManager.Count(ctx, indexes[0])
	} else {
		return q.logManager.CountMultiple(ctx, indexes...)
	}
}

func (q *QueryRunner) HandleMultiSearch(ctx context.Context, defaultIndexName string, body types.NDJSON) ([]byte, error) {

	type msearchQuery struct {
		indexName string
		query     types.JSON
	}

	var queries []msearchQuery

	var currentQuery *msearchQuery

	for _, line := range body {

		if currentQuery == nil {
			currentQuery = &msearchQuery{}

			if v, ok := line["index"].(string); ok {
				currentQuery.indexName = v
			} else {
				currentQuery.indexName = defaultIndexName
			}
			continue
		}

		newQuery := types.JSON{}

		if query, ok := line["query"]; ok {
			newQuery["query"] = query
		} else {
			return nil, fmt.Errorf("query parameter not found")
		}

		if aggs, ok := line["aggs"]; ok {
			newQuery["aggs"] = aggs
		}
		if size, ok := line["size"]; ok {
			newQuery["size"] = size
		}
		if from, ok := line["from"]; ok {
			newQuery["from"] = from
		}

		currentQuery.query = newQuery
		queries = append(queries, *currentQuery)
		currentQuery = nil

	}

	var responses []any

	for _, query := range queries {

		// TODO ask table resolver here and go to the right connector or connectors

		responseBody, err := q.HandleSearch(ctx, query.indexName, query.query)

		if err != nil {

			var wrappedErr any

			// TODO check if it's correct implementation

			if errors.Is(quesma_errors.ErrIndexNotExists(), err) {
				wrappedErr = &quesma_api.Result{StatusCode: http.StatusNotFound}
			} else if errors.Is(err, quesma_errors.ErrCouldNotParseRequest()) {
				wrappedErr = &quesma_api.Result{
					Body:          string(queryparser.BadRequestParseError(err)),
					StatusCode:    http.StatusBadRequest,
					GenericResult: queryparser.BadRequestParseError(err),
				}
			} else {
				logger.ErrorWithCtx(ctx).Msgf("error handling multisearch: %v", err)
				wrappedErr = &quesma_api.Result{
					Body:          "Internal error",
					StatusCode:    http.StatusInternalServerError,
					GenericResult: queryparser.BadRequestParseError(err),
				}
			}

			responses = append(responses, wrappedErr)
		} else {

			parsedResponseBody, err := types.ParseJSON(string(responseBody))
			if err != nil {
				return nil, err
			}
			responses = append(responses, parsedResponseBody)
		}

	}

	type msearchResponse struct {
		Responses []any `json:"responses"`
	}

	resp := msearchResponse{Responses: responses}

	responseBody, err := json.Marshal(resp)
	if err != nil {
		return nil, err
	}

	return responseBody, nil
}

func (q *QueryRunner) HandleSearch(ctx context.Context, indexPattern string, body types.JSON) ([]byte, error) {
	return q.handleSearchCommon(ctx, indexPattern, body, nil)
}

func (q *QueryRunner) HandleAsyncSearch(ctx context.Context, indexPattern string, body types.JSON,
	waitForResultsMs int, keepOnCompletion bool) ([]byte, error) {
	async := AsyncQuery{
		asyncId:          tracing.GetAsyncId(),
		waitForResultsMs: waitForResultsMs,
		keepOnCompletion: keepOnCompletion,
		startTime:        time.Now(),
	}
	ctx = context.WithValue(ctx, tracing.AsyncIdCtxKey, async.asyncId)
	logger.InfoWithCtx(ctx).Msgf("async search request id: %s started", async.asyncId)
	return q.handleSearchCommon(ctx, indexPattern, body, &async)
}

type asyncSearchWithError struct {
	response            *model.SearchResp
	translatedQueryBody []diag.TranslatedSQLQuery
	err                 error
}

type AsyncQuery struct {
	asyncId          string
	waitForResultsMs int
	keepOnCompletion bool
	startTime        time.Time
}

func (q *QueryRunner) transformQueries(ctx context.Context, plan *model.ExecutionPlan) error {
	var err error
	plan.Queries, err = q.transformationPipeline.Transform(plan.Queries)
	if err != nil {
		return fmt.Errorf("error transforming queries: %v", err)
	}
	return nil
}

func (q *QueryRunner) runExecutePlanAsync(ctx context.Context, plan *model.ExecutionPlan, queryTranslator IQueryTranslator, table *clickhouse.Table, doneCh chan asyncSearchWithError, optAsync *AsyncQuery) {
	go func() {
		defer recovery.LogAndHandlePanic(ctx, func(err error) {
			doneCh <- asyncSearchWithError{err: err}
		})

		translatedQueryBody, results, err := q.searchWorker(ctx, plan, table, doneCh, optAsync)
		if err != nil {
			doneCh <- asyncSearchWithError{translatedQueryBody: translatedQueryBody, err: err}
			return
		}

		if len(plan.Queries) > 0 && len(results) == 0 {
			// if there are no queries, empty results are fine
			logger.ErrorWithCtx(ctx).Msgf("no hits, sqls: %v", translatedQueryBody)
			doneCh <- asyncSearchWithError{translatedQueryBody: translatedQueryBody, err: errors.New("no hits")}
			return
		}

		results, err = q.postProcessResults(plan, results)
		if err != nil {
			doneCh <- asyncSearchWithError{translatedQueryBody: translatedQueryBody, err: err}
		}

		searchResponse := queryTranslator.MakeSearchResponse(plan.Queries, results)

		doneCh <- asyncSearchWithError{response: searchResponse, translatedQueryBody: translatedQueryBody, err: err}
	}()
}

func (q *QueryRunner) executePlan(ctx context.Context, plan *model.ExecutionPlan, queryTranslator IQueryTranslator, table *clickhouse.Table, body types.JSON, optAsync *AsyncQuery, optComparePlansCh chan<- executionPlanResult, abTestingMainPlan bool) (responseBody []byte, err error) {
	contextValues := tracing.ExtractValues(ctx)
	id := contextValues.RequestId
	path := contextValues.RequestPath
	opaqueId := contextValues.OpaqueId

	doneCh := make(chan asyncSearchWithError, 1)

	sendMainPlanResult := func(responseBody []byte, err error) {
		if optComparePlansCh != nil {
			optComparePlansCh <- executionPlanResult{
				isMain:       abTestingMainPlan,
				plan:         plan,
				err:          err,
				responseBody: responseBody,
				endTime:      time.Now(),
			}
		}
	}

	err = q.transformQueries(ctx, plan)
	if err != nil {
		return responseBody, err
	}

	q.runExecutePlanAsync(ctx, plan, queryTranslator, table, doneCh, optAsync)

	if optAsync == nil {
		bodyAsBytes, _ := body.Bytes()
		response := <-doneCh
		if response.err != nil {
			err = response.err
			if len(plan.Queries) > 0 {
				logger.ErrorWithCtx(ctx).Msgf("error making response: %v, queries[0]: %+v", err, plan.Queries[0])
			} else {
				logger.ErrorWithCtx(ctx).Msgf("error making response: %v, queries empty", err)
			}
		} else {
			responseBody, err = response.response.Marshal()
		}
		pushSecondaryInfo(q.debugInfoCollector, id, "", path, bodyAsBytes, response.translatedQueryBody, responseBody, plan.StartTime)
		sendMainPlanResult(responseBody, err)
		return responseBody, err
	} else {
		select {
		case <-time.After(time.Duration(optAsync.waitForResultsMs) * time.Millisecond):
			go func() { // Async search takes longer. Return partial results and wait for
				defer recovery.LogPanicWithCtx(ctx)
				res := <-doneCh
				responseBody, err = q.storeAsyncSearch(q.debugInfoCollector, id, optAsync.asyncId, optAsync.startTime, path, body, res, true, opaqueId)
				sendMainPlanResult(responseBody, err)
			}()
			return q.HandlePartialAsyncSearch(ctx, optAsync.asyncId)
		case res := <-doneCh:
			responseBody, err = q.storeAsyncSearch(q.debugInfoCollector, id, optAsync.asyncId, optAsync.startTime, path, body, res,
				optAsync.keepOnCompletion, opaqueId)
			sendMainPlanResult(responseBody, err)
			return responseBody, err
		}
	}
}

func (q *QueryRunner) handleSearchCommon(ctx context.Context, indexPattern string, body types.JSON, optAsync *AsyncQuery) ([]byte, error) {

	decision := q.tableResolver.Resolve(quesma_api.QueryPipeline, indexPattern)

	if decision.Err != nil {

		var resp []byte
		if optAsync != nil {
			resp, _ = queryparser.EmptyAsyncSearchResponse(optAsync.asyncId, false, 200)
		} else {
			resp = queryparser.EmptySearchResponse(ctx)
		}
		return resp, decision.Err
	}

	if decision.IsEmpty {
		if optAsync != nil {
			return queryparser.EmptyAsyncSearchResponse(optAsync.asyncId, false, 200)
		} else {
			return queryparser.EmptySearchResponse(ctx), nil
		}
	}

	if decision.IsClosed {
		return nil, quesma_errors.ErrIndexNotExists() // TODO
	}

	if len(decision.UseConnectors) == 0 {
		return nil, end_user_errors.ErrSearchCondition.New(fmt.Errorf("no connectors to use"))
	}

	var clickhouseConnector *quesma_api.ConnectorDecisionClickhouse

	for _, connector := range decision.UseConnectors {
		switch c := connector.(type) {

		case *quesma_api.ConnectorDecisionClickhouse:
			clickhouseConnector = c

		case *quesma_api.ConnectorDecisionElastic:
			// NOP

		default:
			return nil, fmt.Errorf("unknown connector type: %T", c)
		}
	}

	// it's impossible here to don't have a clickhouse decision
	if clickhouseConnector == nil {
		return nil, fmt.Errorf("no clickhouse connector")
	}

	var responseBody []byte

	startTime := time.Now()
	id := "FAKE_ID"
	if val := ctx.Value(tracing.RequestIdCtxKey); val != nil {
		id = val.(string)
	}
	path := ""
	if value := ctx.Value(tracing.RequestPath); value != nil {
		if str, ok := value.(string); ok {
			path = str
		}
	}

	tables, err := q.logManager.GetTableDefinitions()
	if err != nil {
		return nil, err
	}

	var table *clickhouse.Table // TODO we should use schema here only
	var currentSchema schema.Schema
	resolvedIndexes := clickhouseConnector.ClickhouseIndexes

	if !clickhouseConnector.IsCommonTable {
		if len(resolvedIndexes) < 1 {
			return []byte{}, end_user_errors.ErrNoSuchTable.New(fmt.Errorf("can't load [%s] schema", resolvedIndexes)).Details("Table: [%v]", resolvedIndexes)
		}
		indexName := resolvedIndexes[0] // we got exactly one table here because of the check above
		resolvedTableName := q.cfg.IndexConfig[indexName].TableName(indexName)

		resolvedSchema, ok := q.schemaRegistry.FindSchema(schema.IndexName(indexName))
		if !ok {
			return []byte{}, end_user_errors.ErrNoSuchTable.New(fmt.Errorf("can't load %s schema", resolvedTableName)).Details("Table: %s", resolvedTableName)
		}

		table, _ = tables.Load(resolvedTableName)
		if table == nil {
			return []byte{}, end_user_errors.ErrNoSuchTable.New(fmt.Errorf("can't load %s table", resolvedTableName)).Details("Table: %s", resolvedTableName)
		}

		currentSchema = resolvedSchema

	} else {

		// here we filter out indexes that are not stored in the common table
		var virtualOnlyTables []string
		for _, indexName := range resolvedIndexes {
			table, _ = tables.Load(q.cfg.IndexConfig[indexName].TableName(indexName))
			if table == nil {
				continue
			}
			if table.VirtualTable {
				virtualOnlyTables = append(virtualOnlyTables, indexName)
			}
		}
		resolvedIndexes = virtualOnlyTables

		if len(resolvedIndexes) == 0 {
			if optAsync != nil {
				return queryparser.EmptyAsyncSearchResponse(optAsync.asyncId, false, 200)
			} else {
				return queryparser.EmptySearchResponse(ctx), nil
			}
		}

		commonTable, ok := tables.Load(common_table.TableName)
		if !ok {
			return []byte{}, end_user_errors.ErrNoSuchTable.New(fmt.Errorf("can't load %s table", common_table.TableName)).Details("Table: %s", common_table.TableName)
		}

		// Let's build a  union of schemas
		resolvedSchema := schema.Schema{
			Fields:             make(map[schema.FieldName]schema.Field),
			Aliases:            make(map[schema.FieldName]schema.FieldName),
			ExistsInDataSource: false,
			DatabaseName:       "", // it doesn't matter here, common table will be used
		}

		schemas := q.schemaRegistry.AllSchemas()

		for _, idx := range resolvedIndexes {
			scm, ok := schemas[schema.IndexName(idx)]
			if !ok {
				return []byte{}, end_user_errors.ErrNoSuchTable.New(fmt.Errorf("can't load %s schema", idx)).Details("Table: %s", idx)
			}

			for fieldName := range scm.Fields {
				// here we construct our runtime  schema by merging fields from all resolved indexes
				resolvedSchema.Fields[fieldName] = scm.Fields[fieldName]
			}
		}

		currentSchema = resolvedSchema
		table = commonTable
	}

	queryTranslator := NewQueryTranslator(ctx, currentSchema, table, q.logManager, q.DateMathRenderer, resolvedIndexes)

	plan, err := queryTranslator.ParseQuery(body)

	if err != nil {
		logger.ErrorWithCtx(ctx).Msgf("parsing error: %v", err)
		queries := plan.Queries
		queriesBody := make([]diag.TranslatedSQLQuery, len(queries))
		queriesBodyConcat := ""
		for i, query := range queries {
			queriesBody[i].Query = []byte(query.SelectCommand.String())
			queriesBodyConcat += query.SelectCommand.String() + "\n"
		}
		responseBody = []byte(fmt.Sprintf("Invalid Queries: %v, err: %v", queriesBody, err))
		logger.ErrorWithCtxAndReason(ctx, "Quesma generated invalid SQL query").Msg(queriesBodyConcat)
		bodyAsBytes, _ := body.Bytes()
		pushSecondaryInfo(q.debugInfoCollector, id, "", path, bodyAsBytes, queriesBody, responseBody, startTime)
		return responseBody, errors.New(string(responseBody))
	}

	plan.IndexPattern = indexPattern
	plan.StartTime = startTime
	plan.Name = model.MainExecutionPlan

	if decision.EnableABTesting {
		return q.executeABTesting(ctx, plan, queryTranslator, table, body, optAsync, decision, indexPattern)
	}

	return q.executePlan(ctx, plan, queryTranslator, table, body, optAsync, nil, true)

}

func (q *QueryRunner) storeAsyncSearch(qmc diag.DebugInfoCollector, id, asyncId string,
	startTime time.Time, path string, body types.JSON, result asyncSearchWithError, keep bool, opaqueId string) (responseBody []byte, err error) {

	if result.err == nil {
		okStatus := 200
		asyncResponse := queryparser.SearchToAsyncSearchResponse(result.response, asyncId, false, &okStatus)
		responseBody, err = asyncResponse.Marshal()
	} else {
		responseBody, _ = queryparser.EmptyAsyncSearchResponse(asyncId, false, 503)
		err = result.err
	}

	if qmc != nil {
		took := time.Since(startTime)
		bodyAsBytes, _ := body.Bytes()
		qmc.PushSecondaryInfo(&diag.QueryDebugSecondarySource{
			Id:                     id,
			AsyncId:                asyncId,
			OpaqueId:               opaqueId,
			Path:                   path,
			IncomingQueryBody:      bodyAsBytes,
			QueryBodyTranslated:    result.translatedQueryBody,
			QueryTranslatedResults: responseBody,
			SecondaryTook:          took,
		})
	}

	if keep {
		compressedBody := responseBody
		isCompressed := false
		if err == nil {
			if compressed, compErr := util.Compress(responseBody); compErr == nil {
				compressedBody = compressed
				isCompressed = true
			}
		}
		q.AsyncRequestStorage.Store(asyncId, async_search_storage.NewAsyncRequestResult(compressedBody, err, time.Now(), isCompressed))
	}

	return
}

func (q *QueryRunner) asyncQueriesCumulatedBodySize() int {
	size := 0
	q.AsyncRequestStorage.Range(func(key string, value *async_search_storage.AsyncRequestResult) bool {
		size += len(value.GetResponseBody())
		return true
	})
	return size
}

func (q *QueryRunner) HandleAsyncSearchStatus(_ context.Context, id string) ([]byte, error) {
	if _, ok := q.AsyncRequestStorage.Load(id); ok { // there IS a result in storage, so query is completed/no longer running,
		return queryparser.EmptyAsyncSearchStatusResponse(id, false, false, 200)
	} else { // there is no result so query is might be(*) running
		return queryparser.EmptyAsyncSearchStatusResponse(id, true, true, 0) // 0 is a placeholder for missing completion status
	}
	// (*) - it is an oversimplification as we're responding with "still running" status even for queries that might not exist.
	// However since you're referring to async ID given from Quesma, we naively assume it *does* exist.
}

func (q *QueryRunner) HandlePartialAsyncSearch(ctx context.Context, id string) ([]byte, error) {
	if !strings.Contains(id, tracing.AsyncIdPrefix) {
		logger.ErrorWithCtx(ctx).Msgf("non quesma async id: %v", id)
		return queryparser.EmptyAsyncSearchResponse(id, false, 503)
	}
	if result, ok := q.AsyncRequestStorage.Load(id); ok {
		if err := result.GetErr(); err != nil {
			q.AsyncRequestStorage.Delete(id)
			logger.ErrorWithCtx(ctx).Msgf("error processing async query: %v", err)
			return queryparser.EmptyAsyncSearchResponse(id, false, 503)
		}
		q.AsyncRequestStorage.Delete(id)
		// We use zstd to conserve memory, as we have a lot of async queries
		if result.IsCompressed() {
			buf, err := util.Decompress(result.GetResponseBody())
			if err == nil {
				// Mark trace end is called only when the async query is fully processed
				// which means that isPartial is false
				logger.MarkTraceEndWithCtx(ctx).Msgf("Async query id : %s ended successfully", id)
				return buf, nil
			} else {
				return nil, err
			}
		}
		// Mark trace end is called only when the async query is fully processed
		// which means that isPartial is false
		logger.MarkTraceEndWithCtx(ctx).Msgf("Async query id : %s ended successfully", id)
		return result.GetResponseBody(), nil
	} else {
		const isPartial = true
		logger.InfoWithCtx(ctx).Msgf("async query id : %s partial result", id)
		return queryparser.EmptyAsyncSearchResponse(id, isPartial, 200)
	}
}

func (q *QueryRunner) DeleteAsyncSearch(id string) ([]byte, error) {
	if !strings.Contains(id, tracing.AsyncIdPrefix) {
		return nil, errors.New("invalid quesma async search id : " + id)
	}
	q.AsyncRequestStorage.Delete(id)
	return []byte(`{"acknowledged":true}`), nil
}

func (q *QueryRunner) reachedQueriesLimit(ctx context.Context, asyncId string, doneCh chan<- asyncSearchWithError) bool {
	if q.AsyncRequestStorage.Size() < asyncQueriesLimit && q.asyncQueriesCumulatedBodySize() < asyncQueriesLimitBytes {
		return false
	}
	err := errors.New("too many async queries")
	logger.ErrorWithCtx(ctx).Msgf("cannot handle %s, too many async queries", asyncId)
	doneCh <- asyncSearchWithError{err: err}
	return true
}

func (q *QueryRunner) addAsyncQueryContext(ctx context.Context, cancel context.CancelFunc, asyncRequestIdStr string) {
	q.AsyncQueriesContexts.Store(asyncRequestIdStr, async_search_storage.NewAsyncQueryContext(ctx, cancel, asyncRequestIdStr))
}

// This is a HACK
// This should be removed when we have a schema resolver working.
// It ignores queries against data_stream fields. These queries are kibana internal ones.
// Especially kibana searches indexes using 'namespace' field.
// This will be moved to the router.
// TODO remove this and move to the router  https://github.com/QuesmaOrg/quesma/pull/260#discussion_r1627290579
func (q *QueryRunner) isInternalKibanaQuery(query *model.Query) bool {
	for _, column := range query.SelectCommand.Columns {
		if strings.Contains(model.AsString(column), "data_stream.") {
			return true
		}
	}
	return false
}

type QueryJob func(ctx context.Context) ([]model.QueryResultRow, clickhouse.PerformanceResult, error)

func (q *QueryRunner) runQueryJobsSequence(ctx context.Context, jobs []QueryJob) ([][]model.QueryResultRow, []clickhouse.PerformanceResult, error) {
	var results = make([][]model.QueryResultRow, 0)
	var performance = make([]clickhouse.PerformanceResult, 0)
	for _, job := range jobs {
		rows, perf, err := job(ctx)
		performance = append(performance, perf)
		if err != nil {
			return nil, performance, err
		}

		results = append(results, rows)
	}
	return results, performance, nil
}

func (q *QueryRunner) runQueryJobsParallel(ctx context.Context, jobs []QueryJob) ([][]model.QueryResultRow, []clickhouse.PerformanceResult, error) {

	var results = make([][]model.QueryResultRow, len(jobs))
	var performances = make([]clickhouse.PerformanceResult, len(jobs))
	type result struct {
		rows  []model.QueryResultRow
		perf  clickhouse.PerformanceResult
		err   error
		jobId int
	}

	// this is our context to control the execution of the jobs

	// cancellation is done by the parent context
	// or by the first goroutine that returns an error
	ctx, cancel := context.WithCancel(ctx)
	// clean up on return
	defer cancel()

	collector := make(chan result, len(jobs))
	for n, job := range jobs {
		// produce
		go func(ctx context.Context, jobId int, j QueryJob) {
			defer recovery.LogAndHandlePanic(ctx, func(err error) {
				collector <- result{err: err, jobId: jobId}
			})
			start := time.Now()
			rows, perf, err := j(ctx)
			logger.DebugWithCtx(ctx).Msgf("parallel job %d finished in %v", jobId, time.Since(start))
			collector <- result{rows: rows, perf: perf, err: err, jobId: jobId}
		}(ctx, n, job)
	}

	// consume
	for range len(jobs) {
		res := <-collector
		performances[res.jobId] = res.perf
		if res.err == nil {
			results[res.jobId] = res.rows
		} else {
			return nil, performances, res.err
		}
	}

	return results, performances, nil
}

func (q *QueryRunner) runQueryJobs(ctx context.Context, jobs []QueryJob) ([][]model.QueryResultRow, []clickhouse.PerformanceResult, error) {

	numberOfJobs := len(jobs)

	// here we decide if we run queries in parallel or in sequence
	// if we have only one query, we run it in sequence

	// Decision should be based on query durations. Maybe we should run first nth
	// queries in parallel and in sequence and decide which one is faster.
	//
	// Parallel can be slower when we have a fast network connection.
	//
	if numberOfJobs == 1 || q.maxParallelQueries == 0 {
		return q.runQueryJobsSequence(ctx, jobs)
	}

	current := q.currentParallelQueryJobs.Add(int64(numberOfJobs))

	if current > maxParallelQueries {
		q.currentParallelQueryJobs.Add(int64(-numberOfJobs))
		return q.runQueryJobsSequence(ctx, jobs)
	}

	defer q.currentParallelQueryJobs.Add(int64(-numberOfJobs))

	return q.runQueryJobsParallel(ctx, jobs)

}

func (q *QueryRunner) makeJob(table *clickhouse.Table, query *model.Query) QueryJob {
	return func(ctx context.Context) ([]model.QueryResultRow, clickhouse.PerformanceResult, error) {
		var err error
		rows, performance, err := q.logManager.ProcessQuery(ctx, table, query)

		if err != nil {
			logger.ErrorWithCtx(ctx).Msg(err.Error())
			performance.Error = err
			return nil, performance, err
		}

		return rows, performance, nil
	}
}

func (q *QueryRunner) searchWorkerCommon(
	ctx context.Context,
	plan *model.ExecutionPlan,
	table *clickhouse.Table) (translatedQueryBody []diag.TranslatedSQLQuery, hits [][]model.QueryResultRow, err error) {

	queries := plan.Queries

	translatedQueryBody = make([]diag.TranslatedSQLQuery, len(queries))
	hits = make([][]model.QueryResultRow, len(queries))

	var jobs []QueryJob
	var jobHitsPosition []int // it keeps the position of the hits array for each job

	for i, query := range queries {
		sql := query.SelectCommand.String()

		if q.cfg.Logging.EnableSQLTracing {
			logger.InfoWithCtx(ctx).Msgf("SQL: %s", sql)
		}

		translatedQueryBody[i].Query = []byte(sql)
		if query.OptimizeHints != nil {
			translatedQueryBody[i].PerformedOptimizations = query.OptimizeHints.OptimizationsPerformed
		}
		translatedQueryBody[i].ExecutionPlanName = plan.Name
		translatedQueryBody[i].QueryTransformations = query.TransformationHistory.SchemaTransformers

		if q.isInternalKibanaQuery(query) {
			hits[i] = make([]model.QueryResultRow, 0)
			continue
		}

		job := q.makeJob(table, query)
		jobs = append(jobs, job)
		jobHitsPosition = append(jobHitsPosition, i)
	}

	jobResults, performance, err := q.runQueryJobs(ctx, jobs)
	if err != nil {
		for jobId, resultPosition := range jobHitsPosition {

			if jobId < len(performance) {
				p := performance[jobId]
				translatedQueryBody[resultPosition].QueryID = p.QueryID
				translatedQueryBody[resultPosition].Duration = p.Duration
				translatedQueryBody[resultPosition].ExplainPlan = p.ExplainPlan
				translatedQueryBody[resultPosition].RowsReturned = p.RowsReturned
				translatedQueryBody[resultPosition].Error = p.Error
			}
		}
		return
	}

	// fill the hits array with the results in the order of the database queries
	for jobId, resultPosition := range jobHitsPosition {

		hits[resultPosition] = jobResults[jobId]

		p := performance[jobId]
		translatedQueryBody[resultPosition].QueryID = p.QueryID
		translatedQueryBody[resultPosition].Duration = p.Duration
		translatedQueryBody[resultPosition].ExplainPlan = p.ExplainPlan
		translatedQueryBody[resultPosition].RowsReturned = p.RowsReturned
	}

	// apply the query rows transformers

	for i, t := range plan.QueryRowsTransformers {
		if t != nil {
			hits[i] = t.Transform(ctx, hits[i])
		}
	}

	return
}

func (q *QueryRunner) searchWorker(ctx context.Context,
	plan *model.ExecutionPlan,
	table *clickhouse.Table,
	doneCh chan<- asyncSearchWithError,
	optAsync *AsyncQuery) (translatedQueryBody []diag.TranslatedSQLQuery, resultRows [][]model.QueryResultRow, err error) {
	if optAsync != nil {
		if q.reachedQueriesLimit(ctx, optAsync.asyncId, doneCh) {
			return
		}
		// We need different ctx as our cancel is no longer tied to HTTP request, but to overall timeout.
		dbQueryCtx, dbCancel := context.WithCancel(tracing.NewContextWithRequest(ctx))
		q.addAsyncQueryContext(dbQueryCtx, dbCancel, optAsync.asyncId)
		ctx = dbQueryCtx
	}

	return q.searchWorkerCommon(ctx, plan, table)
}

func (q *QueryRunner) Close() {
	q.cancel()
	logger.Info().Msg("queryRunner Stopped")
}

func (q *QueryRunner) postProcessResults(plan *model.ExecutionPlan, results [][]model.QueryResultRow) ([][]model.QueryResultRow, error) {

	if len(plan.Queries) == 0 {
		return results, nil
	}

	// maybe model.Schema should be part of ExecutionPlan instead of Query
	indexSchema := plan.Queries[0].Schema

	type pipelineElement struct {
		name        string
		transformer model.ResultTransformer
	}

	var pipeline []pipelineElement

	pipeline = append(pipeline, pipelineElement{"replaceColumNamesWithFieldNames", &replaceColumNamesWithFieldNames{indexSchema: indexSchema}})

	// we can take the first one because all queries have the same runtime mappings
	if len(plan.Queries[0].RuntimeMappings) > 0 {

		// this transformer must be called after replaceColumNamesWithFieldNames
		// painless scripts rely on field names not column names

		fieldScripts := make(map[string]painful.Expr)

		for field, runtimeMapping := range plan.Queries[0].RuntimeMappings {
			if runtimeMapping.PostProcessExpression != nil {
				fieldScripts[field] = runtimeMapping.PostProcessExpression
			}
		}

		if len(fieldScripts) > 0 {
			pipeline = append(pipeline, pipelineElement{"applyPainlessScripts", &EvalPainlessScriptOnColumnsTransformer{FieldScripts: fieldScripts}})
		}

	}

	var err error
	for _, t := range pipeline {

		// TODO we should check if the transformer is applicable here
		// for example if the schema doesn't hava array fields, we should skip the arrayResultTransformer
		// these transformers can be cpu and mem consuming

		results, err = t.transformer.Transform(results)
		if err != nil {
			return nil, fmt.Errorf("resuls transformer %s has failed: %w", t.name, err)
		}
	}

	return results, nil
}

func pushPrimaryInfo(qmc diag.DebugInfoCollector, Id string, QueryResp []byte, startTime time.Time) {
	qmc.PushPrimaryInfo(&diag.QueryDebugPrimarySource{
		Id:          Id,
		QueryResp:   QueryResp,
		PrimaryTook: time.Since(startTime),
	})
}

func pushSecondaryInfo(qmc diag.DebugInfoCollector, Id, AsyncId, Path string, IncomingQueryBody []byte, QueryBodyTranslated []diag.TranslatedSQLQuery, QueryTranslatedResults []byte, startTime time.Time) {
	qmc.PushSecondaryInfo(&diag.QueryDebugSecondarySource{
		Id:                     Id,
		AsyncId:                AsyncId,
		Path:                   Path,
		IncomingQueryBody:      IncomingQueryBody,
		QueryBodyTranslated:    QueryBodyTranslated,
		QueryTranslatedResults: QueryTranslatedResults,
		SecondaryTook:          time.Since(startTime)})
}
