// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package quesma

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"quesma/ab_testing"
	"quesma/clickhouse"
	"quesma/common_table"
	"quesma/elasticsearch"
	"quesma/end_user_errors"
	"quesma/logger"
	"quesma/model"
	"quesma/optimize"
	"quesma/painful"
	"quesma/queryparser"
	"quesma/queryparser/query_util"
	"quesma/quesma/async_search_storage"
	"quesma/quesma/config"
	"quesma/quesma/errors"
	"quesma/quesma/recovery"
	"quesma/quesma/types"
	"quesma/quesma/ui"
	"quesma/schema"
	"quesma/table_resolver"
	"quesma/telemetry"
	"quesma/util"
	"quesma_v2/core"
	tracing "quesma_v2/core/tracing"
	"slices"
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
	executionCtx            context.Context
	cancel                  context.CancelFunc
	AsyncRequestStorage     async_search_storage.AsyncRequestResultStorage
	AsyncQueriesContexts    async_search_storage.AsyncQueryContextStorage
	logManager              *clickhouse.LogManager
	cfg                     *config.QuesmaConfiguration
	im                      elasticsearch.IndexManagement
	quesmaManagementConsole *ui.QuesmaManagementConsole

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

func (q *QueryRunner) EnableQueryOptimization(cfg *config.QuesmaConfiguration) {
	q.transformationPipeline.transformers = append(q.transformationPipeline.transformers, optimize.NewOptimizePipeline(cfg))
}

func NewQueryRunner(lm *clickhouse.LogManager,
	cfg *config.QuesmaConfiguration,
	im elasticsearch.IndexManagement,
	qmc *ui.QuesmaManagementConsole,
	schemaRegistry schema.Registry,
	abResultsRepository ab_testing.Sender,
	resolver table_resolver.TableResolver) *QueryRunner {

	ctx, cancel := context.WithCancel(context.Background())

	return &QueryRunner{logManager: lm, cfg: cfg, im: im, quesmaManagementConsole: qmc,
		executionCtx: ctx, cancel: cancel,
		AsyncRequestStorage:  async_search_storage.NewAsyncSearchStorageInMemory(),
		AsyncQueriesContexts: async_search_storage.NewAsyncQueryContextStorageInMemory(),
		transformationPipeline: TransformationPipeline{
			transformers: []model.QueryTransformer{
				&SchemaCheckPass{cfg: cfg},
			},
		},
		schemaRegistry:  schemaRegistry,
		ABResultsSender: abResultsRepository,
		tableResolver:   resolver,

		maxParallelQueries: maxParallelQueries,
	}
}

func NewQueryRunnerDefaultForTests(db *sql.DB, cfg *config.QuesmaConfiguration,
	tableName string, tables *clickhouse.TableMap, staticRegistry *schema.StaticRegistry) *QueryRunner {

	lm := clickhouse.NewLogManagerWithConnection(db, tables)
	logChan := logger.InitOnlyChannelLoggerForTests()

	resolver := table_resolver.NewEmptyTableResolver()
	resolver.Decisions[tableName] = &quesma_api.Decision{
		UseConnectors: []quesma_api.ConnectorDecision{
			&quesma_api.ConnectorDecisionClickhouse{
				ClickhouseTableName: tableName,
				ClickhouseTables:    []string{tableName},
			},
		},
	}

	managementConsole := ui.NewQuesmaManagementConsole(cfg, nil, nil, logChan, telemetry.NewPhoneHomeEmptyAgent(), nil, resolver)
	go managementConsole.RunOnlyChannelProcessor()

	return NewQueryRunner(lm, cfg, nil, managementConsole, staticRegistry, ab_testing.NewEmptySender(), resolver)
}

// returns -1 when table name could not be resolved
func (q *QueryRunner) handleCount(ctx context.Context, indexPattern string) (int64, error) {
	indexes, err := q.logManager.ResolveIndexPattern(ctx, indexPattern)
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

func (q *QueryRunner) handleSearch(ctx context.Context, indexPattern string, body types.JSON) ([]byte, error) {
	return q.handleSearchCommon(ctx, indexPattern, body, nil, QueryLanguageDefault)
}

func (q *QueryRunner) handleEQLSearch(ctx context.Context, indexPattern string, body types.JSON) ([]byte, error) {
	return q.handleSearchCommon(ctx, indexPattern, body, nil, QueryLanguageEQL)
}

func (q *QueryRunner) handleAsyncSearch(ctx context.Context, indexPattern string, body types.JSON,
	waitForResultsMs int, keepOnCompletion bool) ([]byte, error) {
	async := AsyncQuery{
		asyncId:          tracing.GetAsyncId(),
		waitForResultsMs: waitForResultsMs,
		keepOnCompletion: keepOnCompletion,
		startTime:        time.Now(),
	}
	ctx = context.WithValue(ctx, tracing.AsyncIdCtxKey, async.asyncId)
	logger.InfoWithCtx(ctx).Msgf("async search request id: %s started", async.asyncId)
	return q.handleSearchCommon(ctx, indexPattern, body, &async, QueryLanguageDefault)
}

type asyncSearchWithError struct {
	response            *model.SearchResp
	translatedQueryBody []types.TranslatedSQLQuery
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
	planQueries2 := slices.Clone(plan.Queries)
	for i, planQuery := range planQueries2 {
		if planQuery.AlternativeSelectCommand != nil {
			newPlanQuery := *planQuery
			newPlanQuery.SelectCommand = *newPlanQuery.AlternativeSelectCommand
			planQueries2[i] = &newPlanQuery
		}
	}
	planQueries2, err = q.transformationPipeline.Transform(planQueries2)
	if err != nil {
		return fmt.Errorf("error transforming queries: %v", err)
	}
	for i, planQuery := range planQueries2 {
		if planQuery.AlternativeSelectCommand != nil {
			plan.Queries[i].AlternativeSelectCommand = &planQuery.SelectCommand
		}
	}
	return nil
}

// Deprecated - this method should be examined and potentially removed
func (q *QueryRunner) checkProperties(ctx context.Context, plan *model.ExecutionPlan, table *clickhouse.Table, queryTranslator IQueryTranslator) ([]byte, error) {
	queries := plan.Queries
	if len(queries) > 0 && query_util.IsNonAggregationQuery(queries[0]) {
		if properties := q.findNonexistingProperties(queries[0], table, queryTranslator); len(properties) > 0 {
			logger.DebugWithCtx(ctx).Msgf("properties %s not found in table %s", properties, table.Name)
			if elasticsearch.IsIndexPattern(plan.IndexPattern) {
				return queryparser.EmptySearchResponse(ctx), nil
			} else {
				return nil, fmt.Errorf("properties %s not found in table %s", properties, table.Name)
			}
		}
	}
	return nil, nil
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

	if resp, err := q.checkProperties(ctx, plan, table, queryTranslator); err != nil {
		return resp, err
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
		pushSecondaryInfo(q.quesmaManagementConsole, id, "", path, bodyAsBytes, response.translatedQueryBody, responseBody, plan.StartTime)
		sendMainPlanResult(responseBody, err)
		return responseBody, err
	} else {
		select {
		case <-time.After(time.Duration(optAsync.waitForResultsMs) * time.Millisecond):
			go func() { // Async search takes longer. Return partial results and wait for
				recovery.LogPanicWithCtx(ctx)
				res := <-doneCh
				responseBody, err = q.storeAsyncSearch(q.quesmaManagementConsole, id, optAsync.asyncId, optAsync.startTime, path, body, res, true, opaqueId)
				sendMainPlanResult(responseBody, err)
			}()
			return q.handlePartialAsyncSearch(ctx, optAsync.asyncId)
		case res := <-doneCh:
			responseBody, err = q.storeAsyncSearch(q.quesmaManagementConsole, id, optAsync.asyncId, optAsync.startTime, path, body, res,
				optAsync.keepOnCompletion, opaqueId)
			sendMainPlanResult(responseBody, err)
			return responseBody, err
		}
	}
}

func (q *QueryRunner) handleSearchCommon(ctx context.Context, indexPattern string, body types.JSON, optAsync *AsyncQuery, queryLanguage QueryLanguage) ([]byte, error) {

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
	id := ctx.Value(tracing.RequestIdCtxKey).(string)
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
	resolvedIndexes := clickhouseConnector.ClickhouseTables

	if len(resolvedIndexes) == 1 {
		indexName := resolvedIndexes[0] // we got exactly one table here because of the check above
		resolvedTableName := indexName

		if len(q.cfg.IndexConfig[indexName].Override) > 0 {
			resolvedTableName = q.cfg.IndexConfig[indexName].Override
		}

		resolvedSchema, ok := q.schemaRegistry.FindSchema(schema.TableName(indexName))
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
			tableName := indexName
			if len(q.cfg.IndexConfig[indexName].Override) > 0 {
				tableName = q.cfg.IndexConfig[indexName].Override
			}

			table, _ = tables.Load(tableName)
			if table == nil {
				return []byte{}, end_user_errors.ErrNoSuchTable.New(fmt.Errorf("can't load %s table", indexName)).Details("Table: %s", indexName)
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

		for _, idx := range resolvedIndexes {
			scm, ok := q.schemaRegistry.FindSchema(schema.TableName(idx))
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

	queryTranslator := NewQueryTranslator(ctx, queryLanguage, currentSchema, table, q.logManager, q.DateMathRenderer, resolvedIndexes, q.cfg)

	plan, err := queryTranslator.ParseQuery(body)

	if err != nil {
		logger.ErrorWithCtx(ctx).Msgf("parsing error: %v", err)
		queries := plan.Queries
		queriesBody := make([]types.TranslatedSQLQuery, len(queries))
		queriesBodyConcat := ""
		for i, query := range queries {
			queriesBody[i].Query = []byte(query.SelectCommand.String())
			queriesBodyConcat += query.SelectCommand.String() + "\n"
		}
		responseBody = []byte(fmt.Sprintf("Invalid Queries: %v, err: %v", queriesBody, err))
		logger.ErrorWithCtxAndReason(ctx, "Quesma generated invalid SQL query").Msg(queriesBodyConcat)
		bodyAsBytes, _ := body.Bytes()
		pushSecondaryInfo(q.quesmaManagementConsole, id, "", path, bodyAsBytes, queriesBody, responseBody, startTime)
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

func (q *QueryRunner) storeAsyncSearch(qmc *ui.QuesmaManagementConsole, id, asyncId string,
	startTime time.Time, path string, body types.JSON, result asyncSearchWithError, keep bool, opaqueId string) (responseBody []byte, err error) {

	took := time.Since(startTime)
	bodyAsBytes, _ := body.Bytes()
	if result.err == nil {
		okStatus := 200
		asyncResponse := queryparser.SearchToAsyncSearchResponse(result.response, asyncId, false, &okStatus)
		responseBody, err = asyncResponse.Marshal()
	} else {
		responseBody, _ = queryparser.EmptyAsyncSearchResponse(asyncId, false, 503)
		err = result.err
	}

	qmc.PushSecondaryInfo(&ui.QueryDebugSecondarySource{
		Id:                     id,
		AsyncId:                asyncId,
		OpaqueId:               opaqueId,
		Path:                   path,
		IncomingQueryBody:      bodyAsBytes,
		QueryBodyTranslated:    result.translatedQueryBody,
		QueryTranslatedResults: responseBody,
		SecondaryTook:          took,
	})

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

func (q *QueryRunner) handleAsyncSearchStatus(_ context.Context, id string) ([]byte, error) {
	if _, ok := q.AsyncRequestStorage.Load(id); ok { // there IS a result in storage, so query is completed/no longer running,
		return queryparser.EmptyAsyncSearchStatusResponse(id, false, false, 200)
	} else { // there is no result so query is might be(*) running
		return queryparser.EmptyAsyncSearchStatusResponse(id, true, true, 0) // 0 is a placeholder for missing completion status
	}
	// (*) - it is an oversimplification as we're responding with "still running" status even for queries that might not exist.
	// However since you're referring to async ID given from Quesma, we naively assume it *does* exist.
}

func (q *QueryRunner) handlePartialAsyncSearch(ctx context.Context, id string) ([]byte, error) {
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

func (q *QueryRunner) deleteAsyncSearch(id string) ([]byte, error) {
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
	table *clickhouse.Table) (translatedQueryBody []types.TranslatedSQLQuery, hits [][]model.QueryResultRow, err error) {

	queries := plan.Queries

	translatedQueryBody = make([]types.TranslatedSQLQuery, len(queries))
	hits = make([][]model.QueryResultRow, len(queries))

	var jobs []QueryJob
	var jobHitsPosition []int // it keeps the position of the hits array for each job

	for i, queryOrig := range queries {
		queryVal := *queryOrig
		query := &queryVal
		sql := query.SelectCommand.String()

		if query.AlternativeSelectCommand != nil {
			sql = query.AlternativeSelectCommand.String()
			query.SelectCommand = *query.AlternativeSelectCommand
		}

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

	var jobs2 []QueryJob
	var jobHitsPosition2 []int // it keeps the position of the hits array for each job

	// fill the hits array with the results in the order of the database queries
	for jobId, resultPosition := range jobHitsPosition {

		hits[resultPosition] = jobResults[jobId]

		p := performance[jobId]
		translatedQueryBody[resultPosition].QueryID = p.QueryID
		translatedQueryBody[resultPosition].Duration = p.Duration
		translatedQueryBody[resultPosition].ExplainPlan = p.ExplainPlan
		translatedQueryBody[resultPosition].RowsReturned = p.RowsReturned

		if queries[resultPosition].AlternativeSelectCommand != nil && len(hits[resultPosition]) == queries[resultPosition].AlternativeSelectCommand.Limit {
			logger.Info().Msgf("Running on reduced time range succeeded - got %d results on a %d limit.", len(hits[resultPosition]), queries[resultPosition].AlternativeSelectCommand.Limit)
		}

		if queries[resultPosition].AlternativeSelectCommand != nil && len(hits[resultPosition]) < queries[resultPosition].AlternativeSelectCommand.Limit {
			logger.Info().Msgf("Received partial result, got %d results but the limit is %d. Running the query on full time range.", len(hits[resultPosition]), queries[resultPosition].AlternativeSelectCommand.Limit)
			query := queries[resultPosition]
			sql := query.SelectCommand.String()

			if q.cfg.Logging.EnableSQLTracing {
				logger.InfoWithCtx(ctx).Msgf("SQL: %s", sql)
			}

			translatedQueryBody[resultPosition].Query = []byte(sql)
			if query.OptimizeHints != nil {
				translatedQueryBody[resultPosition].PerformedOptimizations = query.OptimizeHints.OptimizationsPerformed
			}
			translatedQueryBody[resultPosition].ExecutionPlanName = plan.Name
			translatedQueryBody[resultPosition].QueryTransformations = query.TransformationHistory.SchemaTransformers

			if q.isInternalKibanaQuery(query) {
				hits[resultPosition] = make([]model.QueryResultRow, 0)
				continue
			}

			job := q.makeJob(table, query)
			jobs2 = append(jobs2, job)
			jobHitsPosition2 = append(jobHitsPosition2, resultPosition)
		}
	}

	if len(jobs2) != 0 {
		jobResults2, performance2, err2 := q.runQueryJobs(ctx, jobs2)
		if err2 != nil {
			return
		}

		// fill the hits array with the results in the order of the database queries
		for jobId, resultPosition := range jobHitsPosition2 {

			hits[resultPosition] = jobResults2[jobId]

			p := performance2[jobId]
			translatedQueryBody[resultPosition].QueryID = p.QueryID
			translatedQueryBody[resultPosition].Duration = p.Duration
			translatedQueryBody[resultPosition].ExplainPlan = p.ExplainPlan
			translatedQueryBody[resultPosition].RowsReturned = p.RowsReturned
		}
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
	optAsync *AsyncQuery) (translatedQueryBody []types.TranslatedSQLQuery, resultRows [][]model.QueryResultRow, err error) {
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

func (q *QueryRunner) findNonexistingProperties(query *model.Query, table *clickhouse.Table, queryTranslator IQueryTranslator) []string {
	// this is not fully correct, but we keep it backward compatible
	var results = make([]string, 0)
	var allReferencedFields = make([]string, 0)
	for _, col := range query.SelectCommand.Columns {
		for _, c := range model.GetUsedColumns(col) {
			allReferencedFields = append(allReferencedFields, c.ColumnName)
		}
	}
	allReferencedFields = append(allReferencedFields, query.SelectCommand.OrderByFieldNames()...)

	// TODO This should be done using query.Schema instead of table
	for _, property := range allReferencedFields {
		queryTranslatorValue, ok := queryTranslator.(*queryparser.ClickhouseQueryTranslator)
		if ok {
			property = queryTranslatorValue.ResolveField(q.executionCtx, property)
		}
		if property != "*" && !table.HasColumn(q.executionCtx, property) {
			results = append(results, property)
		}
	}
	return results
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

func pushPrimaryInfo(qmc *ui.QuesmaManagementConsole, Id string, QueryResp []byte, startTime time.Time) {
	qmc.PushPrimaryInfo(&ui.QueryDebugPrimarySource{
		Id:          Id,
		QueryResp:   QueryResp,
		PrimaryTook: time.Since(startTime),
	})
}

func pushSecondaryInfo(qmc *ui.QuesmaManagementConsole, Id, AsyncId, Path string, IncomingQueryBody []byte, QueryBodyTranslated []types.TranslatedSQLQuery, QueryTranslatedResults []byte, startTime time.Time) {
	qmc.PushSecondaryInfo(&ui.QueryDebugSecondarySource{
		Id:                     Id,
		AsyncId:                AsyncId,
		Path:                   Path,
		IncomingQueryBody:      IncomingQueryBody,
		QueryBodyTranslated:    QueryBodyTranslated,
		QueryTranslatedResults: QueryTranslatedResults,
		SecondaryTook:          time.Since(startTime)})
}
