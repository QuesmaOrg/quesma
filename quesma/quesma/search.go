// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package quesma

import (
	"context"
	"errors"
	"fmt"
	"quesma/ab_testing"
	"quesma/clickhouse"
	"quesma/common_table"
	"quesma/concurrent"
	"quesma/elasticsearch"
	"quesma/end_user_errors"
	"quesma/index_registry"
	"quesma/logger"
	"quesma/model"
	"quesma/optimize"
	"quesma/queryparser"
	"quesma/queryparser/query_util"
	"quesma/quesma/config"
	"quesma/quesma/errors"
	"quesma/quesma/recovery"
	"quesma/quesma/types"
	"quesma/quesma/ui"
	"quesma/schema"
	"quesma/tracing"
	"quesma/util"
	"slices"
	"strings"
	"sync/atomic"
	"time"
)

const (
	asyncQueriesLimit      = 10000
	asyncQueriesLimitBytes = 1024 * 1024 * 500 // 500MB
)

type AsyncRequestResult struct {
	responseBody []byte
	added        time.Time
	isCompressed bool
	err          error
}

type AsyncQueryContext struct {
	id     string
	ctx    context.Context
	cancel context.CancelFunc
	added  time.Time
}

type QueryRunner struct {
	executionCtx            context.Context
	cancel                  context.CancelFunc
	AsyncRequestStorage     *concurrent.Map[string, AsyncRequestResult]
	AsyncQueriesContexts    *concurrent.Map[string, *AsyncQueryContext]
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
	indexRegistry            index_registry.IndexRegistry
}

func (q *QueryRunner) EnableQueryOptimization(cfg *config.QuesmaConfiguration) {
	q.transformationPipeline.transformers = append(q.transformationPipeline.transformers, optimize.NewOptimizePipeline(cfg))
}

func NewQueryRunner(lm *clickhouse.LogManager, cfg *config.QuesmaConfiguration, im elasticsearch.IndexManagement, qmc *ui.QuesmaManagementConsole, schemaRegistry schema.Registry, abResultsRepository ab_testing.Sender, indexRegistry index_registry.IndexRegistry) *QueryRunner {
	ctx, cancel := context.WithCancel(context.Background())

	return &QueryRunner{logManager: lm, cfg: cfg, im: im, quesmaManagementConsole: qmc,
		executionCtx: ctx, cancel: cancel, AsyncRequestStorage: concurrent.NewMap[string, AsyncRequestResult](),
		AsyncQueriesContexts: concurrent.NewMap[string, *AsyncQueryContext](),
		transformationPipeline: TransformationPipeline{
			transformers: []model.QueryTransformer{
				&SchemaCheckPass{cfg: cfg.IndexConfig},
			},
		},
		schemaRegistry:  schemaRegistry,
		ABResultsSender: abResultsRepository,
		indexRegistry:   indexRegistry,
	}
}

func NewAsyncQueryContext(ctx context.Context, cancel context.CancelFunc, id string) *AsyncQueryContext {
	return &AsyncQueryContext{ctx: ctx, cancel: cancel, added: time.Now(), id: id}
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

type AsyncSearchWithError struct {
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

func (q *QueryRunner) runExecutePlanAsync(ctx context.Context, plan *model.ExecutionPlan, queryTranslator IQueryTranslator, table *clickhouse.Table, doneCh chan AsyncSearchWithError, optAsync *AsyncQuery) {
	go func() {
		defer recovery.LogAndHandlePanic(ctx, func(err error) {
			doneCh <- AsyncSearchWithError{err: err}
		})

		translatedQueryBody, results, err := q.searchWorker(ctx, plan, table, doneCh, optAsync)
		if err != nil {
			doneCh <- AsyncSearchWithError{translatedQueryBody: translatedQueryBody, err: err}
			return
		}

		if len(results) == 0 {
			logger.ErrorWithCtx(ctx).Msgf("no hits, sqls: %v", translatedQueryBody)
			doneCh <- AsyncSearchWithError{translatedQueryBody: translatedQueryBody, err: errors.New("no hits")}
			return
		}

		results, err = q.postProcessResults(plan, results)
		if err != nil {
			doneCh <- AsyncSearchWithError{translatedQueryBody: translatedQueryBody, err: err}
		}

		searchResponse := queryTranslator.MakeSearchResponse(plan.Queries, results)

		doneCh <- AsyncSearchWithError{response: searchResponse, translatedQueryBody: translatedQueryBody, err: err}
	}()
}

func (q *QueryRunner) executePlan(ctx context.Context, plan *model.ExecutionPlan, queryTranslator IQueryTranslator, table *clickhouse.Table, body types.JSON, optAsync *AsyncQuery, optComparePlansCh chan<- executionPlanResult) (responseBody []byte, err error) {
	contextValues := tracing.ExtractValues(ctx)
	id := contextValues.RequestId
	path := contextValues.RequestPath
	opaqueId := contextValues.OpaqueId

	doneCh := make(chan AsyncSearchWithError, 1)

	sendMainPlanResult := func(responseBody []byte, err error) {
		if optComparePlansCh != nil {
			optComparePlansCh <- executionPlanResult{
				isMain:       true,
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

	decision := q.indexRegistry.ResolveIngest(indexPattern)
	index_registry.TODO("handleSearchCommon", indexPattern, " -> ", decision)

	sources, sourcesElastic, sourcesClickhouse := ResolveSources(indexPattern, q.cfg, q.im, q.schemaRegistry)

	switch sources {
	case sourceBoth:

		err := end_user_errors.ErrSearchCondition.New(fmt.Errorf("index pattern [%s] resolved to both elasticsearch indices: [%s] and clickhouse tables: [%s]", indexPattern, sourcesElastic, sourcesClickhouse))

		var resp []byte
		if optAsync != nil {
			resp, _ = queryparser.EmptyAsyncSearchResponse(optAsync.asyncId, false, 200)
		} else {
			resp = queryparser.EmptySearchResponse(ctx)
		}
		return resp, err
	case sourceNone:
		if elasticsearch.IsIndexPattern(indexPattern) {
			if optAsync != nil {
				return queryparser.EmptyAsyncSearchResponse(optAsync.asyncId, false, 200)
			} else {
				return queryparser.EmptySearchResponse(ctx), nil
			}
		} else {
			logger.WarnWithCtx(ctx).Msgf("could not resolve any table name for [%s]", indexPattern)
			return nil, quesma_errors.ErrIndexNotExists()
		}
	case sourceClickhouse:
		logger.Debug().Msgf("index pattern [%s] resolved to clickhouse tables: [%s]", indexPattern, sourcesClickhouse)
		if elasticsearch.IsIndexPattern(indexPattern) {
			sourcesClickhouse = q.removeNotExistingTables(sourcesClickhouse)
		}
	case sourceElasticsearch:
		return nil, end_user_errors.ErrSearchCondition.New(fmt.Errorf("index pattern [%s] resolved to elasticsearch indices: [%s]", indexPattern, sourcesElastic))
	}
	logger.Debug().Msgf("resolved sources for index pattern %s -> %s", indexPattern, sources)

	if len(sourcesClickhouse) == 0 {
		if elasticsearch.IsIndexPattern(indexPattern) {
			if optAsync != nil {
				return queryparser.EmptyAsyncSearchResponse(optAsync.asyncId, false, 200)
			} else {
				return queryparser.EmptySearchResponse(ctx), nil
			}
		} else {
			logger.WarnWithCtx(ctx).Msgf("could not resolve any table name for [%s]", indexPattern)
			return nil, quesma_errors.ErrIndexNotExists()
		}
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
	resolvedIndexes := sourcesClickhouse

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

	// Some flags may trigger alternative execution plans, this is primary for dev
	alternativePlan, alternativePlanExecutor := q.maybeCreateAlternativeExecutionPlan(ctx, resolvedIndexes, plan, queryTranslator, body, table, optAsync != nil)

	var optComparePlansCh chan<- executionPlanResult

	if alternativePlan != nil {
		optComparePlansCh = q.runAlternativePlanAndComparison(ctx, alternativePlan, alternativePlanExecutor, body)
	}

	return q.executePlan(ctx, plan, queryTranslator, table, body, optAsync, optComparePlansCh)
}

func (q *QueryRunner) removeNotExistingTables(sourcesClickhouse []string) []string {
	allKnownTables, _ := q.logManager.GetTableDefinitions()
	return slices.DeleteFunc(sourcesClickhouse, func(s string) bool {
		if len(q.cfg.IndexConfig[s].Override) > 0 {
			s = q.cfg.IndexConfig[s].Override
		}

		_, exists := allKnownTables.Load(s)
		return !exists
	})
}

func (q *QueryRunner) storeAsyncSearch(qmc *ui.QuesmaManagementConsole, id, asyncId string,
	startTime time.Time, path string, body types.JSON, result AsyncSearchWithError, keep bool, opaqueId string) (responseBody []byte, err error) {
	took := time.Since(startTime)
	if result.err != nil {
		if keep {
			q.AsyncRequestStorage.Store(asyncId, AsyncRequestResult{err: result.err, added: time.Now(),
				isCompressed: false})
		}
		responseBody, _ = queryparser.EmptyAsyncSearchResponse(asyncId, false, 503)
		err = result.err
		bodyAsBytes, _ := body.Bytes()
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
		return
	}
	asyncResponse := queryparser.SearchToAsyncSearchResponse(result.response, asyncId, false, 200)
	responseBody, err = asyncResponse.Marshal()
	bodyAsBytes, _ := body.Bytes()
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
		q.AsyncRequestStorage.Store(asyncId,
			AsyncRequestResult{responseBody: compressedBody, added: time.Now(), err: err, isCompressed: isCompressed})
	}
	return
}

func (q *QueryRunner) asyncQueriesCumulatedBodySize() int {
	size := 0
	q.AsyncRequestStorage.Range(func(key string, value AsyncRequestResult) bool {
		size += len(value.responseBody)
		return true
	})
	return size
}

func (q *QueryRunner) handlePartialAsyncSearch(ctx context.Context, id string) ([]byte, error) {
	if !strings.Contains(id, tracing.AsyncIdPrefix) {
		logger.ErrorWithCtx(ctx).Msgf("non quesma async id: %v", id)
		return queryparser.EmptyAsyncSearchResponse(id, false, 503)
	}
	if result, ok := q.AsyncRequestStorage.Load(id); ok {
		if result.err != nil {
			q.AsyncRequestStorage.Delete(id)
			logger.ErrorWithCtx(ctx).Msgf("error processing async query: %v", result.err)
			return queryparser.EmptyAsyncSearchResponse(id, false, 503)
		}
		q.AsyncRequestStorage.Delete(id)
		// We use zstd to conserve memory, as we have a lot of async queries
		if result.isCompressed {
			buf, err := util.Decompress(result.responseBody)
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
		return result.responseBody, nil
	} else {
		const isPartial = true
		logger.InfoWithCtx(ctx).Msgf("async query id : %s partial result", id)
		return queryparser.EmptyAsyncSearchResponse(id, isPartial, 200)
	}
}

func (q *QueryRunner) deleteAsyncSeach(id string) ([]byte, error) {
	if !strings.Contains(id, tracing.AsyncIdPrefix) {
		return nil, errors.New("invalid quesma async search id : " + id)
	}
	q.AsyncRequestStorage.Delete(id)
	return []byte{}, nil
}

func (q *QueryRunner) reachedQueriesLimit(ctx context.Context, asyncId string, doneCh chan<- AsyncSearchWithError) bool {
	if q.AsyncRequestStorage.Size() < asyncQueriesLimit && q.asyncQueriesCumulatedBodySize() < asyncQueriesLimitBytes {
		return false
	}
	err := errors.New("too many async queries")
	logger.ErrorWithCtx(ctx).Msgf("cannot handle %s, too many async queries", asyncId)
	doneCh <- AsyncSearchWithError{err: err}
	return true
}

func (q *QueryRunner) addAsyncQueryContext(ctx context.Context, cancel context.CancelFunc, asyncRequestIdStr string) {
	q.AsyncQueriesContexts.Store(asyncRequestIdStr, NewAsyncQueryContext(ctx, cancel, asyncRequestIdStr))
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
	const maxParallelQueries = 25 // this is arbitrary value

	numberOfJobs := len(jobs)

	// here we decide if we run queries in parallel or in sequence
	// if we have only one query, we run it in sequence

	// Decision should be based on query durations. Maybe we should run first nth
	// queries in parallel and in sequence and decide which one is faster.
	//
	// Parallel can be slower when we have a fast network connection.
	//
	if numberOfJobs == 1 {
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

	for i, query := range queries {
		sql := query.SelectCommand.String()
		logger.InfoWithCtx(ctx).Msgf("SQL: %s", sql)
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
	doneCh chan<- AsyncSearchWithError,
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
		return nil, fmt.Errorf("postProcessResults: plan.Queries is empty")
	}

	// maybe model.Schema should be part of ExecutionPlan instead of Query
	indexSchema := plan.Queries[0].Schema

	pipeline := []struct {
		name        string
		transformer model.ResultTransformer
	}{
		{"replaceColumNamesWithFieldNames", &replaceColumNamesWithFieldNames{indexSchema: indexSchema}},
		{"arrayResultTransformer", &ArrayResultTransformer{}},
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
