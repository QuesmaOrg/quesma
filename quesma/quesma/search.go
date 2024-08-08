// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package quesma

import (
	"context"
	"errors"
	"fmt"
	"quesma/ab_testing"
	"quesma/clickhouse"
	"quesma/concurrent"
	"quesma/elasticsearch"
	"quesma/end_user_errors"
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
	cfg                     config.QuesmaConfiguration
	im                      elasticsearch.IndexManagement
	quesmaManagementConsole *ui.QuesmaManagementConsole

	// configuration

	// this is passed to the QueryTranslator to render date math expressions
	DateMathRenderer         string // "clickhouse_interval" or "literal"  if not set, we use "clickhouse_interval"
	currentParallelQueryJobs atomic.Int64
	transformationPipeline   TransformationPipeline
	schemaRegistry           schema.Registry
	ABResultsSender          ab_testing.Sender
}

func (q *QueryRunner) EnableQueryOptimization(cfg config.QuesmaConfiguration) {
	q.transformationPipeline.transformers = append(q.transformationPipeline.transformers, optimize.NewOptimizePipeline(cfg))
}

func NewQueryRunner(lm *clickhouse.LogManager, cfg config.QuesmaConfiguration, im elasticsearch.IndexManagement, qmc *ui.QuesmaManagementConsole, schemaRegistry schema.Registry, abResultsRepository ab_testing.Sender) *QueryRunner {
	ctx, cancel := context.WithCancel(context.Background())

	return &QueryRunner{logManager: lm, cfg: cfg, im: im, quesmaManagementConsole: qmc,
		executionCtx: ctx, cancel: cancel, AsyncRequestStorage: concurrent.NewMap[string, AsyncRequestResult](),
		AsyncQueriesContexts: concurrent.NewMap[string, *AsyncQueryContext](),
		transformationPipeline: TransformationPipeline{
			transformers: []model.QueryTransformer{
				&SchemaCheckPass{cfg: cfg.IndexConfig, schemaRegistry: schemaRegistry, logManager: lm}, // this can be a part of another plugin
			},
		},
		schemaRegistry:  schemaRegistry,
		ABResultsSender: abResultsRepository,
	}
}

func NewAsyncQueryContext(ctx context.Context, cancel context.CancelFunc, id string) *AsyncQueryContext {
	return &AsyncQueryContext{ctx: ctx, cancel: cancel, added: time.Now(), id: id}
}

// returns -1 when table name could not be resolved
func (q *QueryRunner) handleCount(ctx context.Context, indexPattern string) (int64, error) {
	indexes, err := q.logManager.ResolveIndexes(ctx, indexPattern)
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

type executionPlanResult struct {
	plan         *model.ExecutionPlan
	err          error
	responseBody []byte
}

func (q *QueryRunner) transformQueries(ctx context.Context, plan *model.ExecutionPlan, table *clickhouse.Table) {
	var err error
	plan.Queries, err = q.transformationPipeline.Transform(plan.Queries)
	if err != nil {
		logger.ErrorWithCtx(ctx).Msgf("error transforming queries: %v", err)
	}

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
			doneCh <- AsyncSearchWithError{err: err}
			return
		}

		if len(results) == 0 {
			logger.ErrorWithCtx(ctx).Msgf("no hits, sqls: %v", translatedQueryBody)
			doneCh <- AsyncSearchWithError{translatedQueryBody: translatedQueryBody, err: errors.New("no hits")}
			return
		}

		results, err = q.postProcessResults(table, results)
		if err != nil {
			doneCh <- AsyncSearchWithError{translatedQueryBody: translatedQueryBody, err: err}
		}

		searchResponse := queryTranslator.MakeSearchResponse(plan.Queries, results)

		doneCh <- AsyncSearchWithError{response: searchResponse, translatedQueryBody: translatedQueryBody, err: err}
	}()
}

func (q *QueryRunner) executeAlternativePlan(ctx context.Context, plan *model.ExecutionPlan, queryTranslator IQueryTranslator, table *clickhouse.Table, body types.JSON) (responseBody []byte, err error) {

	doneCh := make(chan AsyncSearchWithError, 1)

	q.transformQueries(ctx, plan, table)

	if resp, err := q.checkProperties(ctx, plan, table, queryTranslator); err != nil {
		return resp, err
	}

	q.runExecutePlanAsync(ctx, plan, queryTranslator, table, doneCh, nil)

	response := <-doneCh

	responseBody, err = response.response.Marshal()
	if err != nil {
		return nil, err
	}

	contextValues := tracing.ExtractValues(ctx)
	bodyAsBytes, _ := body.Bytes()
	pushAlternativeInfo(q.quesmaManagementConsole, contextValues.RequestId, "", contextValues.OpaqueId, contextValues.RequestPath, bodyAsBytes, response.translatedQueryBody, responseBody, plan.StartTime)

	return responseBody, response.err

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
				plan:         plan,
				err:          err,
				responseBody: responseBody,
			}
		}
	}

	q.transformQueries(ctx, plan, table)

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

// runAlternativePlanAndComparison runs the alternative plan and comparison method in the background. It returns a channel to collect the main plan results.
func (q *QueryRunner) runAlternativePlanAndComparison(ctx context.Context, alternativePlan *model.ExecutionPlan, table *clickhouse.Table, body types.JSON, queryTranslator IQueryTranslator) chan<- executionPlanResult {
	numberOfExpectedResults := len([]string{model.MainExecutionPlan, model.AlternativeExecutionPlan})

	optComparePlansCh := make(chan executionPlanResult, numberOfExpectedResults)

	// run alternative plan in the background (generator)
	go func(optComparePlansCh chan<- executionPlanResult) {
		defer recovery.LogPanic()

		// results are passed via channel
		newCtx := tracing.NewContextWithRequest(ctx)
		body, err := q.executeAlternativePlan(newCtx, alternativePlan, queryTranslator, table, body)

		optComparePlansCh <- executionPlanResult{
			plan:         alternativePlan,
			err:          err,
			responseBody: body,
		}

	}(optComparePlansCh)

	// collector
	go func(optComparePlansCh <-chan executionPlanResult) {
		defer recovery.LogPanic()
		var alternative executionPlanResult
		var main executionPlanResult

		for range numberOfExpectedResults {
			r := <-optComparePlansCh
			logger.InfoWithCtx(ctx).Msgf("received results  %s", r.plan.Name)
			if r.plan.Name == model.AlternativeExecutionPlan {
				alternative = r
			} else if r.plan.Name == model.MainExecutionPlan {
				main = r
			}
		}

		bytes, err := body.Bytes()
		if err != nil {
			bytes = []byte("error converting body to bytes")
		}

		abResult := ab_testing.Result{
			Request: ab_testing.Request{
				Path: "TODO",
				Body: string(bytes),
			},

			A: ab_testing.Response{
				Body: string(main.responseBody),
				Time: time.Since(main.plan.StartTime),
			},

			B: ab_testing.Response{
				Body: string(alternative.responseBody),
				Time: time.Since(alternative.plan.StartTime),
			},
		}

		q.ABResultsSender.Send(abResult)

		// TODO add JSON comparison here
		if string(alternative.responseBody) != string(main.responseBody) {
			logger.ErrorWithCtx(ctx).Msgf("alternative plan returned different results")
			// dump the results here, or
		} else {
			logger.InfoWithCtx(ctx).Msgf("alternative plan returned same results")
		}

	}(optComparePlansCh)

	return optComparePlansCh
}

func (q *QueryRunner) handleSearchCommon(ctx context.Context, indexPattern string, body types.JSON, optAsync *AsyncQuery, queryLanguage QueryLanguage) ([]byte, error) {
	sources, sourcesElastic, sourcesClickhouse := ResolveSources(indexPattern, q.cfg, q.im)

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
	} else if len(sourcesClickhouse) > 1 { // async search never worked for multiple indexes, TODO fix
		logger.WarnWithCtx(ctx).Msgf("requires union of multiple tables [%s], not yet supported, picking just one", indexPattern)
		sourcesClickhouse = sourcesClickhouse[1:2]
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

	resolvedTableName := sourcesClickhouse[0] // we got exactly one table here because of the check above

	incomingIndexName := resolvedTableName
	if len(q.cfg.IndexConfig[resolvedTableName].Override) > 0 {
		resolvedTableName = q.cfg.IndexConfig[resolvedTableName].Override
	}
	table, _ := tables.Load(resolvedTableName)
	if table == nil {
		return []byte{}, end_user_errors.ErrNoSuchTable.New(fmt.Errorf("can't load %s table", resolvedTableName)).Details("Table: %s", resolvedTableName)
	}

	queryTranslator := NewQueryTranslator(ctx, queryLanguage, table, q.logManager, q.DateMathRenderer, q.schemaRegistry, incomingIndexName, q.cfg)

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
	alternativePlan := q.maybeCreateAlternativeExecutionPlan(ctx, resolvedTableName, plan, queryTranslator, body)

	var optComparePlansCh chan<- executionPlanResult

	if alternativePlan != nil {
		optComparePlansCh = q.runAlternativePlanAndComparison(ctx, alternativePlan, table, body, queryTranslator)
	}

	return q.executePlan(ctx, plan, queryTranslator, table, body, optAsync, optComparePlansCh)
}

func (q *QueryRunner) maybeCreateAlternativeExecutionPlan(ctx context.Context, resolvedTableName string, plan *model.ExecutionPlan, queryTranslator IQueryTranslator, body types.JSON) *model.ExecutionPlan {

	props, enabled := q.cfg.IndexConfig[resolvedTableName].GetOptimizerConfiguration(queryparser.PancakeOptimizerName)
	if enabled && props["mode"] == "alternative" {

		hasAggQuery := false
		queriesWithoutAggr := make([]*model.Query, 0)
		for _, query := range plan.Queries {
			switch query.Type.AggregationType() {
			case model.MetricsAggregation, model.BucketAggregation, model.PipelineAggregation:
				hasAggQuery = true
			default:
				queriesWithoutAggr = append(queriesWithoutAggr, query)
			}
		}

		if hasAggQuery {
			if chQueryTranslator, ok := queryTranslator.(*queryparser.ClickhouseQueryTranslator); ok {

				// TODO FIXME check if the original plan has count query
				addCount := false

				if pancakeQueries, err := chQueryTranslator.PancakeParseAggregationJson(body, addCount); err == nil {
					logger.InfoWithCtx(ctx).Msgf("Running alternative pancake queries")
					queries := append(queriesWithoutAggr, pancakeQueries...)
					return &model.ExecutionPlan{
						IndexPattern:          plan.IndexPattern,
						QueryRowsTransformers: make([]model.QueryRowsTransformer, len(queries)),
						Queries:               queries,
						StartTime:             plan.StartTime,
						Name:                  model.AlternativeExecutionPlan,
					}
				} else {
					// TODO: change to info
					logger.ErrorWithCtx(ctx).Msgf("Error parsing pancake queries: %v", err)
				}
			} else {
				logger.ErrorWithCtx(ctx).Msgf("Alternative plan is not supported for non-clickhouse query translators")
			}
		}
	}
	return nil
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
		if err != nil {
			return nil, nil, err
		}

		results = append(results, rows)
		performance = append(performance, perf)
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
		if res.err == nil {
			results[res.jobId] = res.rows
			performances[res.jobId] = res.perf
		} else {
			return nil, nil, res.err
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
			return nil, clickhouse.PerformanceResult{}, err
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
		if query.NoDBQuery {
			logger.InfoWithCtx(ctx).Msgf("pipeline query: %+v", query)
			hits[i] = make([]model.QueryResultRow, 0)
			continue
		}

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

func (q *QueryRunner) postProcessResults(table *clickhouse.Table, results [][]model.QueryResultRow) ([][]model.QueryResultRow, error) {

	transformer := &replaceColumNamesWithFieldNames{}

	res, err := transformer.Transform(results)

	if err != nil {
		return nil, err
	}

	// TODO this should be created in different place
	geoIpTransformer := GeoIpResultTransformer{schemaRegistry: q.schemaRegistry, fromTable: table.Name}
	return geoIpTransformer.Transform(res)
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

func pushAlternativeInfo(qmc *ui.QuesmaManagementConsole, Id, AsyncId, OpaqueId, Path string, IncomingQueryBody []byte, QueryBodyTranslated []types.TranslatedSQLQuery, QueryTranslatedResults []byte, startTime time.Time) {
	qmc.PushSecondaryInfo(&ui.QueryDebugSecondarySource{
		Id:                     Id,
		AsyncId:                AsyncId,
		OpaqueId:               OpaqueId,
		Path:                   Path,
		IncomingQueryBody:      IncomingQueryBody,
		QueryBodyTranslated:    QueryBodyTranslated,
		QueryTranslatedResults: QueryTranslatedResults,
		SecondaryTook:          time.Since(startTime),
		IsAlternativePlan:      true})

}
