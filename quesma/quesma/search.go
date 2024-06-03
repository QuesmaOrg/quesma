package quesma

import (
	"context"
	"errors"
	"fmt"
	"mitmproxy/quesma/clickhouse"
	"mitmproxy/quesma/concurrent"
	"mitmproxy/quesma/elasticsearch"
	"mitmproxy/quesma/end_user_errors"
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/model"
	"mitmproxy/quesma/queryparser"
	"mitmproxy/quesma/queryparser/aexp"
	"mitmproxy/quesma/queryparser/query_util"
	"mitmproxy/quesma/quesma/config"
	"mitmproxy/quesma/quesma/recovery"
	"mitmproxy/quesma/quesma/types"
	"mitmproxy/quesma/quesma/ui"
	"mitmproxy/quesma/tracing"
	"mitmproxy/quesma/util"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const asyncQueriesLimit = 10000
const asyncQueriesLimitBytes = 1024 * 1024 * 500 // 500MB

var (
	errIndexNotExists       = errors.New("table does not exist")
	errCouldNotParseRequest = errors.New("parse exception")
)
var asyncRequestId atomic.Int64

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
	DateMathRenderer string // "clickhouse_interval" or "literal"  if not set, we use "clickhouse_interval"
}

func NewQueryRunner(lm *clickhouse.LogManager, cfg config.QuesmaConfiguration, im elasticsearch.IndexManagement, qmc *ui.QuesmaManagementConsole) *QueryRunner {
	ctx, cancel := context.WithCancel(context.Background())
	return &QueryRunner{logManager: lm, cfg: cfg, im: im, quesmaManagementConsole: qmc, executionCtx: ctx, cancel: cancel, AsyncRequestStorage: concurrent.NewMap[string, AsyncRequestResult](), AsyncQueriesContexts: concurrent.NewMap[string, *AsyncQueryContext]()}
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
			return -1, errIndexNotExists
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
		asyncRequestIdStr: generateAsyncRequestId(),
		waitForResultsMs:  waitForResultsMs,
		keepOnCompletion:  keepOnCompletion,
		startTime:         time.Now(),
	}
	ctx = context.WithValue(ctx, tracing.AsyncIdCtxKey, async.asyncRequestIdStr)
	logger.InfoWithCtx(ctx).Msgf("async search request id: %s started", async.asyncRequestIdStr)
	return q.handleSearchCommon(ctx, indexPattern, body, &async, QueryLanguageDefault)
}

type AsyncSearchWithError struct {
	response            *model.SearchResp
	translatedQueryBody []byte
	err                 error
}

type AsyncQuery struct {
	asyncRequestIdStr string
	waitForResultsMs  int
	keepOnCompletion  bool
	startTime         time.Time
}

func (q *QueryRunner) handleSearchCommon(ctx context.Context, indexPattern string, body types.JSON, optAsync *AsyncQuery, queryLanguage QueryLanguage) ([]byte, error) {
	sources, sourcesElastic, sourcesClickhouse := ResolveSources(indexPattern, q.cfg, q.im)

	switch sources {
	case sourceBoth:

		err := end_user_errors.ErrSearchCondition.New(fmt.Errorf("index pattern [%s] resolved to both elasticsearch indices: [%s] and clickhouse tables: [%s]", indexPattern, sourcesElastic, sourcesClickhouse))

		var resp []byte
		if optAsync != nil {
			resp, _ = queryparser.EmptyAsyncSearchResponse(optAsync.asyncRequestIdStr, false, 200)
		} else {
			resp = queryparser.EmptySearchResponse(ctx)
		}
		return resp, err
	case sourceNone:
		if elasticsearch.IsIndexPattern(indexPattern) {
			if optAsync != nil {
				return queryparser.EmptyAsyncSearchResponse(optAsync.asyncRequestIdStr, false, 200)
			} else {
				return queryparser.EmptySearchResponse(ctx), nil
			}
		} else {
			logger.WarnWithCtx(ctx).Msgf("could not resolve any table name for [%s]", indexPattern)
			return nil, errIndexNotExists
		}
	case sourceClickhouse:
		logger.Debug().Msgf("index pattern [%s] resolved to clickhouse tables: [%s]", indexPattern, sourcesClickhouse)
	case sourceElasticsearch:
		return nil, end_user_errors.ErrSearchCondition.New(fmt.Errorf("index pattern [%s] resolved to elasticsearch indices: [%s]", indexPattern, sourcesElastic))
	}
	logger.Debug().Msgf("resolved sources for index pattern %s -> %s", indexPattern, sources)

	if len(sourcesClickhouse) == 0 {
		if elasticsearch.IsIndexPattern(indexPattern) {
			if optAsync != nil {
				return queryparser.EmptyAsyncSearchResponse(optAsync.asyncRequestIdStr, false, 200)
			} else {
				return queryparser.EmptySearchResponse(ctx), nil
			}
		} else {
			logger.WarnWithCtx(ctx).Msgf("could not resolve any table name for [%s]", indexPattern)
			return nil, errIndexNotExists
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

	for _, resolvedTableName := range sourcesClickhouse {
		var err error
		doneCh := make(chan AsyncSearchWithError, 1)

		table, _ := tables.Load(resolvedTableName)
		if table == nil {
			return []byte{}, end_user_errors.ErrNoSuchTable.New(fmt.Errorf("can't load %s table", resolvedTableName)).Details("Table: %s", resolvedTableName)
		}

		queryTranslator := NewQueryTranslator(ctx, queryLanguage, table, q.logManager, q.DateMathRenderer)

		queries, isAggregation, canParse, err := queryTranslator.ParseQuery(body)

		if canParse {
			if query_util.IsNonAggregationQuery(queries[0].QueryInfoType, body) {
				if properties := q.findNonexistingProperties(queries[0], table); len(properties) > 0 {
					logger.DebugWithCtx(ctx).Msgf("properties %s not found in table %s", properties, table.Name)
					if elasticsearch.IsIndexPattern(indexPattern) {
						return queryparser.EmptySearchResponse(ctx), nil
					} else {
						return nil, fmt.Errorf("properties %s not found in table %s", properties, table.Name)
					}
				}
			}

			if !isAggregation {
				go func() {
					defer recovery.LogAndHandlePanic(ctx, func(err error) {
						doneCh <- AsyncSearchWithError{err: err}
					})

					translatedQueryBody, hitsSlice, err := q.searchWorker(ctx, queries, table, doneCh, optAsync)
					if err != nil {
						doneCh <- AsyncSearchWithError{err: err}
						return
					}

					if len(hitsSlice) == 0 {
						logger.ErrorWithCtx(ctx).Msgf("no hits, queryInfo: %d", translatedQueryBody)
						doneCh <- AsyncSearchWithError{translatedQueryBody: translatedQueryBody, err: errors.New("no hits")}
						return
					}
					searchResponse, err := queryTranslator.MakeSearchResponse(hitsSlice[0], queries[0])
					if err != nil {
						logger.ErrorWithCtx(ctx).Msgf("error making response: %v, queryInfoType: %+v, rows: %v", err, queries[0].QueryInfoType, hitsSlice[0])
					}
					doneCh <- AsyncSearchWithError{response: searchResponse, translatedQueryBody: translatedQueryBody, err: err}
				}()
			} else {
				go func() {
					defer recovery.LogAndHandlePanic(ctx, func(err error) {
						doneCh <- AsyncSearchWithError{err: err}
					})

					translatedQueryBody, aggregationResults, err := q.searchWorker(ctx, queries, table, doneCh, optAsync)

					searchResponse := queryTranslator.MakeResponseAggregation(queries, aggregationResults)
					doneCh <- AsyncSearchWithError{response: searchResponse, translatedQueryBody: translatedQueryBody, err: err}
				}()
			}
		} else {
			queriesBody := ""
			for _, query := range queries {
				queriesBody += query.String(ctx) + "\n"
			}
			responseBody = []byte(fmt.Sprintf("Invalid Queries: %s, err: %v", queriesBody, err))
			logger.ErrorWithCtxAndReason(ctx, "Quesma generated invalid SQL query").Msg(queriesBody)
			bodyAsBytes, _ := body.Bytes()
			pushSecondaryInfo(q.quesmaManagementConsole, id, path, bodyAsBytes, []byte(queriesBody), responseBody, startTime)
			return responseBody, errors.New(string(responseBody))
		}

		if optAsync == nil {
			bodyAsBytes, _ := body.Bytes()
			response := <-doneCh
			if response.err != nil {
				err = response.err
				logger.ErrorWithCtx(ctx).Msgf("error making response: %v, QueryInfoType: %+v", err, queries[0].QueryInfoType)
			} else {
				responseBody, err = response.response.Marshal()
			}
			pushSecondaryInfo(q.quesmaManagementConsole, id, path, bodyAsBytes, response.translatedQueryBody, responseBody, startTime)
			return responseBody, err
		} else {
			select {
			case <-time.After(time.Duration(optAsync.waitForResultsMs) * time.Millisecond):
				go func() { // Async search takes longer. Return partial results and wait for
					recovery.LogPanicWithCtx(ctx)
					res := <-doneCh
					q.storeAsyncSearch(q.quesmaManagementConsole, id, optAsync.asyncRequestIdStr, optAsync.startTime, path, body, res, true)
				}()
				return q.handlePartialAsyncSearch(ctx, optAsync.asyncRequestIdStr)
			case res := <-doneCh:
				responseBody, err = q.storeAsyncSearch(q.quesmaManagementConsole, id, optAsync.asyncRequestIdStr, optAsync.startTime, path, body, res,
					optAsync.keepOnCompletion)

				return responseBody, err
			}
		}
	}

	return responseBody, nil
}

func (q *QueryRunner) storeAsyncSearch(qmc *ui.QuesmaManagementConsole, id, asyncRequestIdStr string,
	startTime time.Time, path string, body types.JSON, result AsyncSearchWithError, keep bool) (responseBody []byte, err error) {
	took := time.Since(startTime)
	if result.err != nil {
		if keep {
			q.AsyncRequestStorage.Store(asyncRequestIdStr, AsyncRequestResult{err: result.err, added: time.Now(),
				isCompressed: false})
		}
		responseBody, _ = queryparser.EmptyAsyncSearchResponse(asyncRequestIdStr, false, 503)
		err = result.err
		bodyAsBytes, _ := body.Bytes()
		qmc.PushSecondaryInfo(&ui.QueryDebugSecondarySource{
			Id:                     id,
			Path:                   path,
			IncomingQueryBody:      bodyAsBytes,
			QueryBodyTranslated:    result.translatedQueryBody,
			QueryTranslatedResults: responseBody,
			SecondaryTook:          took,
		})
		return
	}
	asyncResponse := queryparser.SearchToAsyncSearchResponse(result.response, asyncRequestIdStr, false, 200)
	responseBody, err = asyncResponse.Marshal()
	bodyAsBytes, _ := body.Bytes()
	qmc.PushSecondaryInfo(&ui.QueryDebugSecondarySource{
		Id:                     id,
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
		q.AsyncRequestStorage.Store(asyncRequestIdStr,
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

func generateAsyncRequestId() string {
	return "quesma_async_search_id_" + strconv.FormatInt(asyncRequestId.Add(1), 10)
}

func (q *QueryRunner) handlePartialAsyncSearch(ctx context.Context, id string) ([]byte, error) {
	if !strings.Contains(id, "quesma_async_search_id_") {
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
	if !strings.Contains(id, "quesma_async_search_id_") {
		return nil, errors.New("invalid quesma async search id : " + id)
	}
	q.AsyncRequestStorage.Delete(id)
	return []byte{}, nil
}

func (q *QueryRunner) reachedQueriesLimit(ctx context.Context, asyncRequestIdStr string, doneCh chan<- AsyncSearchWithError) bool {
	if q.AsyncRequestStorage.Size() < asyncQueriesLimit && q.asyncQueriesCumulatedBodySize() < asyncQueriesLimitBytes {
		return false
	}
	err := errors.New("too many async queries")
	logger.ErrorWithCtx(ctx).Msgf("cannot handle %s, too many async queries", asyncRequestIdStr)
	doneCh <- AsyncSearchWithError{err: err}
	return true
}

func (q *QueryRunner) addAsyncQueryContext(ctx context.Context, cancel context.CancelFunc, asyncRequestIdStr string) {
	q.AsyncQueriesContexts.Store(asyncRequestIdStr, NewAsyncQueryContext(ctx, cancel, asyncRequestIdStr))
}

func (q *QueryRunner) isInternalKibanaQuery(query model.Query) bool {
	for _, column := range query.Columns {
		if strings.Contains(column.SQL(), "data_stream.") {
			return true
		}
	}
	return false
}

type QueryJob func() ([]model.QueryResultRow, error)

func runQueryJobsSequence(ctx context.Context, jobs []QueryJob) ([][]model.QueryResultRow, time.Duration, error) {
	var results = make([][]model.QueryResultRow, 0)

	start := time.Now()

	for _, job := range jobs {
		rows, err := job()
		if err != nil {
			return nil, time.Since(start), err
		}
		results = append(results, rows)
	}

	logger.InfoWithCtx(ctx).Msgf("XXX SEQ %d %v", len(jobs), time.Since(start))

	return results, time.Since(start), nil
}

func runQueryJobsParallel(ctx context.Context, jobs []QueryJob) ([][]model.QueryResultRow, time.Duration, error) {
	var results = make([][]model.QueryResultRow, len(jobs))
	var errs = make([]error, len(jobs))

	var wg sync.WaitGroup

	start := time.Now()

	for n, job := range jobs {
		wg.Add(1)

		go func(number int, j QueryJob) {

			defer wg.Done()
			rows, err := j()
			if err != nil {
				errs[number] = err
				return
			}
			results[number] = rows

		}(n, job)

	}

	wg.Wait()

	for _, err := range errs {
		if err != nil {
			return nil, time.Since(start), err
		}
	}

	logger.InfoWithCtx(ctx).Msgf("XXX PAR %d %v", len(jobs), time.Since(start))

	return results, time.Since(start), nil
}

func (q *QueryRunner) searchWorkerCommon(
	ctx context.Context,
	queries []model.Query,
	table *clickhouse.Table) (translatedQueryBody []byte, hits [][]model.QueryResultRow, err error) {
	sqls := ""

	var sum int64
	var times []int64

	var jobs []QueryJob

	for _, query := range queries {
		if query.NoDBQuery {
			logger.InfoWithCtx(ctx).Msgf("pipeline query: %+v", query)
		} else {
			sql := query.String(ctx)
			logger.InfoWithCtx(ctx).Msgf("SQL: %s", sql)
			sqls += sql + "\n"
		}

		// This is a HACK
		// This should be removed when we have a schema resolver working.
		// It ignores queries against data_stream fields. These queries are kibana internal ones.
		if q.isInternalKibanaQuery(query) {
			continue
		}

		t := time.Now().UnixMilli()

		job := func() ([]model.QueryResultRow, error) {

			rows, err := q.logManager.ProcessQuery(ctx, table, &query)

			if err != nil {
				logger.ErrorWithCtx(ctx).Msg(err.Error())
				return nil, err
			}

			if query.Type != nil {
				rows = query.Type.PostprocessResults(rows)
			}

			return rows, nil
		}

		jobs = append(jobs, job)

		elapsed := time.Now().UnixMilli() - t
		times = append(times, elapsed)
		sum += elapsed
	}
	fmt.Println("XXX RUN SEQ", len(jobs))
	hits2, t2, err2 := runQueryJobsSequence(ctx, jobs)
	time.Sleep(2 * time.Second)
	fmt.Println("XXXX RUN PARALLEL", len(jobs))
	hits1, t1, err1 := runQueryJobsParallel(ctx, jobs)

	fmt.Println("XXX ", len(jobs), " SEQUENCE RES", t2, len(hits2), err2, "PARALLEL", t1, len(hits1), err1)

	hits = hits1
	err = err1

	translatedQueryBody = []byte(sqls)
	return
}

func (q *QueryRunner) searchWorker(ctx context.Context,
	aggregations []model.Query,
	table *clickhouse.Table,
	doneCh chan<- AsyncSearchWithError,
	optAsync *AsyncQuery) (translatedQueryBody []byte, resultRows [][]model.QueryResultRow, err error) {
	if optAsync != nil {
		if q.reachedQueriesLimit(ctx, optAsync.asyncRequestIdStr, doneCh) {
			return
		}
		// We need different ctx as our cancel is no longer tied to HTTP request, but to overall timeout.
		dbQueryCtx, dbCancel := context.WithCancel(tracing.NewContextWithRequest(ctx))
		q.addAsyncQueryContext(dbQueryCtx, dbCancel, optAsync.asyncRequestIdStr)
		ctx = dbQueryCtx
	}

	return q.searchWorkerCommon(ctx, aggregations, table)
}

func (q *QueryRunner) Close() {
	q.cancel()
	logger.Info().Msg("queryRunner Stopped")
}

func (q *QueryRunner) findNonexistingProperties(query model.Query, table *clickhouse.Table) []string {
	// this is not fully correct, but we keep it backward compatible
	var results = make([]string, 0)
	var allReferencedFields = make([]string, 0)
	for _, col := range query.Columns {
		for _, c := range aexp.GetUsedColumns(col.Expression) {
			allReferencedFields = append(allReferencedFields, c.ColumnName)
		}
	}
	allReferencedFields = append(allReferencedFields, query.OrderByFieldNames()...)

	for _, property := range allReferencedFields {
		if property != "*" && !table.HasColumn(q.executionCtx, property) {
			results = append(results, property)
		}
	}
	return results
}

func pushSecondaryInfo(qmc *ui.QuesmaManagementConsole, Id, Path string, IncomingQueryBody, QueryBodyTranslated, QueryTranslatedResults []byte, startTime time.Time) {
	qmc.PushSecondaryInfo(&ui.QueryDebugSecondarySource{
		Id:                     Id,
		Path:                   Path,
		IncomingQueryBody:      IncomingQueryBody,
		QueryBodyTranslated:    QueryBodyTranslated,
		QueryTranslatedResults: QueryTranslatedResults,
		SecondaryTook:          time.Since(startTime)})
}
