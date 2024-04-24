package quesma

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"mitmproxy/quesma/clickhouse"
	"mitmproxy/quesma/concurrent"
	"mitmproxy/quesma/elasticsearch"
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/model"
	"mitmproxy/quesma/queryparser"
	"mitmproxy/quesma/quesma/recovery"
	"mitmproxy/quesma/quesma/ui"
	"mitmproxy/quesma/tracing"
	"mitmproxy/quesma/util"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

const asyncQueriesLimit = 10000
const asyncQueriesLimitBytes = 1024 * 1024 * 500 // 500MB

var errIndexNotExists = errors.New("table does not exist")
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
	executionCtx         context.Context
	cancel               context.CancelFunc
	AsyncRequestStorage  *concurrent.Map[string, AsyncRequestResult]
	AsyncQueriesContexts *concurrent.Map[string, *AsyncQueryContext]
}

func NewQueryRunner() *QueryRunner {
	ctx, cancel := context.WithCancel(context.Background())
	return &QueryRunner{executionCtx: ctx, cancel: cancel, AsyncRequestStorage: concurrent.NewMap[string, AsyncRequestResult](), AsyncQueriesContexts: concurrent.NewMap[string, *AsyncQueryContext]()}
}

func NewAsyncQueryContext(ctx context.Context, cancel context.CancelFunc, id string) *AsyncQueryContext {
	return &AsyncQueryContext{ctx: ctx, cancel: cancel, added: time.Now(), id: id}
}

// returns -1 when table name could not be resolved
func (q *QueryRunner) handleCount(ctx context.Context, indexPattern string, lm *clickhouse.LogManager) (int64, error) {
	indexes := lm.ResolveIndexes(ctx, indexPattern)
	if len(indexes) == 0 {
		if elasticsearch.IsIndexPattern(indexPattern) {
			return 0, nil
		} else {
			logger.WarnWithCtx(ctx).Msgf("could not resolve table name for [%s]", indexPattern)
			return -1, errIndexNotExists
		}
	}

	if len(indexes) == 1 {
		return lm.Count(ctx, indexes[0])
	} else {
		return lm.CountMultiple(ctx, indexes...)
	}
}

func (q *QueryRunner) handleSearch(ctx context.Context, indexPattern string, body []byte, lm *clickhouse.LogManager,
	quesmaManagementConsole *ui.QuesmaManagementConsole) ([]byte, error) {
	return q.handleSearchCommon(ctx, indexPattern, body, lm, quesmaManagementConsole, false, 0, false, "")
}

func (q *QueryRunner) handleAsyncSearch(ctx context.Context, indexPattern string, body []byte, lm *clickhouse.LogManager,
	quesmaManagementConsole *ui.QuesmaManagementConsole, waitForResultsMs int, keepOnCompletion bool) ([]byte, error) {
	asyncRequestIdStr := generateAsyncRequestId()
	ctx = context.WithValue(ctx, tracing.AsyncIdCtxKey, asyncRequestIdStr)
	logger.InfoWithCtx(ctx).Msgf("async search request id: %s started", asyncRequestIdStr)
	return q.handleSearchCommon(ctx, indexPattern, body, lm, quesmaManagementConsole, true, waitForResultsMs, keepOnCompletion, asyncRequestIdStr)
}

type AsyncSearchWithError struct {
	response *model.AsyncSearchEntireResp
	err      error
}

func (q *QueryRunner) handleSearchCommon(ctx context.Context, indexPattern string, body []byte, lm *clickhouse.LogManager,
	quesmaManagementConsole *ui.QuesmaManagementConsole, async bool, waitForResultsMs int, keepOnCompletion bool, asyncRequestIdStr string) ([]byte, error) {

	resolved := lm.ResolveIndexes(ctx, indexPattern)
	if len(resolved) == 0 {
		if elasticsearch.IsIndexPattern(indexPattern) {
			if async {
				return queryparser.EmptyAsyncSearchResponse(asyncRequestIdStr, false, 200)
			} else {
				return queryparser.EmptySearchResponse(ctx), nil
			}
		} else {
			logger.WarnWithCtx(ctx).Msgf("could not resolve any table name for [%s]", indexPattern)
			return nil, errIndexNotExists
		}
	} else if len(resolved) > 1 { // async search never worked for multiple indexes, TODO fix
		logger.WarnWithCtx(ctx).Msgf("could not resolve multiple table names for [%s]", indexPattern)
		resolved = resolved[1:2]
	}

	var responseBody, translatedQueryBody []byte

	startTime := time.Now()
	pushSecondaryInfoToManagementConsole := func() {
		quesmaManagementConsole.PushSecondaryInfo(&ui.QueryDebugSecondarySource{
			Id:                     ctx.Value(tracing.RequestIdCtxKey).(string),
			IncomingQueryBody:      body,
			QueryBodyTranslated:    translatedQueryBody,
			QueryTranslatedResults: responseBody,
			SecondaryTook:          time.Since(startTime),
		})
	}

	doneCh := make(chan AsyncSearchWithError, 1)

	var hits, hitsFallback []model.QueryResultRow
	var aggregationResults [][]model.QueryResultRow
	oldHandlingUsed := false
	newAggregationHandlingUsed := false
	hitsPresent := false

	tables := lm.GetTableDefinitions()

	// TODO: variables below should be per table. Now they are not, as we only support one table.
	var queryTranslator *queryparser.ClickhouseQueryTranslator
	var highlighter queryparser.Highlighter
	var aggregations []model.QueryWithAggregation
	var err error
	var queryInfo model.SearchQueryInfo
	var count int

	for _, resolvedTableName := range resolved {
		table, _ := tables.Load(resolvedTableName)
		queryTranslator = &queryparser.ClickhouseQueryTranslator{ClickhouseLM: lm, Table: table, Ctx: ctx}
		var simpleQuery queryparser.SimpleQuery
		simpleQuery, queryInfo, highlighter = queryTranslator.ParseQuery(string(body))
		if simpleQuery.CanParse {
			if ((queryInfo.Typ == model.ListByField || queryInfo.Typ == model.ListAllFields || queryInfo.Typ == model.Normal) && !bytes.Contains(body, []byte("aggs"))) || queryInfo.Typ == model.Facets || queryInfo.Typ == model.FacetsNumeric {
				logger.InfoWithCtx(ctx).Msgf("received search request, type: %v, async: %v", queryInfo.Typ, async)

				if properties := q.findNonexistingProperties(queryInfo, simpleQuery, table); len(properties) > 0 {
					logger.DebugWithCtx(ctx).Msgf("properties %s not found in table %s", properties, table.Name)
					if elasticsearch.IsIndexPattern(indexPattern) {
						return queryparser.EmptySearchResponse(ctx), nil
					} else {
						return nil, fmt.Errorf("properties %s not found in table %s", properties, table.Name)
					}
				}

				oldHandlingUsed = true
				if async {
					go func() {
						defer recovery.LogPanicWithCtx(ctx)
						q.searchWorker(ctx, quesmaManagementConsole, asyncRequestIdStr, queryTranslator, table, body, doneCh, async)
					}()
				} else {
					translatedQueryBody, hits = q.searchWorker(ctx, quesmaManagementConsole, asyncRequestIdStr, queryTranslator, table, body, doneCh, async)
				}
			} else if aggregations, err = queryTranslator.ParseAggregationJson(string(body)); err == nil {
				newAggregationHandlingUsed = true
				if async {
					go func() {
						defer recovery.LogPanicWithCtx(ctx)
						q.searchAggregationWorker(ctx, quesmaManagementConsole, asyncRequestIdStr, aggregations, queryTranslator, table, body, doneCh, async)
					}()
				} else {
					translatedQueryBody, aggregationResults = q.searchAggregationWorker(ctx, quesmaManagementConsole, asyncRequestIdStr, aggregations, queryTranslator, table, body, doneCh, async)
				}
			}

			if !async && queryInfo.Size > 0 {
				hitsPresent = true
				var fieldName string
				if queryInfo.Typ == model.ListByField {
					fieldName = queryInfo.FieldName
				} else {
					fieldName = "*"
				}
				listQuery := queryTranslator.BuildNRowsQuery(fieldName, simpleQuery, queryInfo.Size)
				hitsFallback, err = queryTranslator.ClickhouseLM.ProcessSelectQuery(ctx, table, listQuery)
				if err != nil {
					logger.ErrorWithCtx(ctx).Msgf("error processing fallback query. Err: %v, query: %+v", err, listQuery)
					pushSecondaryInfoToManagementConsole()
					return responseBody, err
				}
				countQuery := queryTranslator.BuildSimpleCountQuery(simpleQuery.Sql.Stmt)
				countResult, err := queryTranslator.ClickhouseLM.ProcessSelectQuery(ctx, table, countQuery)
				if err != nil {
					logger.ErrorWithCtx(ctx).Msgf("error processing count query. Err: %v, query: %+v", err, countQuery)
					pushSecondaryInfoToManagementConsole()
					return responseBody, err
				}
				if len(countResult) > 0 {
					// This if only for tests... On production it'll never be 0.
					// When e.g. sqlmock starts supporting uint64, we can remove it.
					countRaw := countResult[0].Cols[0].Value
					if countExpectedType, ok := countRaw.(uint64); ok {
						count = int(countExpectedType)
					} else {
						logger.ErrorWithCtx(ctx).Msgf("unexpected count type: %T, count: %v. Defaulting to 0.", countRaw, countRaw)
						count = 0
					}
				}
			}
		} else {
			responseBody = []byte("Invalid Query, err: " + simpleQuery.Sql.Stmt)
			logger.ErrorWithCtxAndReason(ctx, "Quesma generated invalid SQL query").Msg(string(responseBody))
			pushSecondaryInfoToManagementConsole()
			return responseBody, errors.New(string(responseBody))
		}
	}

	/* TODO add this somehow, somewhere
		if err != nil {
		if elasticsearch.IsIndexPattern(indexPattern) {
			logger.WarnWithCtx(ctx).Msgf("Unprocessable: %s, err: %s, resolving to empty (desired behaviour)", fullQuery.String(), err.Error())
			continue
		} else {
			errorMsg := fmt.Sprintf("Error processing query: %s, err: %s", fullQuery.String(), err.Error())
			logger.ErrorWithCtx(ctx).Msg(errorMsg)
			responseBody = []byte(errorMsg)
			pushSecondaryInfoToManagementConsole()
			return responseBody, err
		}
	}
	*/

	if !async {
		var response, responseHits *model.SearchResp = nil, nil
		err = nil
		if oldHandlingUsed {
			response, err = queryTranslator.MakeSearchResponse(hits, queryInfo.Typ, highlighter)
		} else if newAggregationHandlingUsed {
			response = queryTranslator.MakeResponseAggregation(aggregations, aggregationResults)
		}
		if err != nil {
			logger.ErrorWithCtx(ctx).Msgf("error making response: %v, queryInfo: %+v, rows: %v", err, queryInfo, hits)
			pushSecondaryInfoToManagementConsole()
			return responseBody, err
		}

		if hitsPresent {
			if response == nil {
				response, err = queryTranslator.MakeSearchResponse(hitsFallback, queryInfo.Typ, highlighter)
			} else {
				responseHits, err = queryTranslator.MakeSearchResponse(hitsFallback, queryInfo.Typ, highlighter)
				response.Hits = responseHits.Hits
			}
			response.Hits.Total.Value = count
		}
		if err != nil {
			logger.ErrorWithCtx(ctx).Msgf("error making response: %v, queryInfo: %v, rows: %v", err, queryInfo, hitsFallback)
		}
		responseBody, err = response.Marshal()

		pushSecondaryInfoToManagementConsole()
		return responseBody, err
	} else {
		if waitForResultsMs == 0 {
			return queryparser.EmptyAsyncSearchResponse(asyncRequestIdStr, true, 200)
		}
		select {
		case <-time.After(time.Duration(waitForResultsMs) * time.Millisecond):
			return q.handlePartialAsyncSearch(ctx, asyncRequestIdStr)
		case res := <-doneCh:
			if !keepOnCompletion {
				q.AsyncRequestStorage.Delete(asyncRequestIdStr)
			}

			err = res.err

			var errMarshall error
			// Unwind errors
			if res.response != nil {
				res.response.CompletionStatus = nil
				responseBody, errMarshall = res.response.Marshal()
			}
			if err == nil {
				err = errMarshall
			}
			return responseBody, err
		}
	}
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
		logger.InfoWithCtx(ctx).Msgf("async query id : %s ended successfully", id)
		if result.isCompressed {
			return util.Decompress(result.responseBody)
		}
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
	q.AsyncRequestStorage.Store(asyncRequestIdStr, AsyncRequestResult{err: err, added: time.Now(), isCompressed: false})
	logger.ErrorWithCtx(ctx).Msgf("cannot handle %s, too many async queries", asyncRequestIdStr)
	doneCh <- AsyncSearchWithError{response: nil, err: err}
	return true
}

func (q *QueryRunner) addAsyncQueryContext(ctx context.Context, cancel context.CancelFunc, asyncRequestIdStr string) {
	q.AsyncQueriesContexts.Store(asyncRequestIdStr, NewAsyncQueryContext(ctx, cancel, asyncRequestIdStr))
}

func (q *QueryRunner) searchWorkerCommon(ctx context.Context, quesmaManagementConsole *ui.QuesmaManagementConsole, asyncRequestIdStr string, queryTranslator *queryparser.ClickhouseQueryTranslator,
	table *clickhouse.Table, body []byte, doneCh chan<- AsyncSearchWithError, async bool) (translatedQueryBody []byte, hits []model.QueryResultRow) {
	if async && q.reachedQueriesLimit(ctx, asyncRequestIdStr, doneCh) {
		return
	}

	var err error
	var fullQuery *model.Query
	id := ctx.Value(tracing.RequestIdCtxKey).(string)
	startTime := time.Now()
	simpleQuery, queryInfo, highlighter := queryTranslator.ParseQuery(string(body))
	var dbQueryCtx context.Context
	if async {
		var dbCancel context.CancelFunc
		dbQueryCtx, dbCancel = context.WithCancel(context.Background())
		q.addAsyncQueryContext(dbQueryCtx, dbCancel, asyncRequestIdStr)
	} else {
		dbQueryCtx = ctx
	}

	switch queryInfo.Typ {
	case model.CountAsync:
		fullQuery = queryTranslator.BuildSimpleCountQuery(simpleQuery.Sql.Stmt)
		hits, err = queryTranslator.ClickhouseLM.ProcessSelectQuery(dbQueryCtx, table, fullQuery)

	case model.Facets, model.FacetsNumeric:
		// queryInfo = (Facets, fieldName, Limit results, Limit last rows to look into)
		fullQuery = queryTranslator.BuildFacetsQuery(queryInfo.FieldName, simpleQuery, queryInfo.I2)
		hits, err = queryTranslator.ClickhouseLM.ProcessFacetsQuery(dbQueryCtx, table, fullQuery)

	case model.ListByField:
		// queryInfo = (ListByField, fieldName, 0, LIMIT)
		fullQuery = queryTranslator.BuildNRowsQuery(queryInfo.FieldName, simpleQuery, queryInfo.I2)
		hits, err = queryTranslator.ClickhouseLM.ProcessSelectQuery(dbQueryCtx, table, fullQuery)

	case model.ListAllFields:
		// queryInfo = (ListAllFields, "*", 0, LIMIT)
		fullQuery = queryTranslator.BuildNRowsQuery("*", simpleQuery, queryInfo.I2)
		hits, err = queryTranslator.ClickhouseLM.ProcessSelectQuery(dbQueryCtx, table, fullQuery)

	case model.Normal:
		fullQuery = queryTranslator.BuildSimpleSelectQuery(simpleQuery.Sql.Stmt)
		hits, err = queryTranslator.ClickhouseLM.ProcessSelectQuery(dbQueryCtx, table, fullQuery)

	default:
		logger.ErrorWithCtx(ctx).Msgf("unknown query type: %v, query body: %v", queryInfo.Typ, body)
	}
	if fullQuery != nil {
		translatedQueryBody = []byte(fullQuery.String())
	}
	if err != nil {
		logger.ErrorWithCtx(ctx).Msgf("Rows: %+v, err: %+v", hits, err)
		if async {
			q.AsyncRequestStorage.Store(asyncRequestIdStr, AsyncRequestResult{responseBody: []byte{}, added: time.Now(), err: err, isCompressed: false})
			doneCh <- AsyncSearchWithError{nil, err}
			return
		}
	}
	if async {
		searchResponse, err := queryTranslator.MakeSearchResponse(hits, queryInfo.Typ, highlighter)
		if err != nil {
			logger.ErrorWithCtx(ctx).Msgf("error making response: %v, queryInfo: %+v, rows: %v", err, queryInfo, hits)
			q.AsyncRequestStorage.Store(asyncRequestIdStr, AsyncRequestResult{responseBody: []byte{}, added: time.Now(), err: err, isCompressed: false})
			doneCh <- AsyncSearchWithError{nil, err}
			return
		}
		q.storeAsyncResponse(quesmaManagementConsole, asyncRequestIdStr,
			searchResponse, id, body, translatedQueryBody, startTime, doneCh)
	}
	return
}

func (q *QueryRunner) searchWorker(ctx context.Context, quesmaManagementConsole *ui.QuesmaManagementConsole, asyncRequestIdStr string, queryTranslator *queryparser.ClickhouseQueryTranslator,
	table *clickhouse.Table, body []byte, doneCh chan<- AsyncSearchWithError, async bool) (translatedQueryBody []byte, hits []model.QueryResultRow) {
	if !async {
		return q.searchWorkerCommon(ctx, quesmaManagementConsole, asyncRequestIdStr, queryTranslator, table, body, doneCh, async)
	} else {
		select {
		case <-q.executionCtx.Done():
			return
		default:
			_, _ = q.searchWorkerCommon(ctx, quesmaManagementConsole, asyncRequestIdStr, queryTranslator, table, body, doneCh, async)
			return
		}
	}
}

func (q *QueryRunner) storeAsyncResponse(quesmaManagementConsole *ui.QuesmaManagementConsole,
	asyncRequestIdStr string, searchResponse *model.SearchResp,
	id string, body []byte, translatedQueryBody []byte,
	startTime time.Time, doneCh chan<- AsyncSearchWithError) {
	const isPartial = false
	asyncSearchResponse := queryparser.SearchToAsyncSearchResponse(searchResponse, asyncRequestIdStr, isPartial, 200)
	responseBody, err := asyncSearchResponse.Marshal()
	quesmaManagementConsole.PushSecondaryInfo(&ui.QueryDebugSecondarySource{
		Id:                     id,
		IncomingQueryBody:      body,
		QueryBodyTranslated:    translatedQueryBody,
		QueryTranslatedResults: responseBody,
		SecondaryTook:          time.Since(startTime),
	})
	isCompressed := false
	if err == nil {
		if compressed, compErr := util.Compress(responseBody); compErr == nil {
			responseBody = compressed
			isCompressed = true
		}
	}
	q.AsyncRequestStorage.Store(asyncRequestIdStr,
		AsyncRequestResult{
			responseBody: responseBody, added: time.Now(), err: err, isCompressed: isCompressed})
	doneCh <- AsyncSearchWithError{response: asyncSearchResponse, err: err}
}

func (q *QueryRunner) searchAggregationWorkerCommon(ctx context.Context, quesmaManagementConsole *ui.QuesmaManagementConsole, asyncRequestIdStr string, aggregations []model.QueryWithAggregation,
	queryTranslator *queryparser.ClickhouseQueryTranslator, table *clickhouse.Table, body []byte,
	doneCh chan<- AsyncSearchWithError, async bool) (translatedQueryBody []byte, resultRows [][]model.QueryResultRow) {

	if async && q.reachedQueriesLimit(ctx, asyncRequestIdStr, doneCh) {
		return
	}

	sqls := ""
	id := ctx.Value(tracing.RequestIdCtxKey).(string)
	startTime := time.Now()
	var dbQueryCtx context.Context
	if async {
		var dbCancel context.CancelFunc
		dbQueryCtx, dbCancel = context.WithCancel(context.Background())
		q.addAsyncQueryContext(dbQueryCtx, dbCancel, asyncRequestIdStr)
	} else {
		dbQueryCtx = ctx
	}
	logger.InfoWithCtx(ctx).Msg("we're using new Aggregation handling.")
	for _, agg := range aggregations {
		logger.InfoWithCtx(ctx).Msg(agg.String()) // I'd keep for now until aggregations work fully
		sqls += agg.Query.String() + "\n"
		rows, err := queryTranslator.ClickhouseLM.ProcessGeneralAggregationQuery(dbQueryCtx, table, &agg.Query)
		if err != nil {
			logger.ErrorWithCtx(ctx).Msg(err.Error())
			continue
		}
		resultRows = append(resultRows, rows)
	}
	translatedQueryBody = []byte(sqls)
	if async {
		searchResponse := queryTranslator.MakeResponseAggregation(aggregations, resultRows)
		q.storeAsyncResponse(quesmaManagementConsole, asyncRequestIdStr,
			searchResponse, id, body, translatedQueryBody, startTime, doneCh)
	}
	return
}

func (q *QueryRunner) searchAggregationWorker(ctx context.Context, quesmaManagementConsole *ui.QuesmaManagementConsole, asyncRequestIdStr string, aggregations []model.QueryWithAggregation,
	queryTranslator *queryparser.ClickhouseQueryTranslator, table *clickhouse.Table, body []byte,
	doneCh chan<- AsyncSearchWithError, async bool) (translatedQueryBody []byte, resultRows [][]model.QueryResultRow) {
	if !async {
		return q.searchAggregationWorkerCommon(ctx, quesmaManagementConsole, asyncRequestIdStr, aggregations, queryTranslator, table, body, doneCh, async)
	} else {
		select {
		case <-q.executionCtx.Done():
			return
		default:
			_, _ = q.searchAggregationWorkerCommon(ctx, quesmaManagementConsole, asyncRequestIdStr, aggregations, queryTranslator, table, body, doneCh, async)
			return
		}
	}
}

func (q *QueryRunner) Close() {
	q.cancel()
	logger.Info().Msg("queryRunner Stopped")
}

func (q *QueryRunner) findNonexistingProperties(queryInfo model.SearchQueryInfo, simpleQuery queryparser.SimpleQuery, table *clickhouse.Table) []string {
	var results = make([]string, 0)
	var allReferencedFields = make([]string, 0)
	allReferencedFields = append(allReferencedFields, queryInfo.RequestedFields...)
	for _, field := range simpleQuery.SortFields {
		allReferencedFields = append(allReferencedFields, strings.ReplaceAll(strings.Fields(field)[0], `"`, ""))
	}

	for _, property := range allReferencedFields {
		if property != "*" && !table.HasColumn(q.executionCtx, property) {
			results = append(results, property)
		}
	}
	return results
}
