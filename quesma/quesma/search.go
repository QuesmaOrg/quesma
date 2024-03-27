package quesma

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"mitmproxy/quesma/clickhouse"
	"mitmproxy/quesma/concurrent"
	"mitmproxy/quesma/elasticsearch"
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/model"
	"mitmproxy/quesma/queryparser"
	"mitmproxy/quesma/quesma/ui"
	"mitmproxy/quesma/tracing"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

const AsyncQueriesLimit = 1000

var asyncRequestId int64 = 0

type AsyncRequestResult struct {
	isAggregation        bool
	queryTranslator      *queryparser.ClickhouseQueryTranslator
	highlighter          queryparser.Highlighter
	asyncSearchQueryType model.AsyncSearchQueryType
	aggregations         []model.QueryWithAggregation
	rows                 []model.QueryResultRow
	aggregationRows      [][]model.QueryResultRow
	id                   string
	body                 []byte
	translatedQueryBody  []byte
	err                  error
	took                 time.Duration
	added                time.Time
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

func (q *QueryRunner) handleCount(ctx context.Context, indexPattern string, lm *clickhouse.LogManager) (int64, error) {
	indexes := lm.ResolveIndexes(indexPattern)
	if len(indexes) == 0 {
		if elasticsearch.IsIndexPattern(indexPattern) {
			return 0, nil
		} else {
			logger.WarnWithCtx(ctx).Msgf("could not resolve table name for [%s]", indexPattern)
			return -1, errors.New("could not resolve table name")
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

	id := ctx.Value(tracing.RequestIdCtxKey).(string)
	resolved := lm.ResolveIndexes(indexPattern)
	if len(resolved) == 0 {
		logger.WarnWithCtx(ctx).Str(logger.RID, id).Msgf("could not resolve any table name for [%s]", indexPattern)
		return nil, errors.New("could not resolve table name")
	}

	var rawResults, responseBody, translatedQueryBody []byte

	startTime := time.Now()
	pushSecondaryInfoToManagementConsole := func() {
		quesmaManagementConsole.PushSecondaryInfo(&ui.QueryDebugSecondarySource{
			Id:                     id,
			IncomingQueryBody:      body,
			QueryBodyTranslated:    translatedQueryBody,
			QueryRawResults:        rawResults,
			QueryTranslatedResults: responseBody,
			SecondaryTook:          time.Since(startTime),
		})
	}

	var allRows []model.QueryResultRow
	var queryType model.SearchQueryType

	for _, resolvedTableName := range resolved {
		table := lm.GetTable(resolvedTableName)
		queryTranslator := &queryparser.ClickhouseQueryTranslator{ClickhouseLM: lm, Table: table}
		simpleQuery, queryInfo := queryTranslator.ParseQuery(string(body))
		if simpleQuery.CanParse {
			queryType = queryInfo
			var fullQuery *model.Query
			switch queryInfo {
			case model.Count:
				fullQuery = queryTranslator.BuildSimpleCountQuery(simpleQuery.Sql.Stmt)
			case model.Normal:
				fullQuery = queryTranslator.BuildSimpleSelectQuery(simpleQuery.Sql.Stmt)
			}
			translatedQueryBody = []byte(fullQuery.String())
			rows, err := queryTranslator.ClickhouseLM.ProcessSimpleSelectQuery(ctx, table, fullQuery)
			if err != nil {
				errorMsg := fmt.Sprintf("Error processing query: %s, err: %s", fullQuery.String(), err.Error())
				logger.ErrorWithCtx(ctx).Msg(errorMsg)
				responseBody = []byte(errorMsg)
				pushSecondaryInfoToManagementConsole()
				return responseBody, err
			}
			allRows = append(allRows, rows...)
		} else {
			responseBody = []byte("Invalid Query, err: " + simpleQuery.Sql.Stmt)
			logger.ErrorWithCtxAndReason(ctx, "Quesma generated invalid SQL query").Msg(string(responseBody))
			pushSecondaryInfoToManagementConsole()
			return responseBody, errors.New(string(responseBody))
		}
	}

	responseBody, err := queryparser.MakeResponseSearchQuery(allRows, queryType)
	if err != nil {
		logger.ErrorWithCtx(ctx).Msgf("Error making response: %v rows: %v", err, allRows)
		pushSecondaryInfoToManagementConsole()
		return responseBody, err
	}

	pushSecondaryInfoToManagementConsole()
	return responseBody, nil
}

func createAsyncSearchResponseHitJson(ctx context.Context,
	rows []model.QueryResultRow,
	typ model.AsyncSearchQueryType,
	queryTranslator *queryparser.ClickhouseQueryTranslator,
	highlighter queryparser.Highlighter,
	asyncRequestIdStr string,
	isPartial bool) ([]byte, error) {
	responseBody, err := queryTranslator.MakeResponseAsyncSearchQuery(rows, typ, highlighter, asyncRequestIdStr, isPartial)
	if err != nil {
		logger.ErrorWithCtx(ctx).Msgf("%v rows: %v", err, rows)
		return nil, err
	}
	return responseBody, nil
}

func generateAsyncRequestId() string {
	atomic.AddInt64(&asyncRequestId, 1)
	return "quesma_async_search_id_" + strconv.FormatInt(asyncRequestId, 10)
}

func createEmptyAsyncSearchResponse(id string, isPartial bool, status int) ([]byte, error) {
	hits := make([]model.SearchHit, 0) // need to remove count result from hits
	total := &model.Total{
		Value: 0,
	}
	response := model.AsyncSearchEntireResp{
		Response: model.SearchResp{
			Hits: model.SearchHits{
				Total: total,
				Hits:  hits,
			},
		},
	}
	response.ID = &id
	response.IsPartial = isPartial
	response.IsRunning = isPartial
	response.CompletionStatus = &status
	return json.MarshalIndent(response, "", "  ")
}

func (q *QueryRunner) handlePartialAsyncSearch(id string, quesmaManagementConsole *ui.QuesmaManagementConsole) ([]byte, error) {
	if !strings.Contains(id, "quesma_async_search_id_") {
		return createEmptyAsyncSearchResponse(id, false, 503)
	}
	if result, ok := q.AsyncRequestStorage.Load(id); ok {
		const isPartial = false
		var responseBody []byte
		var err error
		if result.err != nil {
			q.AsyncRequestStorage.Delete(id)
			return createEmptyAsyncSearchResponse(id, false, 503)
		}
		if !result.isAggregation {
			responseBody, err = createAsyncSearchResponseHitJson(context.Background(),
				result.rows, result.asyncSearchQueryType,
				result.queryTranslator,
				result.highlighter, id, isPartial)
			q.AsyncRequestStorage.Delete(id)
		} else {
			responseBody, err = result.queryTranslator.MakeResponseAggregation(result.aggregations,
				result.aggregationRows, id, isPartial)
			q.AsyncRequestStorage.Delete(id)
		}
		quesmaManagementConsole.PushSecondaryInfo(&ui.QueryDebugSecondarySource{
			Id:                     result.id,
			IncomingQueryBody:      result.body,
			QueryBodyTranslated:    result.translatedQueryBody,
			QueryRawResults:        []byte{},
			QueryTranslatedResults: responseBody,
			SecondaryTook:          result.took,
		})
		return responseBody, err
	} else {
		const isPartial = true
		if !result.isAggregation {
			responseBody, err := createAsyncSearchResponseHitJson(context.Background(),
				result.rows, result.asyncSearchQueryType,
				result.queryTranslator,
				result.highlighter, id, isPartial)
			return responseBody, err
		} else {
			responseBody, err := result.queryTranslator.MakeResponseAggregation(result.aggregations,
				result.aggregationRows, id, isPartial)
			return responseBody, err
		}
	}
}

func (q *QueryRunner) deleteAsyncSeach(id string) ([]byte, error) {
	if !strings.Contains(id, "quesma_async_search_id_") {
		return nil, errors.New("invalid quesma async search id : " + id)
	}
	q.AsyncRequestStorage.Delete(id)
	return []byte{}, nil
}

func (q *QueryRunner) reachedQueriesLimit(asyncRequestIdStr string, doneCh chan struct{}) bool {
	if q.AsyncRequestStorage.Size() < AsyncQueriesLimit {
		return false
	}
	q.AsyncRequestStorage.Store(asyncRequestIdStr, AsyncRequestResult{err: errors.New("too many async queries"), added: time.Now()})
	logger.Error().Msgf("Cannot handle %s, too many async queries", asyncRequestIdStr)
	doneCh <- struct{}{}
	return true
}

func (q *QueryRunner) addAsyncQueryContext(ctx context.Context, cancel context.CancelFunc, asyncRequestIdStr string) {
	q.AsyncQueriesContexts.Store(asyncRequestIdStr, NewAsyncQueryContext(ctx, cancel, asyncRequestIdStr))
}

func (q *QueryRunner) asyncSearchWorker(ctx context.Context, asyncRequestIdStr string, queryTranslator *queryparser.ClickhouseQueryTranslator,
	table *clickhouse.Table, body []byte, doneCh chan struct{}) {
	select {
	case <-q.executionCtx.Done():
		return
	default:
		if q.reachedQueriesLimit(asyncRequestIdStr, doneCh) {
			return
		}

		var err error
		var fullQuery *model.Query
		var rows []model.QueryResultRow
		var translatedQueryBody []byte
		id := ctx.Value(tracing.RequestIdCtxKey).(string)
		startTime := time.Now()
		simpleQuery, queryInfo, highlighter := queryTranslator.ParseQueryAsyncSearch(string(body))
		dbQueryCtx, dbCancel := context.WithCancel(context.Background())
		q.addAsyncQueryContext(dbQueryCtx, dbCancel, asyncRequestIdStr)

		switch queryInfo.Typ {
		case model.Histogram:
			var bucket time.Duration
			fullQuery, bucket := queryTranslator.BuildHistogramQuery(queryInfo.FieldName, simpleQuery.Sql.Stmt, queryInfo.Interval)
			rows, err = queryTranslator.ClickhouseLM.ProcessHistogramQuery(dbQueryCtx, table, fullQuery, bucket)

		case model.CountAsync:

			fullQuery = queryTranslator.BuildSimpleCountQuery(simpleQuery.Sql.Stmt)
			rows, err = queryTranslator.ClickhouseLM.ProcessSimpleSelectQuery(dbQueryCtx, table, fullQuery)

		case model.AggsByField:
			// queryInfo = (AggsByField, fieldName, Limit results, Limit last rows to look into)
			fmt.Println("AggsByField")

			fullQuery = queryTranslator.BuildFacetsQuery(queryInfo.FieldName, simpleQuery, queryInfo.I2)
			rows, err = queryTranslator.ClickhouseLM.ProcessFacetsQuery(dbQueryCtx, table, fullQuery)

		case model.ListByField:
			// queryInfo = (ListByField, fieldName, 0, LIMIT)
			fullQuery = queryTranslator.BuildNRowsQuery(queryInfo.FieldName, simpleQuery, queryInfo.I2)
			rows, err = queryTranslator.ClickhouseLM.ProcessNRowsQuery(dbQueryCtx, table, fullQuery)

		case model.ListAllFields:
			// queryInfo = (ListAllFields, "*", 0, LIMIT)

			fullQuery = queryTranslator.BuildNRowsQuery("*", simpleQuery, queryInfo.I2)
			rows, err = queryTranslator.ClickhouseLM.ProcessNRowsQuery(dbQueryCtx, table, fullQuery)

		case model.EarliestLatestTimestamp:

			var rowsEarliest, rowsLatest []model.QueryResultRow
			fullQuery = queryTranslator.BuildTimestampQuery(queryInfo.FieldName, simpleQuery.Sql.Stmt, true)
			rowsEarliest, err = queryTranslator.ClickhouseLM.ProcessTimestampQuery(dbQueryCtx, table, fullQuery)
			if err != nil {
				logger.ErrorWithCtx(ctx).Msgf("Rows: %+v, err: %+v", rowsEarliest, err)
			}
			fullQuery = queryTranslator.BuildTimestampQuery(queryInfo.FieldName, simpleQuery.Sql.Stmt, false)
			rowsLatest, err = queryTranslator.ClickhouseLM.ProcessTimestampQuery(dbQueryCtx, table, fullQuery)
			rows = append(rowsEarliest, rowsLatest...)
		default:
			panic(fmt.Sprintf("Unknown query type: %v", queryInfo.Typ))
		}
		if fullQuery != nil {
			translatedQueryBody = []byte(fullQuery.String())
		}
		if err != nil {
			logger.ErrorWithCtx(ctx).Msgf("Rows: %+v, err: %+v", rows, err)
		}
		q.AsyncRequestStorage.Store(asyncRequestIdStr, AsyncRequestResult{isAggregation: false,
			queryTranslator: queryTranslator, highlighter: highlighter, asyncSearchQueryType: queryInfo.Typ,
			rows: rows, translatedQueryBody: translatedQueryBody, body: body, id: id,
			took: time.Since(startTime), err: err})
		doneCh <- struct{}{}
	}
}

func (q *QueryRunner) asyncSearchAggregationWorker(ctx context.Context, asyncRequestIdStr string, aggregations []model.QueryWithAggregation,
	queryTranslator *queryparser.ClickhouseQueryTranslator, table *clickhouse.Table, body []byte,
	doneCh chan struct{}) {
	select {
	case <-q.executionCtx.Done():
		return
	default:
		if q.reachedQueriesLimit(asyncRequestIdStr, doneCh) {
			return
		}
		var results [][]model.QueryResultRow
		sqls := ""
		var translatedQueryBody []byte
		var err error
		id := ctx.Value(tracing.RequestIdCtxKey).(string)
		startTime := time.Now()
		dbQueryCtx, dbCancel := context.WithCancel(context.Background())
		q.addAsyncQueryContext(dbQueryCtx, dbCancel, asyncRequestIdStr)
		logger.InfoWithCtx(ctx).Msg("We're using new Aggregation handling.")
		for _, agg := range aggregations {
			logger.InfoWithCtx(ctx).Msg(agg.String()) // I'd keep for now until aggregations work fully
			rows, err := queryTranslator.ClickhouseLM.ProcessGeneralAggregationQuery(dbQueryCtx, table, &agg.Query)
			if err != nil {
				logger.ErrorWithCtx(ctx).Msg(err.Error())
				continue
			}
			results = append(results, rows)
			sqls += agg.Query.String() + "\n"
		}
		translatedQueryBody = []byte(sqls)
		q.AsyncRequestStorage.Store(asyncRequestIdStr, AsyncRequestResult{isAggregation: true,
			queryTranslator: queryTranslator, aggregations: aggregations, aggregationRows: results,
			translatedQueryBody: translatedQueryBody, body: body, id: id,
			took: time.Since(startTime),
			err:  err})
		doneCh <- struct{}{}
	}
}

func (q *QueryRunner) handleAsyncSearch(ctx context.Context, index string, body []byte, lm *clickhouse.LogManager,
	quesmaManagementConsole *ui.QuesmaManagementConsole, waitForResultsMs int, keepOnCompletion bool) ([]byte, error) {
	id := ctx.Value(tracing.RequestIdCtxKey).(string)
	resolvedTableName := lm.ResolveTableName(index)
	if resolvedTableName == "" {
		logger.WarnWithCtx(ctx).Msgf("could not resolve table name for [%s]", index)
		return nil, errors.New("could not resolve table name")
	}
	table := lm.GetTable(resolvedTableName)

	queryTranslator := &queryparser.ClickhouseQueryTranslator{ClickhouseLM: lm, Table: table, Ctx: ctx}
	simpleQuery, queryInfo, _ := queryTranslator.ParseQueryAsyncSearch(string(body))
	asyncRequestIdStr := generateAsyncRequestId()

	doneCh := make(chan struct{}, 1)

	// Let's try old one only if:
	// 1) it's a ListFields type without "aggs" part. It doesn't have "aggs" part, so we can't handle it with new logic.
	// 2) it's AggsByField request. It's facets - better handled here.
	//    ==== CARE ====
	//    Maybe there are requests with similar structure, so we label them as AggsByField, but they would be better handled with the new logic.
	if simpleQuery.CanParse && (((queryInfo.Typ == model.ListByField || queryInfo.Typ == model.ListAllFields) && !bytes.Contains(body, []byte("aggs"))) || queryInfo.Typ == model.AggsByField) {
		logger.InfoWithCtx(ctx).Msgf("Received _async_search request, type: %v", queryInfo.Typ)
		go q.asyncSearchWorker(ctx, asyncRequestIdStr, queryTranslator, table, body, doneCh)

	} else if aggregations, err := queryTranslator.ParseAggregationJson(string(body)); err == nil {
		go q.asyncSearchAggregationWorker(ctx, asyncRequestIdStr, aggregations, queryTranslator, table, body, doneCh)
	} else {
		responseBody := []byte("Invalid Query, err: " + simpleQuery.Sql.Stmt)
		quesmaManagementConsole.PushSecondaryInfo(&ui.QueryDebugSecondarySource{
			Id:                     id,
			IncomingQueryBody:      body,
			QueryBodyTranslated:    []byte{},
			QueryRawResults:        []byte{},
			QueryTranslatedResults: responseBody,
		})
		return responseBody, errors.New(string(responseBody))
	}

	if waitForResultsMs == 0 {
		return createEmptyAsyncSearchResponse(asyncRequestIdStr, true, 200)
	}
	select {
	case <-time.After(time.Duration(waitForResultsMs) * time.Millisecond):
		return q.handlePartialAsyncSearch(asyncRequestIdStr, quesmaManagementConsole)
	case <-doneCh:
		res, err := q.handlePartialAsyncSearch(asyncRequestIdStr, quesmaManagementConsole)
		if !keepOnCompletion {
			q.AsyncRequestStorage.Delete(asyncRequestIdStr)
		}
		return res, err
	}
}

func (q *QueryRunner) Close() {
	q.cancel()
	logger.Info().Msg("QueryRunner Stopped")
}
