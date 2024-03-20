package quesma

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"mitmproxy/quesma/clickhouse"
	"mitmproxy/quesma/concurrent"
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/model"
	"mitmproxy/quesma/queryparser"
	"mitmproxy/quesma/quesma/ui"
	"mitmproxy/quesma/tracing"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

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
}

var AsyncRequestStorage *concurrent.Map[string, AsyncRequestResult]

func handleCount(ctx context.Context, indexPattern string, lm *clickhouse.LogManager) (int64, error) {
	id := ctx.Value(tracing.RequestIdCtxKey).(string)
	indexes := lm.ResolveIndexes(indexPattern)
	if len(indexes) == 0 {
		logger.Warn().Str(logger.RID, id).Msgf("could not resolve table name for [%s]", indexPattern)
		return -1, errors.New("could not resolve table name")
	}

	if len(indexes) == 1 {
		return lm.Count(indexes[0])
	} else {
		return lm.CountMultiple(indexes...)
	}
}

func handleSearch(ctx context.Context, indexPattern string, body []byte, lm *clickhouse.LogManager,
	quesmaManagementConsole *ui.QuesmaManagementConsole) ([]byte, error) {

	id := ctx.Value(tracing.RequestIdCtxKey).(string)
	resolvedTableName := lm.ResolveTableName(indexPattern)
	if resolvedTableName == "" {
		logger.Warn().Str(logger.RID, id).Msgf("could not resolve table name for [%s]", indexPattern)
		return nil, errors.New("could not resolve table name")
	}
	table := lm.GetTable(resolvedTableName)

	var rawResults, responseBody, translatedQueryBody []byte
	pushSecondaryInfoToManagementConsole := func() {
		quesmaManagementConsole.PushSecondaryInfo(&ui.QueryDebugSecondarySource{
			Id:                     id,
			IncomingQueryBody:      body,
			QueryBodyTranslated:    translatedQueryBody,
			QueryRawResults:        rawResults,
			QueryTranslatedResults: responseBody,
		})
	}

	queryTranslator := &queryparser.ClickhouseQueryTranslator{ClickhouseLM: lm, Table: table}
	simpleQuery, queryInfo := queryTranslator.ParseQuery(string(body))
	if simpleQuery.CanParse {
		var fullQuery *model.Query
		switch queryInfo {
		case model.Count:
			fullQuery = queryTranslator.BuildSimpleCountQuery(simpleQuery.Sql.Stmt)
		case model.Normal:
			fullQuery = queryTranslator.BuildSimpleSelectQuery(simpleQuery.Sql.Stmt)
		}
		translatedQueryBody = []byte(fullQuery.String())
		rows, err := queryTranslator.ClickhouseLM.ProcessSimpleSelectQuery(table, fullQuery)
		if err != nil {
			errorMsg := fmt.Sprintf("Error processing query: %s, err: %s", fullQuery.String(), err.Error())
			logger.ErrorWithCtx(ctx).Msg(errorMsg)
			responseBody = []byte(errorMsg)
			pushSecondaryInfoToManagementConsole()
			return responseBody, err
		}
		responseBody, err = queryparser.MakeResponseSearchQuery(rows, queryInfo)
		if err != nil {
			logger.ErrorWithCtx(ctx).Msgf("Error making response: %v rows: %v", err, rows)
			pushSecondaryInfoToManagementConsole()
			return responseBody, err
		}
	} else {
		responseBody = []byte("Invalid Query, err: " + simpleQuery.Sql.Stmt)
		logger.ErrorWithCtxAndReason(ctx, "Quesma generated invalid SQL query").Msg(string(responseBody))
		pushSecondaryInfoToManagementConsole()
		return responseBody, errors.New(string(responseBody))
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

func handlePartialAsyncSearch(id string, quesmaManagementConsole *ui.QuesmaManagementConsole) ([]byte, error) {
	if !strings.Contains(id, "quesma_async_search_id_") {
		return createEmptyAsyncSearchResponse(id, false, 503)
	}
	if result, ok := AsyncRequestStorage.Load(id); ok {
		const isPartial = false
		var responseBody []byte
		var err error
		if !result.isAggregation {
			responseBody, err = createAsyncSearchResponseHitJson(context.Background(),
				result.rows, result.asyncSearchQueryType,
				result.queryTranslator,
				result.highlighter, id, isPartial)
			AsyncRequestStorage.Delete(id)
		} else {
			responseBody, err = result.queryTranslator.MakeResponseAggregation(result.aggregations,
				result.aggregationRows, id, isPartial)
			AsyncRequestStorage.Delete(id)
		}
		quesmaManagementConsole.PushSecondaryInfo(&ui.QueryDebugSecondarySource{
			Id:                     result.id,
			IncomingQueryBody:      result.body,
			QueryBodyTranslated:    result.translatedQueryBody,
			QueryRawResults:        []byte{},
			QueryTranslatedResults: responseBody,
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

func handleAsyncSearch(ctx context.Context, index string, body []byte, lm *clickhouse.LogManager,
	quesmaManagementConsole *ui.QuesmaManagementConsole, wg *sync.WaitGroup) ([]byte, error) {
	id := ctx.Value(tracing.RequestIdCtxKey).(string)
	resolvedTableName := lm.ResolveTableName(index)
	if resolvedTableName == "" {
		logger.Warn().Str(logger.RID, id).Msgf("could not resolve table name for [%s]", index)
		return nil, errors.New("could not resolve table name")
	}
	table := lm.GetTable(resolvedTableName)

	queryTranslator := &queryparser.ClickhouseQueryTranslator{ClickhouseLM: lm, Table: table}
	simpleQuery, queryInfo, highlighter := queryTranslator.ParseQueryAsyncSearch(string(body))
	asyncRequestIdStr := generateAsyncRequestId()
	// Let's try old one only if:
	// 1) it's a ListFields type without "aggs" part. It doesn't have "aggs" part, so we can't handle it with new logic.
	// 2) it's AggsByField request. It's facets - better handled here.
	//    ==== CARE ====
	//    Maybe there are requests with similar structure, so we label them as AggsByField, but they would be better handled with the new logic.
	if simpleQuery.CanParse && (((queryInfo.Typ == model.ListByField || queryInfo.Typ == model.ListAllFields) && !bytes.Contains(body, []byte("aggs"))) || queryInfo.Typ == model.AggsByField) {
		logger.Info().Str(logger.RID, id).Ctx(ctx).Msgf("Received _async_search request, type: %v", queryInfo.Typ)
		go func() {
			var err error
			var fullQuery *model.Query
			var rows []model.QueryResultRow
			var translatedQueryBody []byte
			switch queryInfo.Typ {
			case model.Histogram:
				var bucket time.Duration
				fullQuery, bucket := queryTranslator.BuildHistogramQuery(queryInfo.FieldName, simpleQuery.Sql.Stmt, queryInfo.Interval)
				rows, err = queryTranslator.ClickhouseLM.ProcessHistogramQuery(fullQuery, bucket)

			case model.CountAsync:

				fullQuery = queryTranslator.BuildSimpleCountQuery(simpleQuery.Sql.Stmt)
				rows, err = queryTranslator.ClickhouseLM.ProcessSimpleSelectQuery(table, fullQuery)

			case model.AggsByField:
				// queryInfo = (AggsByField, fieldName, Limit results, Limit last rows to look into)
				fmt.Println("AggsByField")

				fullQuery = queryTranslator.BuildFacetsQuery(queryInfo.FieldName, simpleQuery.Sql.Stmt, queryInfo.I2)
				rows, err = queryTranslator.ClickhouseLM.ProcessFacetsQuery(table, fullQuery)

			case model.ListByField:
				// queryInfo = (ListByField, fieldName, 0, LIMIT)
				fullQuery = queryTranslator.BuildNRowsQuery(queryInfo.FieldName, simpleQuery, queryInfo.I2)
				rows, err = queryTranslator.ClickhouseLM.ProcessNRowsQuery(table, fullQuery)

			case model.ListAllFields:
				// queryInfo = (ListAllFields, "*", 0, LIMIT)

				fullQuery = queryTranslator.BuildNRowsQuery("*", simpleQuery, queryInfo.I2)
				rows, err = queryTranslator.ClickhouseLM.ProcessNRowsQuery(table, fullQuery)

			case model.EarliestLatestTimestamp:

				var rowsEarliest, rowsLatest []model.QueryResultRow
				fullQuery = queryTranslator.BuildTimestampQuery(queryInfo.FieldName, simpleQuery.Sql.Stmt, true)
				rowsEarliest, err = queryTranslator.ClickhouseLM.ProcessTimestampQuery(fullQuery)
				if err != nil {
					logger.ErrorWithCtx(ctx).Msgf("Rows: %+v, err: %+v", rowsEarliest, err)
				}
				fullQuery = queryTranslator.BuildTimestampQuery(queryInfo.FieldName, simpleQuery.Sql.Stmt, false)
				rowsLatest, err = queryTranslator.ClickhouseLM.ProcessTimestampQuery(fullQuery)
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
			AsyncRequestStorage.Store(asyncRequestIdStr, AsyncRequestResult{isAggregation: false,
				queryTranslator: queryTranslator, highlighter: highlighter, asyncSearchQueryType: queryInfo.Typ,
				rows: rows, translatedQueryBody: translatedQueryBody, body: body, id: id, err: err})
			wg.Done()
		}()

		return createEmptyAsyncSearchResponse(asyncRequestIdStr, true, 200)

	} else if aggregations, err := queryTranslator.ParseAggregationJson(string(body)); err == nil {
		go func() {
			var results [][]model.QueryResultRow
			sqls := ""
			var translatedQueryBody []byte
			logger.Info().Str(logger.RID, id).Ctx(ctx).Msg("We're using new Aggregation handling.")
			for _, agg := range aggregations {
				logger.Info().Msg(agg.String()) // I'd keep for now until aggregations work fully
				rows, err := queryTranslator.ClickhouseLM.ProcessGeneralAggregationQuery(table, &agg.Query)
				if err != nil {
					logger.ErrorWithCtx(ctx).Msg(err.Error())
					continue
				}
				results = append(results, rows)
				sqls += agg.Query.String() + "\n"
			}
			translatedQueryBody = []byte(sqls)
			AsyncRequestStorage.Store(asyncRequestIdStr, AsyncRequestResult{isAggregation: true,
				queryTranslator: queryTranslator, aggregations: aggregations, aggregationRows: results,
				translatedQueryBody: translatedQueryBody, body: body, id: id,
				err: err})
			wg.Done()
		}()
		return createEmptyAsyncSearchResponse(asyncRequestIdStr, true, 200)
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
}
