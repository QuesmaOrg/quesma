package quesma

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"mitmproxy/quesma/clickhouse"
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/model"
	"mitmproxy/quesma/queryparser"
	"mitmproxy/quesma/quesma/ui"
	"mitmproxy/quesma/tracing"
	"time"
)

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

func createAsyncSearchResponseHitJson(ctx context.Context, rows []model.QueryResultRow, typ model.AsyncSearchQueryType) ([]byte, error) {
	responseBody, err := queryparser.MakeResponseAsyncSearchQuery(rows, typ)
	if err != nil {
		logger.ErrorWithCtx(ctx).Msgf("%v rows: %v", err, rows)
		return nil, err
	}
	return responseBody, nil
}

func handleAsyncSearch(ctx context.Context, index string, body []byte, lm *clickhouse.LogManager,
	quesmaManagementConsole *ui.QuesmaManagementConsole) ([]byte, error) {

	id := ctx.Value(tracing.RequestIdCtxKey).(string)
	resolvedTableName := lm.ResolveTableName(index)
	if resolvedTableName == "" {
		logger.Warn().Str(logger.RID, id).Msgf("could not resolve table name for [%s]", index)
		return nil, errors.New("could not resolve table name")
	}
	table := lm.GetTable(resolvedTableName)

	queryTranslator := &queryparser.ClickhouseQueryTranslator{ClickhouseLM: lm, Table: table}
	var rawResults []byte
	simpleQuery, queryInfo := queryTranslator.ParseQueryAsyncSearch(string(body))
	var responseBody, translatedQueryBody []byte

	// Let's try old one only if it's a ListFields type without "aggs" part.
	// It doesn't have "aggs" part, so we can't handle it with new logic.
	if simpleQuery.CanParse && (queryInfo.Typ == model.ListByField || queryInfo.Typ == model.ListAllFields) && !bytes.Contains(body, []byte("aggs")) {
		logger.Info().Str(logger.RID, id).Ctx(ctx).Msgf("Received _async_search request, type: %v", queryInfo.Typ)
		var fullQuery *model.Query
		var err error
		var rows []model.QueryResultRow
		switch queryInfo.Typ {
		case model.Histogram:
			var bucket time.Duration
			fullQuery, bucket = queryTranslator.BuildHistogramQuery(queryInfo.FieldName, simpleQuery.Sql.Stmt, queryInfo.Interval)
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
			fullQuery = queryTranslator.BuildNMostRecentRowsQuery(queryInfo.FieldName, simpleQuery.FieldName, simpleQuery.Sql.Stmt, queryInfo.I2)
			rows, err = queryTranslator.ClickhouseLM.ProcessNMostRecentRowsQuery(table, fullQuery)
		case model.ListAllFields:
			// queryInfo = (ListAllFields, "*", 0, LIMIT)
			fullQuery = queryTranslator.BuildNMostRecentRowsQuery("*", simpleQuery.FieldName, simpleQuery.Sql.Stmt, queryInfo.I2)
			rows, err = queryTranslator.ClickhouseLM.ProcessNMostRecentRowsQuery(table, fullQuery)
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
		}
		if err != nil {
			logger.ErrorWithCtx(ctx).Msgf("Rows: %+v, err: %+v", rows, err)
		}
		responseBody, err = createAsyncSearchResponseHitJson(ctx, rows, queryInfo.Typ)
		if err != nil {
			return responseBody, err
		}
		if fullQuery != nil {
			translatedQueryBody = []byte(fullQuery.String())
		} else {
			logger.ErrorWithCtx(ctx).Msgf("fullQuery is nil")
			return responseBody, errors.New("fullQuery is nil")
		}
	} else if aggregations, err := queryTranslator.ParseAggregationJson(string(body)); err == nil {
		logger.Info().Str(logger.RID, id).Ctx(ctx).Msg("We're using new Aggregation handling.")
		for _, agg := range aggregations {
			logger.Info().Msg(agg.String()) // I'd keep for now until aggregations work fully
		}
		var results [][]model.QueryResultRow
		sqls := ""
		for _, agg := range aggregations {
			// logger.Info().Msg("Processing query.")
			rows, err := queryTranslator.ClickhouseLM.ProcessGeneralAggregationQuery(table, &agg.Query)
			if err != nil {
				logger.ErrorWithCtx(ctx).Msg(err.Error())
				continue
			}
			// logger.Info().Msgf("Error: %v, first 2 rows:", err)
			// howMany := 2 // this variable and generally a lot in this code: just debug to be removed
			// if len(rows) < howMany {
			// 	howMany = len(rows)
			// }
			// for _, row := range rows[:howMany] {
			// logger.Info().Msgf("Row: %+v", row)
			// }
			// logger.Error().Msgf("len: %v", len(rows))
			results = append(results, rows)
			sqls += agg.Query.String() + "\n"
		}
		translatedQueryBody = []byte(sqls)
		responseBody, _ = queryTranslator.MakeResponseAggregation(aggregations, results)
		// fmt.Println("HOHOH\n", err, string(responseBody))
	} else {
		responseBody = []byte("Invalid Query, err: " + simpleQuery.Sql.Stmt)
		quesmaManagementConsole.PushSecondaryInfo(&ui.QueryDebugSecondarySource{
			Id:                     id,
			IncomingQueryBody:      body,
			QueryBodyTranslated:    translatedQueryBody,
			QueryRawResults:        rawResults,
			QueryTranslatedResults: responseBody,
		})
		return responseBody, errors.New(string(responseBody))
	}

	quesmaManagementConsole.PushSecondaryInfo(&ui.QueryDebugSecondarySource{
		Id:                     id,
		IncomingQueryBody:      body,
		QueryBodyTranslated:    translatedQueryBody,
		QueryRawResults:        rawResults,
		QueryTranslatedResults: responseBody,
	})
	return responseBody, nil
}
