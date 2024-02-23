package quesma

import (
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
	resolvedTableName := lm.ResolveTableName(indexPattern)
	queryTranslator := &queryparser.ClickhouseQueryTranslator{ClickhouseLM: lm, TableName: resolvedTableName}

	var rawResults []byte
	simpleQuery, queryInfo := queryTranslator.ParseQuery(string(body))
	var responseBody, translatedQueryBody []byte
	id := ctx.Value(tracing.RequestIdCtxKey).(string)
	if simpleQuery.CanParse {
		var fullQuery *model.Query
		switch queryInfo {
		case model.Count:
			fullQuery = queryTranslator.BuildSimpleCountQuery(resolvedTableName, simpleQuery.Sql.Stmt)
		case model.Normal:
			fullQuery = queryTranslator.BuildSimpleSelectQuery(resolvedTableName, simpleQuery.Sql.Stmt)
		}
		translatedQueryBody = []byte(fullQuery.String())
		rows, err := queryTranslator.ClickhouseLM.ProcessSimpleSelectQuery(fullQuery)
		if err != nil {
			errorMsg := fmt.Sprintf("Error processing query: %s, err: %s", fullQuery.String(), err.Error())
			logger.ErrorWithCtx(ctx).Msg(errorMsg)
			responseBody = []byte(errorMsg)
			quesmaManagementConsole.PushSecondaryInfo(&ui.QueryDebugSecondarySource{
				Id:                     id,
				IncomingQueryBody:      body,
				QueryBodyTranslated:    translatedQueryBody,
				QueryRawResults:        rawResults,
				QueryTranslatedResults: responseBody,
			})
			return responseBody, err
		}
		responseBody, err = queryparser.MakeResponseSearchQuery(rows, queryInfo)
		if err != nil {
			logger.ErrorWithCtx(ctx).Msgf("Error making response: %v rows: %v", err, rows)
			quesmaManagementConsole.PushSecondaryInfo(&ui.QueryDebugSecondarySource{
				Id:                     id,
				IncomingQueryBody:      body,
				QueryBodyTranslated:    translatedQueryBody,
				QueryRawResults:        rawResults,
				QueryTranslatedResults: responseBody,
			})
			return responseBody, err
		}
	} else {
		responseBody = []byte("Invalid Query, err: " + simpleQuery.Sql.Stmt)
		logger.ErrorWithCtxAndReason(ctx, "Quesma generated invalid SQL query").Msg(string(responseBody))
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

	queryTranslator := &queryparser.ClickhouseQueryTranslator{ClickhouseLM: lm, TableName: lm.ResolveTableName(index)}
	var rawResults []byte
	simpleQuery, queryInfo := queryTranslator.ParseQueryAsyncSearch(string(body))
	var responseBody, translatedQueryBody []byte

	id := ctx.Value(tracing.RequestIdCtxKey).(string)
	if simpleQuery.CanParse && queryInfo.Typ != model.None {
		var fullQuery *model.Query
		var err error
		var rows []model.QueryResultRow
		switch queryInfo.Typ {
		case model.Histogram:
			var bucket time.Duration
			fullQuery, bucket = queryTranslator.BuildHistogramQuery(simpleQuery.FieldName, simpleQuery.Sql.Stmt, queryInfo.FieldName)
			rows, err = queryTranslator.ClickhouseLM.ProcessHistogramQuery(fullQuery, bucket)
		case model.AggsByField:
			// queryInfo = (AggsByField, fieldName, Limit results, Limit last rows to look into)
			fullQuery = queryTranslator.BuildFacetsQuery(queryInfo.FieldName, simpleQuery.Sql.Stmt, queryInfo.I2)
			rows, err = queryTranslator.ClickhouseLM.ProcessFacetsQuery(fullQuery)
		case model.ListByField:
			// queryInfo = (ListByField, fieldName, 0, LIMIT)
			fullQuery = queryTranslator.BuildNMostRecentRowsQuery(queryInfo.FieldName, simpleQuery.FieldName, simpleQuery.Sql.Stmt, queryInfo.I2)
			rows, err = queryTranslator.ClickhouseLM.ProcessNMostRecentRowsQuery(fullQuery)
		case model.ListAllFields:
			// queryInfo = (ListAllFields, "*", 0, LIMIT)
			fullQuery = queryTranslator.BuildNMostRecentRowsQuery("*", simpleQuery.FieldName, simpleQuery.Sql.Stmt, queryInfo.I2)
			rows, err = queryTranslator.ClickhouseLM.ProcessNMostRecentRowsQuery(fullQuery)
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
