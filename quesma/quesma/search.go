package quesma

import (
	"context"
	"errors"
	"mitmproxy/quesma/clickhouse"
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/model"
	"mitmproxy/quesma/queryparser"
	"mitmproxy/quesma/quesma/ui"
	"mitmproxy/quesma/tracing"
)

func handleSearch(ctx context.Context, index string, body []byte, lm *clickhouse.LogManager,
	quesmaManagementConsole *ui.QuesmaManagementConsole) ([]byte, error) {
	queryTranslator := &queryparser.ClickhouseQueryTranslator{ClickhouseLM: lm}
	// TODO index argument is not used yet
	_ = index
	var rawResults []byte
	simpleQuery, queryInfo := queryTranslator.ParseQuery(string(body))
	var responseBody, translatedQueryBody []byte
	id := ctx.Value(tracing.RequestIdCtxKey).(string)
	if simpleQuery.CanParse {
		var fullQuery *model.Query
		switch queryInfo {
		case model.Count:
			fullQuery = queryTranslator.BuildSimpleCountQuery(queryparser.TableName, simpleQuery.Sql.Stmt)
		case model.Normal:
			fullQuery = queryTranslator.BuildSimpleSelectQuery(queryparser.TableName, simpleQuery.Sql.Stmt)
		}
		translatedQueryBody = []byte(fullQuery.String())
		rows, err := queryTranslator.ClickhouseLM.ProcessSimpleSelectQuery(fullQuery)
		if err != nil {
			logger.Error().Str(logger.RID, id).Msgf("Error processing query: %s, err: %s", simpleQuery.Sql.Stmt, err.Error())
			responseBody = []byte("Error processing query: " + simpleQuery.Sql.Stmt + ", err: " + err.Error())
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
			logger.Error().Str(logger.RID, id).Msgf(err.Error(), "rows: ", rows)
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

func createAsyncSearchResponseHitJson(requestId string, rows []clickhouse.QueryResultRow, typ model.AsyncSearchQueryType) []byte {
	responseBody, err := queryparser.MakeResponseAsyncSearchQuery(rows, typ)
	if err != nil {
		logger.Error().Str(logger.RID, requestId).Msgf("%v rows: %v", err, rows)
	}
	return responseBody
}

func handleAsyncSearch(ctx context.Context, index string, body []byte, lm *clickhouse.LogManager,
	quesmaManagementConsole *ui.QuesmaManagementConsole) ([]byte, error) {
	queryTranslator := &queryparser.ClickhouseQueryTranslator{ClickhouseLM: lm}
	// TODO index argument is not used yet
	_ = index
	var rawResults []byte
	simpleQuery, queryInfo := queryTranslator.ParseQueryAsyncSearch(string(body))
	var responseBody, translatedQueryBody []byte

	id := ctx.Value(tracing.RequestIdCtxKey).(string)
	if simpleQuery.CanParse && queryInfo.Typ != model.None {
		var fullQuery *model.Query
		var err error
		var rows []clickhouse.QueryResultRow
		switch queryInfo.Typ {
		case model.Histogram:
			// queryInfo = (Histogram, "30s", 0 0) TODO accept different time intervals (now default, 15min)
			fullQuery = queryTranslator.BuildHistogramQuery(queryparser.TableName, "@timestamp", simpleQuery.Sql.Stmt) // TODO change timestamp
			rows, err = queryTranslator.ClickhouseLM.ProcessHistogramQuery(fullQuery)
		case model.AggsByField:
			// queryInfo = (AggsByField, fieldName, Limit results, Limit last rows to look into)
			fullQuery = queryTranslator.BuildFacetsQuery(queryparser.TableName, queryInfo.FieldName, simpleQuery.Sql.Stmt, queryInfo.I2)
			rows, err = queryTranslator.ClickhouseLM.ProcessFacetsQuery(fullQuery)
		case model.ListByField:
			// queryInfo = (ListByField, fieldName, 0, LIMIT)
			fullQuery = queryTranslator.BuildNMostRecentRowsQuery(queryparser.TableName, queryInfo.FieldName,
				"@timestamp", simpleQuery.Sql.Stmt, queryInfo.I2)
			rows, err = queryTranslator.ClickhouseLM.ProcessNMostRecentRowsQuery(fullQuery)
		case model.ListAllFields:
			// queryInfo = (ListAllFields, "*", 0, LIMIT)
			fullQuery = queryTranslator.BuildNMostRecentRowsQuery(queryparser.TableName, "*",
				"@timestamp", simpleQuery.Sql.Stmt, queryInfo.I2)
			rows, err = queryTranslator.ClickhouseLM.ProcessNMostRecentRowsQuery(fullQuery)
		case model.EarliestLatestTimestamp:
			var rowsEarliest, rowsLatest []clickhouse.QueryResultRow
			fullQuery = queryTranslator.BuildTimestampQuery(queryparser.TableName, queryInfo.FieldName, simpleQuery.Sql.Stmt, true)
			rowsEarliest, err = queryTranslator.ClickhouseLM.ProcessTimestampQuery(fullQuery)
			if err != nil {
				logger.Error().Str(logger.RID, id).Msgf("Rows: %+v, err: %+v\n", rowsEarliest, err)
			}
			fullQuery = queryTranslator.BuildTimestampQuery(queryparser.TableName, queryInfo.FieldName, simpleQuery.Sql.Stmt, false)
			rowsLatest, err = queryTranslator.ClickhouseLM.ProcessTimestampQuery(fullQuery)
			rows = append(rowsEarliest, rowsLatest...)
		}
		if err != nil {
			logger.Error().Str(logger.RID, id).Msgf("Rows: %+v, err: %+v\n", rows, err)
		}
		responseBody = createAsyncSearchResponseHitJson(id, rows, queryInfo.Typ)
		translatedQueryBody = []byte(fullQuery.String())
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
