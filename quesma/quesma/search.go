package quesma

import (
	"context"
	"errors"
	"log"
	"mitmproxy/quesma/clickhouse"
	"mitmproxy/quesma/model"
	"mitmproxy/quesma/queryparser"
)

func handleSearch(ctx context.Context, index string, body []byte, lm *clickhouse.LogManager,
	quesmaManagementConsole *QuesmaManagementConsole) ([]byte, error) {
	queryTranslator := &queryparser.ClickhouseQueryTranslator{ClickhouseLM: lm}
	// TODO index argument is not used yet
	_ = index

	simpleQuery, queryInfo := queryTranslator.Write(body)
	var responseBody, translatedQueryBody []byte
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
			log.Println("Error processing query: " + simpleQuery.Sql.Stmt + ", err: " + err.Error())
			return responseBody, err
		}
		responseBody, err = queryparser.MakeResponseSearchQuery(rows, queryInfo)
		if err != nil {
			log.Println(err, "rows: ", rows)
			return responseBody, err
		}
	} else {
		responseBody = []byte("Invalid Query, err: " + simpleQuery.Sql.Stmt)
		return responseBody, errors.New(string(responseBody))
	}

	var rawResults []byte
	quesmaManagementConsole.PushSecondaryInfo(&QueryDebugSecondarySource{
		id:                     ctx.Value(RequestId{}).(string),
		incomingQueryBody:      body,
		queryBodyTranslated:    translatedQueryBody,
		queryRawResults:        rawResults,
		queryTranslatedResults: responseBody,
	})
	return responseBody, nil
}

func createAsyncSearchResponseHitJson(rows []clickhouse.QueryResultRow, typ model.AsyncSearchQueryType) []byte {
	responseBody, err := queryparser.MakeResponseAsyncSearchQuery(rows, typ)
	if err != nil {
		log.Println(err, "rows:", rows)
	}
	return responseBody
}

func handleAsyncSearch(ctx context.Context, index string, body []byte, lm *clickhouse.LogManager,
	quesmaManagementConsole *QuesmaManagementConsole) ([]byte, error) {
	queryTranslator := &queryparser.ClickhouseQueryTranslator{ClickhouseLM: lm}
	// TODO index argument is not used yet
	_ = index

	simpleQuery, queryInfo := queryTranslator.WriteAsyncSearch(body)
	var responseBody, translatedQueryBody []byte

	if simpleQuery.CanParse && queryInfo.Typ != model.None {
		var fullQuery *model.Query
		switch queryInfo.Typ {
		case model.Histogram:
			// queryInfo = (Histogram, "30s", 0 0) TODO accept different time intervals (now default, 15min)
			fullQuery = queryTranslator.BuildHistogramQuery(queryparser.TableName, "@timestamp", simpleQuery.Sql.Stmt) // TODO change timestamp
			histogram, err := queryTranslator.ClickhouseLM.ProcessHistogramQuery(fullQuery)
			log.Printf("Histogram: %+v, err: %+v\n", histogram, err)
			responseBody = createAsyncSearchResponseHitJson(histogram, model.Histogram)
		case model.AggsByField:
			// queryInfo = (AggsByField, fieldName, Limit results, Limit last rows to look into)
			fullQuery = queryTranslator.BuildFacetsQuery(queryparser.TableName, queryInfo.FieldName, simpleQuery.Sql.Stmt, queryInfo.I2)
			rows, err := queryTranslator.ClickhouseLM.ProcessFacetsQuery(fullQuery)
			log.Printf("Rows: %+v, err: %+v\n", rows, err)
			responseBody = createAsyncSearchResponseHitJson(rows, model.AggsByField)
		case model.ListByField:
			// queryInfo = (ListByField, fieldName, 0, LIMIT)
			fullQuery = queryTranslator.BuildNMostRecentRowsQuery(queryparser.TableName, queryInfo.FieldName,
				"@timestamp", simpleQuery.Sql.Stmt, queryInfo.I2)
			rows, err := queryTranslator.ClickhouseLM.ProcessNMostRecentRowsQuery(fullQuery)
			log.Printf("Rows: %+v, err: %+v\n", rows, err)
			responseBody = createAsyncSearchResponseHitJson(rows, model.ListByField)
		case model.ListAllFields:
			// queryInfo = (ListAllFields, "*", 0, LIMIT)
			fullQuery = queryTranslator.BuildNMostRecentRowsQuery(queryparser.TableName, "*",
				"@timestamp", simpleQuery.Sql.Stmt, queryInfo.I2)
			rows, err := queryTranslator.ClickhouseLM.ProcessNMostRecentRowsQuery(fullQuery)
			log.Printf("Rows: %+v, err: %+v\n", rows, err)
			responseBody = createAsyncSearchResponseHitJson(rows, model.ListAllFields)
		case model.EarliestLatestTimestamp:
			fullQuery = queryTranslator.BuildTimestampQuery(queryparser.TableName, queryInfo.FieldName, simpleQuery.Sql.Stmt, true)
			rowsEarliest, err := queryTranslator.ClickhouseLM.ProcessTimestampQuery(fullQuery)
			if err != nil {
				log.Println("------------------ CARE Error processing query: " + simpleQuery.Sql.Stmt + ", err: " + err.Error())
			}
			fullQuery = queryTranslator.BuildTimestampQuery(queryparser.TableName, queryInfo.FieldName, simpleQuery.Sql.Stmt, false)
			rowsLatest, err := queryTranslator.ClickhouseLM.ProcessTimestampQuery(fullQuery)
			if err != nil {
				log.Println("------------------ CARE Error processing query: " + simpleQuery.Sql.Stmt + ", err: " + err.Error())
			}
			responseBody = createAsyncSearchResponseHitJson(append(rowsEarliest, rowsLatest...), model.EarliestLatestTimestamp)
		case model.None:
			log.Println("------------------------------ CARE! NOT IMPLEMENTED /_async/search REQUEST")
			responseBody = []byte("Invalid Query, err: " + simpleQuery.Sql.Stmt)
			return responseBody, errors.New(string(responseBody))
		}
		translatedQueryBody = []byte(fullQuery.String())
	} else {
		responseBody = []byte("Invalid Query, err: " + simpleQuery.Sql.Stmt)
		return responseBody, errors.New(string(responseBody))
	}

	var rawResults []byte
	quesmaManagementConsole.PushSecondaryInfo(&QueryDebugSecondarySource{
		id:                     ctx.Value(RequestId{}).(string),
		incomingQueryBody:      body,
		queryBodyTranslated:    translatedQueryBody,
		queryRawResults:        rawResults,
		queryTranslatedResults: responseBody,
	})
	return responseBody, nil
}
