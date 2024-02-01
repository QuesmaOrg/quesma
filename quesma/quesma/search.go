package quesma

import (
	"errors"
	"fmt"
	"log"
	"mitmproxy/quesma/clickhouse"
	"mitmproxy/quesma/model"
	"mitmproxy/quesma/queryparser"
)

func handleSearch(index string, body []byte, lm *clickhouse.LogManager,
	queryDebugger *QueryDebugger,
	requestId string) ([]byte, error) {
	var translatedQueryBody []byte
	queryTranslator := &queryparser.ClickhouseQueryTranslator{ClickhouseLM: lm}

	// TODO index argument is not used yet
	_ = index
	query := queryTranslator.Write(body)
	var responseBody []byte
	if query.CanParse {
		rows, err := queryTranslator.QueryClickhouse(query)
		if err != nil {
			log.Println("Error processing query: " + query.Sql + ", err: " + err.Error())
			return responseBody, err
		}
		responseBody, err = queryparser.MakeResponse(rows)
		if err != nil {
			log.Println(err)
		}
	} else {
		responseBody = []byte("Invalid Query, err: " + query.Sql)
		return responseBody, errors.New(string(responseBody))
	}

	var rawResults []byte
	translatedQueryBody = []byte(query.Sql)
	queryDebugger.PushSecondaryInfo(&QueryDebugSecondarySource{
		id:                     requestId,
		incomingQueryBody:      body,
		queryBodyTranslated:    translatedQueryBody,
		queryRawResults:        rawResults,
		queryTranslatedResults: responseBody,
	})
	return responseBody, nil
}

func createResponseHitJson(rows []clickhouse.QueryResultRow) []byte {
	responseBody, err := queryparser.MakeResponse(rows)
	if err != nil {
		log.Println(err)
	}
	return responseBody
}

func createResponseHistogramJson(rows []clickhouse.HistogramResult) []byte {
	responseBody, err := queryparser.MakeResponse(rows)
	if err != nil {
		log.Println(err)
	}
	return responseBody
}

func handleAsyncSearch(index string, body []byte, lm *clickhouse.LogManager,
	queryDebugger *QueryDebugger,
	requestId string) ([]byte, error) {
	var translatedQueryBody []byte
	queryTranslator := &queryparser.ClickhouseQueryTranslator{ClickhouseLM: lm}

	// TODO index argument is not used yet
	_ = index

	query, queryInfo := queryTranslator.WriteAsyncSearch(body)
	var responseBody []byte

	if query.CanParse && queryInfo.Typ != model.None {
		// TODO cast results from switch below to responseBody
		switch queryInfo.Typ {
		case model.Histogram:
			// queryInfo = (Histogram, "30s", 0 0) TODO accept different time intervals (now default, 15min)
			histogram, err := queryTranslator.GetHistogram(query.TableName)
			fmt.Printf("Histogram: %+v, err: %+v\n", histogram, err)
			responseBody = createResponseHistogramJson(histogram)
		case model.AggsByField:
			// queryInfo = (AggsByField, fieldName, Limit results, Limit last rows to look into)
			rows, err := queryTranslator.GetFacets(query.TableName, queryInfo.FieldName, query.Sql, queryInfo.I2)
			fmt.Printf("Rows: %+v, err: %+v\n", rows, err)
			responseBody = createResponseHitJson(rows)
		case model.ListByField:
			// queryInfo = (ListByField, fieldName, 0, LIMIT)
			rows, err := queryTranslator.GetNMostRecentRows(query.TableName, queryInfo.FieldName, query.Sql, queryInfo.I2)
			fmt.Printf("Rows: %+v, err: %+v\n", rows, err)
			responseBody = createResponseHitJson(rows)
		case model.ListAllFields:
			// queryInfo = (ListAllFields, "*", 0, LIMIT)
			rows, err := queryTranslator.GetNMostRecentRows(query.TableName, "*", query.Sql, queryInfo.I2)
			fmt.Printf("Rows: %+v, err: %+v\n", rows, err)
			responseBody = createResponseHitJson(rows)
		}
	} else {
		responseBody = []byte("Invalid Query, err: " + query.Sql)
		return responseBody, errors.New(string(responseBody))
	}

	var rawResults []byte
	translatedQueryBody = []byte(query.Sql)
	queryDebugger.PushSecondaryInfo(&QueryDebugSecondarySource{
		id:                     requestId,
		incomingQueryBody:      body,
		queryBodyTranslated:    translatedQueryBody,
		queryRawResults:        rawResults,
		queryTranslatedResults: responseBody,
	})
	return responseBody, nil
}
