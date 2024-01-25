package quesma

import (
	"fmt"
	"mitmproxy/quesma/clickhouse"
	"mitmproxy/quesma/model"
	"mitmproxy/quesma/queryparser"
)

func handleSearch(index string, body []byte, lm *clickhouse.LogManager,
	responseMatcher *ResponseMatcher,
	queryDebugger *QueryDebugger,
	requestId string) {
	// TODO: implement
	var translatedQueryBody []byte
	queryTranslator := &queryparser.ClickhouseQueryTranslator{ClickhouseLM: lm}

	// TODO index argument is not used yet
	_ = index

	// old TODO: query clickhouse
	// get response
	// and translate

	query := queryTranslator.Write(body)
	var responseBody []byte
	if query.CanParse {
		rows, err := queryTranslator.QueryClickhouse(query)
		if err != nil {
			responseBody = []byte("Error processing query: " + query.Sql + ", err: " + err.Error())
		}
		responseBody = append([]byte(responseBody), []byte("{\n")...)
		responseBody = append([]byte(responseBody), []byte("\"hit\": [")...)
		numRows := len(rows)
		i := 0
		for _, row := range rows {
			_ = row
			responseBody = append(responseBody, []byte(row.String())...)
			if i < numRows-1 {
				responseBody = append(responseBody, []byte(",\n")...)
			}
			i++
		}
		responseBody = append([]byte(responseBody), []byte("]}")...)
		rows, err = queryTranslator.GetNMostRecentRows(query.TableName, "*", "timestamp", query.Sql, 2)
		if err == nil {
			fmt.Println(rows)
		} else {
			fmt.Println(err)
		}
		responseTranslator := &queryparser.ClickhouseResultReader{ClickhouseLM: lm}
		responseTranslator.Read(responseBody) // TODO implement this, not line below
		histogram, err := queryTranslator.GetHistogram(query.TableName)
		fmt.Printf("Histogram: %+v, err: %+v\n", histogram, err)

		facets, err := queryTranslator.GetFacets(query.TableName, "severity", query.Sql, 0)
		fmt.Printf("Facets: %+v, err: %+v\n", facets, err)
	} else {
		responseBody = []byte("Invalid Query, err: " + query.Sql)
	}

	var rawResults []byte
	responseMatcher.Push(&QResponse{requestId, responseBody})
	translatedQueryBody = []byte(query.Sql)
	queryDebugger.PushSecondaryInfo(&QueryDebugSecondarySource{
		id:                     requestId,
		incomingQueryBody:      body,
		queryBodyTranslated:    translatedQueryBody,
		queryRawResults:        rawResults,
		queryTranslatedResults: responseBody,
	})
}

func createResponseHitJson(rows []clickhouse.QueryResultRow) []byte {
	responseBody := []byte{}
	responseBody = append([]byte(responseBody), []byte("{\n")...)
	numRows := len(rows)
	i := 0
	for _, row := range rows {
		_ = row
		responseBody = append(responseBody, []byte("\"hit\":"+row.String())...)
		if i < numRows-1 {
			responseBody = append(responseBody, []byte(",\n")...)
		}
		i++
	}
	responseBody = append([]byte(responseBody), []byte("}")...)

	return responseBody
}

func createResponseHistogramJson(rows []clickhouse.HistogramResult) []byte {
	responseBody := []byte{}
	responseBody = append([]byte(responseBody), []byte("{\n")...)
	numRows := len(rows)
	i := 0
	for _, row := range rows {
		_ = row
		responseBody = append(responseBody, []byte("\"bucket\":"+row.String())...)
		if i < numRows-1 {
			responseBody = append(responseBody, []byte(",\n")...)
		}
		i++
	}
	responseBody = append([]byte(responseBody), []byte("}")...)

	return responseBody
}

func handleAsyncSearch(index string, body []byte, lm *clickhouse.LogManager,
	responseMatcher *ResponseMatcher,
	queryDebugger *QueryDebugger,
	requestId string) {
	// TODO: implement
	var translatedQueryBody []byte
	queryTranslator := &queryparser.ClickhouseQueryTranslator{ClickhouseLM: lm}

	// TODO index argument is not used yet
	_ = index

	// old TODO: query clickhouse
	// get response
	// and translate

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
			rows, err := queryTranslator.GetNMostRecentRows(query.TableName, queryInfo.FieldName, "timestamp", query.Sql, queryInfo.I2)
			fmt.Printf("Rows: %+v, err: %+v\n", rows, err)
			responseBody = createResponseHitJson(rows)
		case model.ListAllFields:
			// queryInfo = (ListAllFields, "*", 0, LIMIT)
			rows, err := queryTranslator.GetNMostRecentRows(query.TableName, "*", "timestamp", query.Sql, queryInfo.I2)
			fmt.Printf("Rows: %+v, err: %+v\n", rows, err)
			responseBody = createResponseHitJson(rows)

		}

		//cnt, err := queryTranslator.queryClickhouse(query.sql)
		//if err != nil {
		//	responseBody = []byte("Error processing query: " + query.sql + ", err: " + err.Error())
		//}
		//responseTranslator := &ClickhouseResultReader{clickhouseLM: lm}
		//responseTranslator.Read(responseBody) // TODO implement this, not line below
		// responseBody = []byte(strconv.Itoa(cnt))
	} else {
		responseBody = []byte("Invalid Query, err: " + query.Sql)
	}

	var rawResults []byte
	responseMatcher.Push(&QResponse{requestId, responseBody})
	translatedQueryBody = []byte(query.Sql)
	queryDebugger.PushSecondaryInfo(&QueryDebugSecondarySource{
		id:                     requestId,
		incomingQueryBody:      body,
		queryBodyTranslated:    translatedQueryBody,
		queryRawResults:        rawResults,
		queryTranslatedResults: responseBody,
	})
}
