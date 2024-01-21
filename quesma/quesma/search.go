package quesma

import (
	"mitmproxy/quesma/clickhouse"
	"strings"
)

func handleQuery(url string, body []byte, lm *clickhouse.LogManager,
	responseMatcher *ResponseMatcher,
	queryDebugger *QueryDebugger,
	requestId string) {
	if strings.Contains(url, "/_search?pretty") {
		var translatedQueryBody []byte
		queryTranslator := &ClickhouseQueryTranslator{clickhouseLM: lm}
		queryTranslator.Write(body)
		// TODO query clickhouse
		// get response
		// and translate
		var responseBody []byte
		responseTranslator := &ClickhouseResultReader{clickhouseLM: lm}
		responseTranslator.Read(responseBody)
		responseBody = []byte("clickhouse")
		var rawResults []byte
		responseMatcher.Push(&QResponse{requestId, responseBody})
		translatedQueryBody = []byte("select * from ...")
		queryDebugger.PushSecondaryInfo(&QueryDebugSecondarySource{
			id:                     requestId,
			incomingQueryBody:      body,
			queryBodyTranslated:    translatedQueryBody,
			queryRawResults:        rawResults,
			queryTranslatedResults: responseBody,
		})
	}
}

func handleSearch(index string, url string, body []byte, lm *clickhouse.LogManager,
	responseMatcher *ResponseMatcher,
	queryDebugger *QueryDebugger,
	requestId string) {
	// TODO: implement
	var translatedQueryBody []byte
	queryTranslator := &ClickhouseQueryTranslator{clickhouseLM: lm}
	queryTranslator.Write(body)
	// TODO query clickhouse
	// get response
	// and translate
	var responseBody []byte
	responseTranslator := &ClickhouseResultReader{clickhouseLM: lm}
	responseTranslator.Read(responseBody)
	responseBody = []byte("clickhouse")
	var rawResults []byte
	responseMatcher.Push(&QResponse{requestId, responseBody})
	translatedQueryBody = []byte("select * from ...")
	queryDebugger.PushSecondaryInfo(&QueryDebugSecondarySource{
		id:                     requestId,
		incomingQueryBody:      body,
		queryBodyTranslated:    translatedQueryBody,
		queryRawResults:        rawResults,
		queryTranslatedResults: responseBody,
	})
}
