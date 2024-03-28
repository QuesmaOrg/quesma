package testdata

import "mitmproxy/quesma/model"

type SearchTestCase struct {
	Name            string
	QueryJson       string
	WantedSql       []string // array because of non-determinism
	WantedQueryType model.SearchQueryType
	WantedQuery     []model.Query // array because of non-determinism
	WantedRegexes   []string      // regexes saying what SELECT queries to CH should look like (in order). A lot of '.' here because of non-determinism.
}

type AsyncSearchTestCase struct {
	Name              string
	QueryJson         string
	ResultJson        string // from ELK
	Comment           string
	WantedParseResult model.QueryInfoAsyncSearch
	WantedRegexes     []string // queries might be a bit weird at times, because of non-determinism of our parser (need to use a lot of "." in regexes) (they also need to happen as ordered in this slice)
	IsAggregation     bool     // is it an aggregation query?
}

type AggregationTestCase struct {
	TestName         string
	QueryRequestJson string                   // JSON query request, just like received from Kibana
	ExpectedResponse string                   // JSON response, just like Elastic would respond to the query request
	ExpectedResults  [][]model.QueryResultRow // [0] = result for first aggregation, [1] = result for second aggregation, etc.
	ExpectedSQLs     []string                 // [0] = translated SQLs for first aggregation, [1] = translated SQL for second aggregation, etc.
}
