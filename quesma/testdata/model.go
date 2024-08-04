// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package testdata

import (
	"quesma/model"
)

type SearchTestCase struct {
	Name           string
	QueryJson      string
	WantedSql      []string // array because of non-determinism
	WantedHitsType model.HitsType
	//WantedQuery     []model.Query // array because of non-determinism
	WantedRegexes []string // regexes saying what SELECT queries to CH should look like (in order). A lot of '.' here because of non-determinism.
}

type AsyncSearchTestCase struct {
	Name           string
	QueryJson      string
	ResultJson     string // from ELK
	Comment        string
	WantedHitsInfo model.HitsInfo
	WantedRegexes  []string // queries might be a bit weird at times, because of non-determinism of our parser (need to use a lot of "." in regexes) (they also need to happen as ordered in this slice)
	IsAggregation  bool     // is it an aggregation query?
}

type AggregationTestCase struct {
	TestName                         string
	QueryRequestJson                 string                   // JSON query request, just like received from Kibana
	ExpectedResponse                 string                   // JSON response, just like Elastic would respond to the query request
	ExpectedResults                  [][]model.QueryResultRow // [0] = result for first aggregation, [1] = result for second aggregation, etc.
	ExpectedPancakeResults           []model.QueryResultRow   // nil if we don't have pancake results for this test
	ExpectedSQLs                     []string                 // [0] = translated SQLs for first aggregation, [1] = translated SQL for second aggregation, etc.
	ExpectedPancakeSQL               string                   // "" if we don't have pancake results for this test
	ExpectedAdditionalPancakeSQLs    []string                 // additional SQLs that are not part of the main query
	ExpectedAdditionalPancakeResults [][]model.QueryResultRow // additional results that are not part of the main query
	AdditionalAcceptableDifference   []string                 // additional keys that may differ in json response
}

type UnsupportedQueryTestCase struct {
	TestName         string
	QueryType        string
	QueryRequestJson string
}

type FullSearchTestCase struct {
	Name               string
	QueryRequestJson   string
	ExpectedResponse   string
	ExpectedSQLs       []string
	ExpectedSQLResults [][]model.QueryResultRow
}
