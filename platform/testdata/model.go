// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package testdata

import (
	"github.com/QuesmaOrg/quesma/platform/model"
)

type SearchTestCase struct {
	Name            string
	QueryJson       string
	WantedSql       []string // array because of non-determinism
	WantedQueryType model.HitsInfo
	//WantedQuery     []model.Query // array because of non-determinism
	WantedRegexes []string // regexes saying what SELECT queries to CH should look like (in order). A lot of '.' here because of non-determinism.
	WantedQueries []string
}

type AsyncSearchTestCase struct {
	Name              string
	QueryJson         string
	ResultJson        string // from ELK
	Comment           string
	WantedParseResult model.HitsCountInfo
	WantedQuery       []string
	IsAggregation     bool // is it an aggregation query?
}

type AggregationTestCase struct {
	TestName                         string
	QueryRequestJson                 string                   // JSON query request, just like received from Kibana
	ExpectedResponse                 string                   // JSON response, just like Elastic would respond to the query request
	ExpectedPancakeResults           []model.QueryResultRow   // nil if we don't have pancake results for this test
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
