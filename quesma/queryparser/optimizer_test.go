// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package queryparser

import (
	"context"
	"fmt"
	"github.com/jinzhu/copier"
	"github.com/k0kubun/pp"
	"github.com/stretchr/testify/assert"
	"quesma/clickhouse"
	"quesma/concurrent"
	"quesma/logger"
	"quesma/model"
	"quesma/queryparser/query_util"
	"quesma/quesma/config"
	"quesma/quesma/types"
	"quesma/schema"
	"quesma/testdata/query_optimizers"
	"quesma/util"
	"strconv"
	"strings"
	"testing"
)

func TestMergeMetricsAggsTransformer(t *testing.T) {
	logger.InitSimpleLoggerForTests()
	table := clickhouse.Table{
		Cols: map[string]*clickhouse.Column{
			"@timestamp":  {Name: "@timestamp", Type: clickhouse.NewBaseType("DateTime64")},
			"timestamp":   {Name: "timestamp", Type: clickhouse.NewBaseType("DateTime64")},
			"order_date":  {Name: "order_date", Type: clickhouse.NewBaseType("DateTime64")},
			"message":     {Name: "message", Type: clickhouse.NewBaseType("String"), IsFullTextMatch: true},
			"bytes_gauge": {Name: "bytes_gauge", Type: clickhouse.NewBaseType("UInt64")},
		},
		Name:   "logs-generic-default",
		Config: clickhouse.NewDefaultCHConfig(),
	}
	lm := clickhouse.NewLogManager(concurrent.NewMapWith(tableName, &table), config.QuesmaConfiguration{})

	s := staticRegistry{
		tables: map[schema.TableName]schema.Schema{
			"logs-generic-default": {
				Fields: map[schema.FieldName]schema.Field{
					"host.name":         {PropertyName: "host.name", InternalPropertyName: "host.name", Type: schema.TypeObject},
					"type":              {PropertyName: "type", InternalPropertyName: "type", Type: schema.TypeText},
					"name":              {PropertyName: "name", InternalPropertyName: "name", Type: schema.TypeText},
					"content":           {PropertyName: "content", InternalPropertyName: "content", Type: schema.TypeText},
					"message":           {PropertyName: "message", InternalPropertyName: "message", Type: schema.TypeText},
					"host_name.keyword": {PropertyName: "host_name.keyword", InternalPropertyName: "host_name.keyword", Type: schema.TypeKeyword},
					"FlightDelay":       {PropertyName: "FlightDelay", InternalPropertyName: "FlightDelay", Type: schema.TypeText},
					"Cancelled":         {PropertyName: "Cancelled", InternalPropertyName: "Cancelled", Type: schema.TypeText},
					"FlightDelayMin":    {PropertyName: "FlightDelayMin", InternalPropertyName: "FlightDelayMin", Type: schema.TypeText},
					"_id":               {PropertyName: "_id", InternalPropertyName: "_id", Type: schema.TypeText},
				},
			},
		},
	}
	cw := ClickhouseQueryTranslator{ClickhouseLM: lm, Table: &table, Ctx: context.Background(), SchemaRegistry: s}
	//for i, test := range query_optimizers.MergeMetricsAggsOptimizerTests {
	for i, test := range getAllAggregationTestCases() {
		t.Run(test.TestName+"("+strconv.Itoa(i)+")", func(t *testing.T) {
			if test.TestName == "Max/Sum bucket with some null buckets. Reproduce: Visualize -> Vertical Bar: Metrics: Max (Sum) Bucket (Aggregation: Date Histogram, Metric: Min)" {
				t.Skip("Needs to be fixed by keeping last key for every aggregation. Now we sometimes don't know it. Hard to reproduce, leaving it for separate PR")
			}
			if test.TestName == "complex sum_bucket. Reproduce: Visualize -> Vertical Bar: Metrics: Sum Bucket (Bucket: Date Histogram, Metric: Average), Buckets: X-Asis: Histogram" {
				t.Skip("Waiting for fix. Now we handle only the case where pipeline agg is at the same nesting level as its parent. Should be quick to fix.")
			}
			if i == 27 || i == 29 || i == 30 {
				t.Skip("New tests, harder, failing for now. Fixes for them in 2 next PRs")
			}
			if strings.HasPrefix(test.TestName, "dashboard-1") {
				t.Skip("Those 2 tests have nested histograms with min_doc_count=0. I'll add support for that in next PR, already most of work done")
			}
			if test.TestName == "Range with subaggregations. Reproduce: Visualize -> Pie chart -> Aggregation: Top Hit, Buckets: Aggregation: Range" {
				t.Skip("Need a (most likely) small fix to top_hits.")
			}
			if i == 20 {
				t.Skip("Fixed in next PR.")
			}
			if i == 7 {
				t.Skip("Let's implement top_hits in next PR. Easily doable, just a bit of code.")
			}
			if test.TestName == "it's the same input as in previous test, but with the original output from Elastic."+
				"Skipped for now, as our response is different in 2 things: key_as_string date (probably not important) + we don't return 0's (e.g. doc_count: 0)."+
				"If we need clients/kunkka/test_0, used to be broken before aggregations merge fix" {
				t.Skip("Unskip and remove the previous test after those fixes.")
			}
			if test.TestName == "clients/kunkka/test_1, used to be broken before aggregations merge fix" {
				t.Skip("Small details left for this test to be correct. I'll (Krzysiek) fix soon after returning to work")
			}

			if i == 1 || i == 5 || i == 28 || i == 80 {
				t.Skip() // FIX FILTERS
			}
			if i == 10 || i == 32 {
				t.Skip() // FIX TOP_METRICS
				// 32 std dev
			}

			body, parseErr := types.ParseJSON(test.QueryRequestJson)
			assert.NoError(t, parseErr)

			queries_before_optimization, canParse, err := cw.ParseQuery(body)
			assert.True(t, canParse)
			assert.NoError(t, err)

			assert.Len(t, test.ExpectedResults, len(queries_before_optimization))
			sortAggregations(queries_before_optimization) // to make test runs deterministic

			queries, err := MergeMetricsAggsTransformer{ctx: context.Background()}.Transform(queries_before_optimization)
			assert.NoError(t, err)

			var expectedSQLs []string
			var expectedResults [][]model.QueryResultRow
			found := false
			for _, testUpdate := range query_optimizers.MergeMetricsAggsTestUpdates {
				if testUpdate.TestName == test.TestName {
					expectedSQLs = testUpdate.ExpectedSQLs
					expectedResults = testUpdate.ExpectedResults
					found = true
					break
				}
			}
			if !found {
				expectedSQLs = test.ExpectedSQLs
				expectedResults = test.ExpectedResults
			}

			// Let's leave those commented debugs for now, they'll be useful in next PRs
			for j, query := range queries {
				fmt.Printf("--- Aggregation %d: %+v\n\n---SQL string: %s\n\n", j, query, model.AsString(query.SelectCommand))
				if expectedSQLs[j] != "NoDBQuery" {
					util.AssertSqlEqual(t, expectedSQLs[j], query.SelectCommand.String())
				}
				if query_util.IsNonAggregationQuery(query) {
					continue
				}
				expectedResults[j] = query.Type.PostprocessResults(expectedResults[j])
				fmt.Println(j, expectedResults[j])
				// fmt.Println("--- Group by: ", query.GroupByFields)
			}

			// I copy `test.ExpectedResults`, as it's processed 2 times and each time it might be modified by
			// pipeline aggregation processing.
			var expectedResultsCopy [][]model.QueryResultRow
			err = copier.CopyWithOption(&expectedResultsCopy, &expectedResults, copier.Option{DeepCopy: true})
			assert.NoError(t, err)
			// pp.Println("EXPECTED", expectedResultsCopy)
			fmt.Println(expectedResults)
			response := cw.MakeSearchResponse(queries, expectedResults)
			responseMarshalled, marshalErr := response.Marshal()
			pp.Println("ACTUAL", response)
			assert.NoError(t, marshalErr)

			expectedResponseMap, _ := util.JsonToMap(test.ExpectedResponse)
			var expectedAggregationsPart JsonMap
			if responseSubMap, hasResponse := expectedResponseMap["response"]; hasResponse {
				expectedAggregationsPart = responseSubMap.(JsonMap)["aggregations"].(JsonMap)
			} else {
				expectedAggregationsPart = expectedResponseMap["aggregations"].(JsonMap)
			}
			actualMinusExpected, expectedMinusActual := util.MapDifference(response.Aggregations, expectedAggregationsPart, true, true)

			// probability and seed are present in random_sampler aggregation. I'd assume they are not needed, thus let's not care about it for now.
			acceptableDifference := []string{"doc_count_error_upper_bound", "sum_other_doc_count", "probability", "seed", "bg_count", "doc_count", model.KeyAddedByQuesma}
			// pp.Println("ACTUAL", actualMinusExpected)
			// pp.Println("EXPECTED", expectedMinusActual)
			assert.True(t, util.AlmostEmpty(actualMinusExpected, acceptableDifference))
			assert.True(t, util.AlmostEmpty(expectedMinusActual, acceptableDifference))
			if body["track_total_hits"] == true { // FIXME some better check after track_total_hits
				assert.Contains(t, string(responseMarshalled), `"value":`+strconv.FormatUint(test.ExpectedResults[0][0].Cols[0].Value.(uint64), 10))
			} // checks if hits nr is OK
		})
	}
}
