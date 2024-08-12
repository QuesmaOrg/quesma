// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package queryparser

import (
	"context"
	"fmt"
	"github.com/k0kubun/pp"
	"github.com/stretchr/testify/assert"
	"quesma/clickhouse"
	"quesma/concurrent"
	"quesma/model"
	"quesma/quesma/config"
	"quesma/quesma/types"
	"quesma/schema"
	"quesma/util"
	"strconv"
	"strings"
	"testing"
)

func TestPancakeQueryGeneration(t *testing.T) {

	// logger.InitSimpleLoggerForTests()
	table := clickhouse.Table{
		Cols: map[string]*clickhouse.Column{
			"@timestamp":  {Name: "@timestamp", Type: clickhouse.NewBaseType("DateTime64")},
			"timestamp":   {Name: "timestamp", Type: clickhouse.NewBaseType("DateTime64")},
			"order_date":  {Name: "order_date", Type: clickhouse.NewBaseType("DateTime64")},
			"message":     {Name: "message", Type: clickhouse.NewBaseType("String"), IsFullTextMatch: true},
			"bytes_gauge": {Name: "bytes_gauge", Type: clickhouse.NewBaseType("UInt64")},
		},
		Name:   tableName,
		Config: clickhouse.NewDefaultCHConfig(),
	}

	lm := clickhouse.NewLogManager(concurrent.NewMapWith(tableName, &table), config.QuesmaConfiguration{})
	schemaRegistry := schema.StaticRegistry{}

	cw := ClickhouseQueryTranslator{ClickhouseLM: lm, Table: &table, Ctx: context.Background(), SchemaRegistry: schemaRegistry}

	for i, test := range allAggregationTestsWithoutPipeline() { // TODO fix pipeline
		t.Run(test.TestName+"("+strconv.Itoa(i)+")", func(t *testing.T) {
			if test.ExpectedPancakeSQL == "" || test.ExpectedPancakeResults == nil { // TODO remove this
				t.Skip("Not updated answers for pancake.")
			}
			if strings.HasPrefix(test.TestName, "dashboard-1") {
				t.Skip("Skipped also for previous implementation. Those 2 tests have nested histograms with min_doc_count=0. Some work done long time ago (Krzysiek)")
			}
			if i == 29 || i == 30 {
				t.Skip("Skipped also for previous implementation. New tests, harder, failing for now.")
			}
			if Range(test.TestName) {
				t.Skip("Fix range")
			}
			if dateRange(test.TestName) {
				t.Skip("Fix date range")
			}
			if percentileRanks(test.TestName) {
				t.Skip("Fix percentile ranks")
			}
			if topHits(test.TestName) {
				t.Skip("Fix top_hits")
			}
			if topMetrics(test.TestName) {
				t.Skip("Fix top metrics")
			}
			if multiplePancakes(test.TestName) {
				t.Skip("Fix multiple pancakes")
			}
			if histogramMinDocCount0(test.TestName) {
				t.Skip("Fix histogram min doc count 0")
			}
			if filter(test.TestName) {
				t.Skip("Fix filter")
			}
			if filters(test.TestName) {
				t.Skip("Fix filters")
			}
			if multiTerms(test.TestName) {
				t.Skip("Fix multi terms")
			}
			if valueCount(test.TestName) {
				t.Skip("Fix value count")
			}

			if i != 19 {
				//t.Skip()
			}

			fmt.Println("i:", i, "test:", test.TestName)

			jsonp, err := types.ParseJSON(test.QueryRequestJson)
			assert.NoError(t, err)

			pancakeSqls, err := cw.PancakeParseAggregationJson(jsonp, false)
			assert.NoError(t, err)
			assert.True(t, len(pancakeSqls) == 1, "pancakeSqls should have only one query")
			if len(pancakeSqls) < 1 {
				return
			}
			pancakeSqlStr := model.AsString(pancakeSqls[0].SelectCommand)

			prettyExpectedSql := util.SqlPrettyPrint([]byte(strings.TrimSpace(test.ExpectedPancakeSQL)))

			prettyPancakeSql := util.SqlPrettyPrint([]byte(pancakeSqlStr))

			/*
				pp.Println("Expected SQL:")
				fmt.Println(prettyExpectedSql)
				pp.Println("Actual (pancake) SQL:")
				fmt.Println(prettyPancakeSql)
			*/

			util.AssertSqlEqual(t, prettyExpectedSql, prettyPancakeSql)
			// assert.Equal(t, prettyExpectedSql, prettyPancakeSql)

			queryType, ok := pancakeSqls[0].Type.(PancakeQueryType)
			if !ok {
				assert.Fail(t, "Expected pancake query type")
			}

			expectedJson, err := util.JsonToMap(test.ExpectedResponse)
			if err != nil {
				assert.Fail(t, "Failed to parse expected JSON")
			}
			var expectedAggregationsPart model.JsonMap
			if responseSubMap, hasResponse := expectedJson["response"]; hasResponse {
				expectedAggregationsPart = responseSubMap.(JsonMap)["aggregations"].(JsonMap)
			} else {
				expectedAggregationsPart = expectedJson["aggregations"].(JsonMap)
			}
			assert.NotNil(t, expectedAggregationsPart, "Expected JSON should have 'response'/'aggregations' part")

			renderer := &pancakeJSONRenderer{}
			pancakeJson, err := renderer.toJSON(queryType.pancakeAggregation, test.ExpectedPancakeResults)

			if err != nil {
				t.Fatal("Failed to render pancake JSON", err)
			}

			// FIXME we can quite easily remove 'probability' and 'seed' from above - just start remembering them in RandomSampler struct and print in JSON response.
			acceptableDifference := []string{"sum_other_doc_count", "probability", "seed", "bg_count", "doc_count", model.KeyAddedByQuesma,
				"sum_other_doc_count", "doc_count_error_upper_bound"} // Don't know why, but those 2 are still needed in new (clients/ophelia) tests. Let's fix it in another PR

			actualMinusExpected, expectedMinusActual := util.MapDifference(pancakeJson,
				expectedAggregationsPart, acceptableDifference, true, true)
			if len(actualMinusExpected) != 0 {
				pp.Println("ACTUAL diff", actualMinusExpected)
			}
			if len(expectedMinusActual) != 0 {
				pp.Println("EXPECTED diff", expectedMinusActual)
			}
			pp.Println("ACTUAL", pancakeJson)
			//pp.Println("EXPECTED", expectedAggregationsPart)
			assert.True(t, util.AlmostEmpty(actualMinusExpected, acceptableDifference))
			assert.True(t, util.AlmostEmpty(expectedMinusActual, acceptableDifference))

			/*
				if i == 0 {
					 Sample code for Rafal:
					sqlRowResults := clients.OpheliaTestsPancake[0]
					pancakeSqls[0].pancakeItself
					jsonP := panckakeGenerateJsonReturn(pancakeSqls[0].pancakeItself, sqlRowResults)
					test.ExpectedResponse // parse and take "aggs" and compare
				}
			*/
		})
	}
}

// TODO remove after fix
func Range(testName string) bool {
	t1 := testName == "Range with subaggregations. Reproduce: Visualize -> Heat Map -> Metrics: Median, Buckets: X-Asis Range"
	t2 := testName == "Range with subaggregations. Reproduce: Visualize -> Pie chart -> Aggregation: Sum, Buckets: Aggregation: Range"
	t3 := testName == "Range with subaggregations. Reproduce: Visualize -> Pie chart -> Aggregation: Top Hit, Buckets: Aggregation: Range"
	t4 := testName == "Range with subaggregations. Reproduce: Visualize -> Pie chart -> Aggregation: Unique Count, Buckets: Aggregation: Range"
	t5 := testName == "range bucket aggregation, both keyed and not"
	return t1 || t2 || t3 || t4 || t5
}

// TODO remove after fix
func dateRange(testName string) bool {
	t1 := testName == "range bucket aggregation, both keyed and not"
	return t1
}

func percentileRanks(testName string) bool {
	return testName == "Percentile_ranks keyed=false. Reproduce: Visualize -> Line -> Metrics: Percentile Ranks, Buckets: X-Asis Date Histogram"
}

// TODO remove after fix
func topHits(testName string) bool {
	t1 := testName == "Range with subaggregations. Reproduce: Visualize -> Pie chart -> Aggregation: Top Hit, Buckets: Aggregation: Range" // also range
	t2 := testName == "top hits, quite complex"
	return t1 || t2
}

// TODO remove after fix
func topMetrics(testName string) bool {
	t1 := testName == "Kibana Visualize -> Last Value. Used to panic" // also filter
	t2 := testName == "simplest top_metrics, no sort"
	t3 := testName == "simplest top_metrics, with sort"
	t4 := testName == "very long: multiple top_metrics + histogram" // also top_metrics
	return t1 || t2 || t3 || t4
}

// TODO remove after fix
func multiplePancakes(testName string) bool {
	return testName == "histogram with all possible calendar_intervals"
}

// TODO remove after fix
func histogramMinDocCount0(testName string) bool {
	t1 := testName == "simple histogram, but min_doc_count: 0"
	t2 := testName == "simple date_histogram, but min_doc_count: 0"
	return t1 || t2
}

// TODO remove after fix
func filter(testName string) bool {
	t1 := testName == "Terms, completely different tree results from 2 queries - merging them didn't work before"
	t2 := testName == "Kibana Visualize -> Last Value. Used to panic" // also top_metrics
	t3 := testName == "2 sibling count aggregations"
	t4 := testName == "simple filter/count"
	t5 := testName == "triple nested aggs"
	t6 := testName == "Field statistics > summary for numeric fields" // also percentiles
	t7 := testName == "clients/kunkka/test_0, used to be broken before aggregations merge fix"+
		"Output more or less works, but is different and worse than what Elastic returns."+
		"If it starts failing, maybe that's a good thing"
	t8 := testName == "it's the same input as in previous test, but with the original output from Elastic."+
		"Skipped for now, as our response is different in 2 things: key_as_string date (probably not important) + we don't return 0's (e.g. doc_count: 0)."+
		"If we need clients/kunkka/test_0, used to be broken before aggregations merge fix"
	t9 := testName == "clients/kunkka/test_1, used to be broken before aggregations merge fix" // also filters
	return t1 || t2 || t3 || t4 || t5 || t6 || t7 || t8 || t9
}

// TODO remove after fix
func filters(testName string) bool {
	t1 := testName == "filters"
	t2 := testName == "very long: multiple top_metrics + histogram" // also filters
	t3 := testName == "complex filters"
	t4 := testName == "clients/kunkka/test_1, used to be broken before aggregations merge fix" // also filter
	return t1 || t2 || t3 || t4
}

// TODO remove after fix
func multiTerms(testName string) bool {
	t1 := testName == "Multi_terms without subaggregations. Visualize: Bar Vertical: Horizontal Axis: Date Histogram, Vertical Axis: Count of records, Breakdown: Top values (2 values)"
	t2 := testName == "Multi_terms with simple count. Visualize: Bar Vertical: Horizontal Axis: Top values (2 values), Vertical: Count of records, Breakdown: @timestamp"
	t3 := testName == "Multi_terms with double-nested subaggregations. Visualize: Bar Vertical: Horizontal Axis: Top values (2 values), Vertical: Unique count, Breakdown: @timestamp"
	t4 := testName == "Quite simple multi_terms, but with non-string keys. Visualize: Bar Vertical: Horizontal Axis: Date Histogram, Vertical Axis: Count of records, Breakdown: Top values (2 values)"
	return t1 || t2 || t3 || t4
}

// TODO remove after fix
func valueCount(testName string) bool {
	return testName == "value_count + top_values: regression test"
}

func TestPancakeQueryGeneration_halfpancake(t *testing.T) {

	debug := true

	table := clickhouse.Table{
		Cols: map[string]*clickhouse.Column{
			"@timestamp":  {Name: "@timestamp", Type: clickhouse.NewBaseType("DateTime64")},
			"timestamp":   {Name: "timestamp", Type: clickhouse.NewBaseType("DateTime64")},
			"order_date":  {Name: "order_date", Type: clickhouse.NewBaseType("DateTime64")},
			"message":     {Name: "message", Type: clickhouse.NewBaseType("String"), IsFullTextMatch: true},
			"bytes_gauge": {Name: "bytes_gauge", Type: clickhouse.NewBaseType("UInt64")},
		},
		Name:   tableName,
		Config: clickhouse.NewDefaultCHConfig(),
	}

	lm := clickhouse.NewLogManager(concurrent.NewMapWith(tableName, &table), config.QuesmaConfiguration{})
	schemaRegistry := schema.StaticRegistry{}

	cw := ClickhouseQueryTranslator{ClickhouseLM: lm, Table: &table, Ctx: context.Background(), SchemaRegistry: schemaRegistry}

	tests := []struct {
		name string
		json string
		sql  string
	}{
		{
			name: "test1",
			json: `
{
  "aggs": {
    "0": {
      "terms": {
        "field": "host.name",
        "order": {
          "_count": "desc"
        },
        "shard_size": 25,
        "size": 3
      }
    }
  },
  "track_total_hits": true
}

`,
			sql: `
SELECT "host.name" AS "aggr__0__key_0", count(*) AS "aggr__0__count", count() AS
   "aggr__0__order_1"
FROM "logs-generic-default"
GROUP BY "host.name" AS "aggr__0__key_0"
ORDER BY "aggr__0__order_1" DESC, "aggr__0__key_0" ASC
LIMIT 4`, // -- we added one more as filtering nulls happens during rendering
		},

		{"test2",
			`
{
  "aggs": {
    "0": {
      "aggs": {
          "2": {
            "avg": {
              "field": "bytes_gauge"
          }
        }
      },
      "terms": {
        "field": "host.name",
        "size": 3
      }
    }
  }
}
`,
			`
SELECT "host.name" AS "aggr__0__key_0", count(*) AS "aggr__0__count", count() AS
   "aggr__0__order_1", avgOrNull("bytes_gauge") AS "metric__0__2_col_0"
FROM "logs-generic-default"
GROUP BY "host.name" AS "aggr__0__key_0"
ORDER BY "aggr__0__order_1" DESC, "aggr__0__key_0" ASC
LIMIT 4`, // we increased limit by 1 to allow filtering of nulls druing json rendering
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			jsonp, err := types.ParseJSON(tt.json)
			assert.NoError(t, err)

			pancakeSqls, err := cw.PancakeParseAggregationJson(jsonp, false)
			assert.NoError(t, err)
			assert.True(t, len(pancakeSqls) == 1, "pancakeSqls should have only one query")
			if len(pancakeSqls) < 1 {
				return
			}
			pancakeSqlStr := model.AsString(pancakeSqls[0].SelectCommand)
			prettyPancakeSql := util.SqlPrettyPrint([]byte(pancakeSqlStr))

			if debug {
				fmt.Println("Expected SQL:")
				fmt.Println(tt.sql)
				fmt.Println("Actual (pancake) SQL:")
				fmt.Println(prettyPancakeSql)
			}
			assert.Equal(t, strings.TrimSpace(tt.sql), strings.TrimSpace(prettyPancakeSql))

		})
	}

}
