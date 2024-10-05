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
	"quesma/model/bucket_aggregations"
	"quesma/quesma/config"
	"quesma/quesma/types"
	"quesma/schema"
	"quesma/util"
	"strconv"
	"strings"
	"testing"
)

const TableName = model.SingleTableNamePlaceHolder

func TestPancakeQueryGeneration(t *testing.T) {

	// logger.InitSimpleLoggerForTests()
	table := clickhouse.Table{
		Cols: map[string]*clickhouse.Column{
			"@timestamp":  {Name: "@timestamp", Type: clickhouse.NewBaseType("DateTime64")},
			"timestamp":   {Name: "timestamp", Type: clickhouse.NewBaseType("DateTime64")},
			"order_date":  {Name: "order_date", Type: clickhouse.NewBaseType("DateTime64")},
			"message":     {Name: "message", Type: clickhouse.NewBaseType("String")},
			"bytes_gauge": {Name: "bytes_gauge", Type: clickhouse.NewBaseType("UInt64")},
		},
		Name:   tableName,
		Config: clickhouse.NewDefaultCHConfig(),
	}

	lm := clickhouse.NewLogManager(concurrent.NewMapWith(tableName, &table), &config.QuesmaConfiguration{})
	currentSchema := schema.Schema{
		Fields:             nil,
		Aliases:            nil,
		ExistsInDataSource: false,
		DatabaseName:       "",
	}

	cw := ClickhouseQueryTranslator{ClickhouseLM: lm, Table: &table, Ctx: context.Background(), Schema: currentSchema}

	for i, test := range allAggregationTests() {
		t.Run(test.TestName+"("+strconv.Itoa(i)+")", func(t *testing.T) {
			if test.TestName == "Range with subaggregations. Reproduce: Visualize -> Pie chart -> Aggregation: Top Hit, Buckets: Aggregation: Range(file:opensearch-visualize/agg_req,nr:1)" {
				t.Skip("Skipped also for previous implementation. Top_hits needs to be better.")
			}
			if filters(test.TestName) {
				t.Skip("Fix filters")
			}

			if test.TestName == "complex sum_bucket. Reproduce: Visualize -> Vertical Bar: Metrics: Sum Bucket (Bucket: Date Histogram, Metric: Average), Buckets: X-Asis: Histogram(file:opensearch-visualize/pipeline_agg_req,nr:22)" {
				t.Skip("error: filter(s)/range/dataRange aggregation must be the last bucket aggregation")
			}

			if test.TestName == "Reproduce: Visualize -> Vertical Bar: Metrics: Cumulative Sum (Aggregation: Avg), Buckets: Date Histogram(file:kibana-visualize/pipeline_agg_req,nr:1)" {
				t.Skip("test generally passes, but we don't add empty rows for cumulative_sum, and that needs fixing")
			}

			fmt.Println("i:", i, "test:", test.TestName)

			jsonp, err := types.ParseJSON(test.QueryRequestJson)
			assert.NoError(t, err)

			pancakeSqls, err := cw.PancakeParseAggregationJson(jsonp, false)
			assert.NoError(t, err)
			assert.True(t, len(pancakeSqls) >= 1, "pancakeSqls should have at least one query")
			if len(pancakeSqls) < 1 {
				return
			}

			assert.Len(t, pancakeSqls, 1+len(test.ExpectedAdditionalPancakeSQLs),
				"Mismatch pancake sqls vs main and 'ExpectedAdditionalPancakeSQLs'")
			for pancakeIdx, pancakeSql := range pancakeSqls {
				pancakeSqlStr := model.AsString(pancakeSql.SelectCommand)

				prettyPancakeSql := util.SqlPrettyPrint([]byte(pancakeSqlStr))

				var expectedSql string
				if pancakeIdx == 0 {
					expectedSql = test.ExpectedPancakeSQL
				} else {
					if pancakeIdx-1 >= len(test.ExpectedAdditionalPancakeSQLs) {
						pp.Println("=== Expected additional SQL:")
						fmt.Println(prettyPancakeSql)
						continue
					}
					if pancakeIdx-1 >= len(test.ExpectedAdditionalPancakeResults) {
						pp.Println("=== Expected additional results for SQL:")
						fmt.Println(prettyPancakeSql)
					}
					expectedSql = test.ExpectedAdditionalPancakeSQLs[pancakeIdx-1]
				}
				prettyExpectedSql := util.SqlPrettyPrint([]byte(strings.TrimSpace(expectedSql)))

				util.AssertSqlEqual(t, prettyExpectedSql, prettyPancakeSql)

				_, ok := pancakeSql.Type.(PancakeQueryType)
				if !ok {
					assert.Fail(t, "Expected pancake query type")
				}
			}

			if incorrectResult(test.TestName) {
				t.Skip("We don't have result yet")
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

			sqlResults := [][]model.QueryResultRow{test.ExpectedPancakeResults}
			if len(test.ExpectedAdditionalPancakeResults) > 0 {
				sqlResults = append(sqlResults, test.ExpectedAdditionalPancakeResults...)
			}

			pancakeJson, err := cw.MakeAggregationPartOfResponse(pancakeSqls, sqlResults)

			if err != nil {
				t.Fatal("Failed to render pancake JSON", err)
			}

			// FIXME we can quite easily remove 'probability' and 'seed' from above - just start remembering them in RandomSampler struct and print in JSON response.
			acceptableDifference := []string{"probability", "seed", bucket_aggregations.OriginalKeyName,
				"bg_count", "doc_count_error_upper_bound"} // Don't know why, but those 2 are still needed in new (clients/ophelia) tests. Let's fix it in another PR
			if len(test.AdditionalAcceptableDifference) > 0 {
				acceptableDifference = append(acceptableDifference, test.AdditionalAcceptableDifference...)
			}

			actualMinusExpected, expectedMinusActual := util.MapDifference(pancakeJson,
				expectedAggregationsPart, acceptableDifference, true, true)

			if len(actualMinusExpected) != 0 {
				pp.Println("ACTUAL diff", actualMinusExpected)
			}
			if len(expectedMinusActual) != 0 {
				pp.Println("EXPECTED diff", expectedMinusActual)
			}
			//pp.Println("ACTUAL", pancakeJson)
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

// We generate correct SQL, but result JSON did not match
func incorrectResult(testName string) bool {
	t1 := testName == "date_range aggregation(file:agg_req,nr:22)" // we use relative time
	t2 := testName == "complex filters(file:agg_req,nr:18)"        // almost, we differ in doc 0 counts
	// to be deleted after pancakes
	t3 := testName == "clients/kunkka/test_0, used to be broken before aggregations merge fix"+
		"Output more or less works, but is different and worse than what Elastic returns."+
		"If it starts failing, maybe that's a good thing(file:clients/kunkka,nr:0)"
	// below test is replacing it
	// testName == "it's the same input as in previous test, but with the original output from Elastic."+
	//	"Skipped for now, as our response is different in 2 things: key_as_string date (probably not important) + we don't return 0's (e.g. doc_count: 0)."+
	//	"If we need clients/kunkka/test_0, used to be broken before aggregations merge fix"
	return t1 || t2 || t3
}

// TODO remove after fix
func filters(testName string) bool {
	// this works, but is very suboptimal and didn't update the test case
	t1 := testName == "clients/kunkka/test_1, used to be broken before aggregations merge fix(file:clients/kunkka,nr:2)" // multi level filters
	return t1
}

func TestPancakeQueryGeneration_halfpancake(t *testing.T) {

	debug := true

	table := clickhouse.Table{
		Cols: map[string]*clickhouse.Column{
			"@timestamp":  {Name: "@timestamp", Type: clickhouse.NewBaseType("DateTime64")},
			"timestamp":   {Name: "timestamp", Type: clickhouse.NewBaseType("DateTime64")},
			"order_date":  {Name: "order_date", Type: clickhouse.NewBaseType("DateTime64")},
			"message":     {Name: "message", Type: clickhouse.NewBaseType("String")},
			"bytes_gauge": {Name: "bytes_gauge", Type: clickhouse.NewBaseType("UInt64")},
		},
		Name:   tableName,
		Config: clickhouse.NewDefaultCHConfig(),
	}

	lm := clickhouse.NewLogManager(concurrent.NewMapWith(tableName, &table), &config.QuesmaConfiguration{})

	currentSchema := schema.Schema{}

	cw := ClickhouseQueryTranslator{ClickhouseLM: lm, Table: &table, Ctx: context.Background(), Schema: currentSchema}

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
SELECT sum(count(*)) OVER () AS "aggr__0__parent_count",
  "host.name" AS "aggr__0__key_0", count(*) AS "aggr__0__count"
FROM ` + TableName + `
GROUP BY "host.name" AS "aggr__0__key_0"
ORDER BY "aggr__0__count" DESC, "aggr__0__key_0" ASC
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
SELECT sum(count(*)) OVER () AS "aggr__0__parent_count",
  "host.name" AS "aggr__0__key_0", count(*) AS "aggr__0__count",
  avgOrNull("bytes_gauge") AS "metric__0__2_col_0"
FROM ` + TableName + `
GROUP BY "host.name" AS "aggr__0__key_0"
ORDER BY "aggr__0__count" DESC, "aggr__0__key_0" ASC
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
