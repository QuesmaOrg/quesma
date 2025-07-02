// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package elastic_query_dsl

import (
	"context"
	"fmt"
	"github.com/QuesmaOrg/quesma/platform/clickhouse"
	"github.com/QuesmaOrg/quesma/platform/model"
	"github.com/QuesmaOrg/quesma/platform/model/bucket_aggregations"
	"github.com/QuesmaOrg/quesma/platform/schema"
	"github.com/QuesmaOrg/quesma/platform/util"
	"github.com/QuesmaOrg/quesma/platform/v2/core/types"
	"github.com/k0kubun/pp"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
)

const TableName = model.SingleTableNamePlaceHolder

func TestPancakeQueryGeneration(t *testing.T) {

	// logger.InitSimpleLoggerForTestsWarnLevel()
	table := clickhouse.Table{
		Cols: map[string]*clickhouse.Column{
			"@timestamp":                     {Name: "@timestamp", Type: clickhouse.NewBaseType("DateTime64")},
			"timestamp":                      {Name: "timestamp", Type: clickhouse.NewBaseType("DateTime64")},
			"order_date":                     {Name: "order_date", Type: clickhouse.NewBaseType("DateTime64")},
			"message":                        {Name: "message", Type: clickhouse.NewBaseType("String")},
			"bytes_gauge":                    {Name: "bytes_gauge", Type: clickhouse.NewBaseType("UInt64")},
			"customer_birth_date":            {Name: "customer_birth_date", Type: clickhouse.NewBaseType("DateTime")},
			"customer_birth_date_datetime64": {Name: "customer_birth_date_datetime64", Type: clickhouse.NewBaseType("DateTime64")},
		},
		Name:   tableName,
		Config: clickhouse.NewDefaultCHConfig(),
	}

	currentSchema := schema.Schema{
		Fields:             nil,
		Aliases:            nil,
		ExistsInDataSource: false,
		DatabaseName:       "",
	}

	cw := ClickhouseQueryTranslator{Table: &table, Ctx: context.Background(), Schema: currentSchema}

	for i, test := range allAggregationTests() {
		t.Run(util.PrettyTestName(test.TestName, i), func(t *testing.T) {
			// sample_ecommerce
			if test.TestName == "TODO Top products this/last week(file:kibana-sample-data-ecommerce,nr:9)" {
				t.Skip("works IRL, need to update test's schema. It's already WIP https://github.com/QuesmaOrg/quesma/pull/1255. Let's wait for merge.")
			}

			if filters(test.TestName) {
				t.Skip("Fix filters")
			}

			if test.TestName == "Line, Y-axis: Min, Buckets: Date Range, X-Axis: Terms, Split Chart: Date Histogram(file:kibana-visualize/agg_req,nr:9)" {
				t.Skip("Date range is broken, fix in progress (PR #971)")
			}

			if test.TestName == "Terms with order by top metrics(file:kibana-visualize/agg_req,nr:8)" {
				t.Skip("Need to implement order by top metrics (talk with Jacek, he has an idea)")
			}

			if test.TestName == "max_bucket. Reproduce: Visualize -> Line: Metrics: Max Bucket (Bucket: Filters, Metric: Sum)(file:opensearch-visualize/pipeline_agg_req,nr:20)" ||
				test.TestName == "complex max_bucket. Reproduce: Visualize -> Line: Metrics: Max Bucket (Bucket: Filters, Metric: Sum), Buckets: Split chart: Rows -> Range(file:opensearch-visualize/pipeline_agg_req,nr:21)" {
				t.Skip("Was skipped before. Wrong key in max_bucket, should be an easy fix")
			}

			// TODO: add test for filter(s) both at the beginning and end of aggregation tree

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
			acceptableDifference := []string{"probability", "seed", bucket_aggregations.OriginalKeyName, "_id", "_score",
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
	// to be deleted after pancakes
	t2 := testName == "clients/kunkka/test_0, used to be broken before aggregations merge fix"+
		"Output more or less works, but is different and worse than what Elastic returns."+
		"If it starts failing, maybe that's a good thing(file:clients/kunkka,nr:0)"
	// below test is replacing it
	// testName == "it's the same input as in previous test, but with the original output from Elastic."+
	//	"Skipped for now, as our response is different in 2 things: key_as_string date (probably not important) + we don't return 0's (e.g. doc_count: 0)."+
	//	"If we need clients/kunkka/test_0, used to be broken before aggregations merge fix"
	return t1 || t2
}

// TODO remove after fix
func filters(testName string) bool {
	// this works, but is very suboptimal and didn't update the test case
	t1 := testName == "clients/kunkka/test_1, used to be broken before aggregations merge fix(file:clients/kunkka,nr:2)" // multi level filters
	return t1
}

func TestPancakeQueryGeneration_halfpancake(t *testing.T) {

	debug := false

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

	currentSchema := schema.NewSchema(
		map[schema.FieldName]schema.Field{
			"host.name":   {PropertyName: "host.name", InternalPropertyName: "host.name", Type: schema.QuesmaTypeObject},
			"bytes_gauge": {PropertyName: "bytes_gauge", InternalPropertyName: "bytes_gauge", Type: schema.QuesmaTypeInteger},
		}, true, "",
	)

	cw := ClickhouseQueryTranslator{Table: &table, Ctx: context.Background(), Schema: currentSchema}

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

	for i, tt := range tests {
		t.Run(util.PrettyTestName(tt.name, i), func(t *testing.T) {

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
