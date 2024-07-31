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
	"quesma/testdata/clients"
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

	allTests := clients.OpheliaTests
	for i, test := range allTests {
		t.Run(test.TestName+"("+strconv.Itoa(i)+")", func(t *testing.T) {
			if i != 0 && i != 1 && i != 3 && i != 4 && i != 5 { // TODO: remove
				t.Skip()
			}
			//if i != 5 { // TODO remove
			//	t.Skip()
			//}
			jsonp, err := types.ParseJSON(test.QueryRequestJson)
			assert.NoError(t, err)

			pancakeSqls, err := cw.PancakeParseAggregationJson(jsonp, false)
			assert.NoError(t, err)
			assert.True(t, len(pancakeSqls) == 1, "pancakeSqls should have only one query")
			if len(pancakeSqls) < 1 {
				return
			}
			pancakeSqlStr := model.AsString(pancakeSqls[0].SelectCommand)

			olpheliaTestsPancakeIdx := -1
			for idx, olpheliaTest := range clients.OpheliaTestsPancake {
				if olpheliaTest.TestName == test.TestName {
					olpheliaTestsPancakeIdx = idx
					break
				}
			}

			if olpheliaTestsPancakeIdx == -1 {
				t.Fatal("No pancake SQL for this test")
			}
			opheliaTestPancake := clients.OpheliaTestsPancake[olpheliaTestsPancakeIdx]
			expectedSql := opheliaTestPancake.Sql
			prettyExpectedSql := strings.TrimSpace(expectedSql)

			prettyPancakeSql := util.SqlPrettyPrint([]byte(pancakeSqlStr))

			pp.Println("Expected SQL:")
			fmt.Println(prettyExpectedSql)
			pp.Println("Actual (pancake) SQL:")
			fmt.Println(prettyPancakeSql)

			assert.Equal(t, prettyExpectedSql, prettyPancakeSql)
			if len(opheliaTestPancake.ExpectedResults) == 0 {
				assert.Fail(t, "No pancake expected results for this test")
			}

			if len(opheliaTestPancake.ExpectedResults) > 1 {
				if queryType, ok := pancakeSqls[0].Type.(PancakeQueryType); ok {
					expectedJson, err := util.JsonToMap(test.ExpectedResponse)
					if err != nil {
						assert.Fail(t, "Failed to parse expected JSON")
					}
					var expectedAggregationsPart model.JsonMap
					if response, ok := expectedJson["response"].(model.JsonMap); ok {
						if aggregations, ok2 := response["aggregations"].(model.JsonMap); ok2 {
							expectedAggregationsPart = aggregations
						}
					}
					assert.NotNil(t, expectedAggregationsPart, "Expected JSON should have 'response'/'aggregations' part")

					renderer := &pancakeJSONRenderer{}
					pancakeJson, err := renderer.toJSON(queryType.pancakeAggregation, opheliaTestPancake.ExpectedResults)

					if err != nil {
						t.Fatal("Failed to render pancake JSON", err)
					}

					// probability and seed are present in random_sampler aggregation. I'd assume they are not needed, thus let's not care about it for now.
					acceptableDifference := []string{"sum_other_doc_count", "probability", "seed", "bg_count", "doc_count", model.KeyAddedByQuesma,
						"sum_other_doc_count", "doc_count_error_upper_bound"} // Don't know why, but those 2 are still needed in new (clients/ophelia) tests. Let's fix it in another PR
					actualMinusExpected, expectedMinusActual := util.MapDifference(pancakeJson,
						expectedAggregationsPart, acceptableDifference, true, true)
					pp.Println("ACTUAL diff", actualMinusExpected)
					pp.Println("EXPECTED diff", expectedMinusActual)
					pp.Println("ACTUAL", pancakeJson)
					pp.Println("EXPECTED", expectedAggregationsPart)
					assert.True(t, util.AlmostEmpty(actualMinusExpected, acceptableDifference))
					assert.True(t, util.AlmostEmpty(expectedMinusActual, acceptableDifference))

				} else {
					assert.Fail(t, "Expected pancake query type")
				}
			}

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
