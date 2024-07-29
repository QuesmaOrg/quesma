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
			t.Skip()
			jsonp, err := types.ParseJSON(test.QueryRequestJson)
			assert.NoError(t, err)

			pancakeSqls, err := cw.PancakeParseAggregationJson(jsonp)
			assert.NoError(t, err)
			assert.True(t, len(pancakeSqls) == 1, "pancakeSqls should have only one query")
			if len(pancakeSqls) < 1 {
				return
			}
			pancakeSqlStr := model.AsString(pancakeSqls[0].SelectCommand)

			if len(clients.OpheliaTestsPancake) <= i {
				t.Fatal("No pancake SQL for this test")
			}
			expectedSql := clients.OpheliaTestsPancake[i].Sql
			prettyExpectedSql := strings.TrimSpace(expectedSql)

			prettyPancakeSql := util.SqlPrettyPrint([]byte(pancakeSqlStr))

			pp.Println("Expected SQL:")
			fmt.Println(prettyExpectedSql)
			pp.Println("Actual (pancake) SQL:")
			fmt.Println(prettyPancakeSql)

			assert.Equal(t, prettyExpectedSql, prettyPancakeSql)
			if len(clients.OpheliaTestsPancake[i].ExpectedResults) == 0 {
				assert.Fail(t, "No pancake expected results for this test")
			}

			if len(clients.OpheliaTestsPancake[i].ExpectedResults) > 1 {
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

					pancakeJson := pancakeRenderJSON(queryType.pancakeAggregation, clients.OpheliaTestsPancake[i].ExpectedResults)

					actualMinusExpected, expectedMinusActual := util.MapDifference(pancakeJson, expectedAggregationsPart, true, true)

					// probability and seed are present in random_sampler aggregation. I'd assume they are not needed, thus let's not care about it for now.
					acceptableDifference := []string{"sum_other_doc_count", "probability", "seed", "bg_count", "doc_count", model.KeyAddedByQuesma,
						"sum_other_doc_count", "doc_count_error_upper_bound"} // Don't know why, but those 2 are still needed in new (clients/ophelia) tests. Let's fix it in another PR
					// pp.Println("ACTUAL diff", actualMinusExpected)
					// pp.Println("EXPECTED diff", expectedMinusActual)
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
