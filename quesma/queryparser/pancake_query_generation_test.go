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
			if i > 0 {
				t.Skip("Skipping all tests except the first one")
				return
			}
			jsonp, err := types.ParseJSON(test.QueryRequestJson)
			assert.NoError(t, err)

			pancakeSqls, err := cw.PancakeParseAggregationJson(jsonp)
			assert.NoError(t, err)
			assert.True(t, len(pancakeSqls) == 1, "pancakeSqls should have only one query")
			pancakeSqlStr := model.AsString(pancakeSqls[0].SelectCommand)

			expectedSql := test.ExpectedSQLs[0]
			// cheap trick to find the aggregation sql for now
			for _, alternativeExpectedSql := range test.ExpectedSQLs {
				if strings.Contains(alternativeExpectedSql, "cte_") {
					expectedSql = alternativeExpectedSql
					break
				}
			}

			pp.Println("Expected SQL:")
			fmt.Println(util.SqlPrettyPrint([]byte(expectedSql)))
			pp.Println("Actual (pancake) SQL:")
			fmt.Println(util.SqlPrettyPrint([]byte(pancakeSqlStr)))
			assert.Equal(t, expectedSql, pancakeSqlStr)
		})
	}
}
