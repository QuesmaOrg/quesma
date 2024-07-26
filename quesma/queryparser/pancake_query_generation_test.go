// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package queryparser

import (
	"context"
	"github.com/stretchr/testify/assert"
	"quesma/clickhouse"
	"quesma/concurrent"
	"quesma/quesma/config"
	"quesma/quesma/types"
	"quesma/schema"
	"quesma/testdata/clients"
	"strconv"
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
			jsonp, err := types.ParseJSON(test.QueryRequestJson)
			assert.NoError(t, err)

			cw.PancakeParseAggregationJson(jsonp)
		})
	}
}
