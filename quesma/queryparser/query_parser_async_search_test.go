// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package queryparser

import (
	"context"
	"github.com/stretchr/testify/assert"
	"quesma/clickhouse"
	"quesma/concurrent"
	"quesma/quesma/config"
	"quesma/schema"
	"quesma/testdata"
	"testing"
)

func TestQueryParserAsyncSearch(t *testing.T) {
	table := clickhouse.Table{
		Name:   tableName,
		Config: clickhouse.NewChTableConfigTimestampStringAttr(),
		Cols: map[string]*clickhouse.Column{
			"message": {
				Name:            "message",
				Type:            clickhouse.NewBaseType("String"),
				IsFullTextMatch: true,
			},
		},
		Created: true,
	}
	lm := clickhouse.NewLogManager(concurrent.NewMapWith(tableName, &table), config.QuesmaConfiguration{})
	s := staticRegistry{
		tables: map[schema.TableName]schema.Schema{
			"logs-generic-default": {
				Fields: map[schema.FieldName]schema.Field{
					"service.name":           {PropertyName: "service.name", InternalPropertyName: "service.name", Type: schema.TypeKeyword},
					"arrayOfArraysOfStrings": {PropertyName: "arrayOfArraysOfStrings", InternalPropertyName: "arrayOfArraysOfStrings", Type: schema.TypeKeyword},
					"arrayOfTuples":          {PropertyName: "arrayOfTuples", InternalPropertyName: "arrayOfTuples", Type: schema.TypeObject},
					"host.name":              {PropertyName: "host.name", InternalPropertyName: "host.name", Type: schema.TypeObject},
				},
			},
		},
	}
	cw := ClickhouseQueryTranslator{ClickhouseLM: lm, Table: &table, Ctx: context.Background(), SchemaRegistry: s}
	for _, tt := range testdata.TestsAsyncSearch {
		t.Run(tt.Name, func(t *testing.T) {
			query, queryInfo, _ := cw.ParseQueryAsyncSearch(tt.QueryJson)
			assert.True(t, query.CanParse)
			assert.Equal(t, tt.WantedParseResult, queryInfo)
		})
	}
}

// TODO this test doesn't work for now, as it's left for next (last) PR
func TestQueryParserAggregation(t *testing.T) {
	table := clickhouse.NewEmptyTable("tablename")
	lm := clickhouse.NewLogManager(concurrent.NewMapWith("tablename", table), config.QuesmaConfiguration{})
	s := staticRegistry{
		tables: map[schema.TableName]schema.Schema{
			"logs-generic-default": {
				Fields: map[schema.FieldName]schema.Field{
					"service.name":           {PropertyName: "service.name", InternalPropertyName: "service.name", Type: schema.TypeKeyword},
					"arrayOfArraysOfStrings": {PropertyName: "arrayOfArraysOfStrings", InternalPropertyName: "arrayOfArraysOfStrings", Type: schema.TypeKeyword},
					"arrayOfTuples":          {PropertyName: "arrayOfTuples", InternalPropertyName: "arrayOfTuples", Type: schema.TypeObject},
					"host.name":              {PropertyName: "host.name", InternalPropertyName: "host.name", Type: schema.TypeObject},
				},
			},
		},
	}
	cw := ClickhouseQueryTranslator{ClickhouseLM: lm, Table: table, Ctx: context.Background(), SchemaRegistry: s}
	for _, tt := range testdata.AggregationTests {
		t.Run(tt.TestName, func(t *testing.T) {
			cw.ParseQueryAsyncSearch(tt.QueryRequestJson)
			// fmt.Println(query, queryInfo)
			// assert.Equal(t, len(tt.WantedSqls), len(queries))
			// for i, wantedSql := range tt.WantedSqls {
			//	assert.Contains(t, wantedSql, queries[i].String())
			// }
		})
	}
}
