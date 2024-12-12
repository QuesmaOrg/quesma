// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package queryparser

import (
	"context"
	"github.com/stretchr/testify/assert"
	"quesma/clickhouse"
	"quesma/quesma/config"
	"quesma/schema"
	"quesma/testdata"
	"quesma/util"
	"testing"
)

func TestQueryParserAsyncSearch(t *testing.T) {
	table := clickhouse.Table{
		Name:   tableName,
		Config: clickhouse.NewChTableConfigTimestampStringAttr(),
		Cols: map[string]*clickhouse.Column{
			"message": {
				Name: "message",
				Type: clickhouse.NewBaseType("String"),
			},
		},
		Created: true,
	}
	lm := clickhouse.NewLogManager(util.NewSyncMapWith(tableName, &table), &config.QuesmaConfiguration{})
	s := schema.StaticRegistry{
		Tables: map[schema.IndexName]schema.Schema{
			"logs-generic-default": {
				Fields: map[schema.FieldName]schema.Field{
					"host.name":         {PropertyName: "host.name", InternalPropertyName: "host.name", Type: schema.QuesmaTypeObject},
					"type":              {PropertyName: "type", InternalPropertyName: "type", Type: schema.QuesmaTypeText},
					"name":              {PropertyName: "name", InternalPropertyName: "name", Type: schema.QuesmaTypeText},
					"content":           {PropertyName: "content", InternalPropertyName: "content", Type: schema.QuesmaTypeText},
					"message":           {PropertyName: "message", InternalPropertyName: "message", Type: schema.QuesmaTypeText},
					"host.name.keyword": {PropertyName: "host.name.keyword", InternalPropertyName: "host.name.keyword", Type: schema.QuesmaTypeKeyword},
					"FlightDelay":       {PropertyName: "FlightDelay", InternalPropertyName: "FlightDelay", Type: schema.QuesmaTypeText},
					"Cancelled":         {PropertyName: "Cancelled", InternalPropertyName: "Cancelled", Type: schema.QuesmaTypeText},
					"FlightDelayMin":    {PropertyName: "FlightDelayMin", InternalPropertyName: "FlightDelayMin", Type: schema.QuesmaTypeText},
				},
			},
		},
	}

	cw := ClickhouseQueryTranslator{ClickhouseLM: lm, Table: &table, Ctx: context.Background(), Schema: s.Tables["logs-generic-default"]}
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
	lm := clickhouse.NewLogManager(util.NewSyncMapWith("tablename", table), &config.QuesmaConfiguration{})
	s := schema.StaticRegistry{
		Tables: map[schema.IndexName]schema.Schema{
			"tablename": {
				Fields: map[schema.FieldName]schema.Field{
					"host.name":         {PropertyName: "host.name", InternalPropertyName: "host.name", Type: schema.QuesmaTypeObject},
					"type":              {PropertyName: "type", InternalPropertyName: "type", Type: schema.QuesmaTypeText},
					"name":              {PropertyName: "name", InternalPropertyName: "name", Type: schema.QuesmaTypeText},
					"content":           {PropertyName: "content", InternalPropertyName: "content", Type: schema.QuesmaTypeText},
					"message":           {PropertyName: "message", InternalPropertyName: "message", Type: schema.QuesmaTypeText},
					"host_name.keyword": {PropertyName: "host_name.keyword", InternalPropertyName: "host_name.keyword", Type: schema.QuesmaTypeKeyword},
					"FlightDelay":       {PropertyName: "FlightDelay", InternalPropertyName: "FlightDelay", Type: schema.QuesmaTypeText},
					"Cancelled":         {PropertyName: "Cancelled", InternalPropertyName: "Cancelled", Type: schema.QuesmaTypeText},
					"FlightDelayMin":    {PropertyName: "FlightDelayMin", InternalPropertyName: "FlightDelayMin", Type: schema.QuesmaTypeText},
					"_id":               {PropertyName: "_id", InternalPropertyName: "_id", Type: schema.QuesmaTypeText},
				},
			},
		},
	}

	cw := ClickhouseQueryTranslator{ClickhouseLM: lm, Table: table, Ctx: context.Background(), Schema: s.Tables["tablename"]}
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
