// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package queryparser

import (
	"context"
	"github.com/QuesmaOrg/quesma/quesma/clickhouse"
	"github.com/QuesmaOrg/quesma/quesma/schema"
	"github.com/stretchr/testify/assert"
	"testing"
)

type parseRangeTest struct {
	name             string
	rangePartOfQuery QueryMap
	createTableQuery string
	expectedWhere    string
}

var parseRangeTests = []parseRangeTest{
	{
		"DateTime64",
		QueryMap{
			"timestamp": QueryMap{
				"format": "strict_date_optional_time",
				"gte":    "2024-02-02T13:47:16.029Z",
				"lte":    "2024-02-09T13:47:16.029Z",
			},
		},
		`CREATE TABLE ` + tableName + `
		( "message" String, "timestamp" DateTime64(3, 'UTC') )
		ENGINE = Memory`,
		`("timestamp">=__quesma_from_unix_timestamp_ms(1706881636029) AND "timestamp"<=__quesma_from_unix_timestamp_ms(1707486436029))`,
	},
	{
		"parseDateTimeBestEffort",
		QueryMap{
			"timestamp": QueryMap{
				"format": "strict_date_optional_time",
				"gte":    "2024-02-02T13:47:16.029Z",
				"lte":    "2024-02-09T13:47:16.029Z",
			},
		},
		`CREATE TABLE ` + tableName + `
		( "message" String, "timestamp" DateTime )
		ENGINE = Memory`,
		`("timestamp">=__quesma_from_unix_timestamp_ms(1706881636029) AND "timestamp"<=__quesma_from_unix_timestamp_ms(1707486436029))`,
	},
	{
		"numeric range",
		QueryMap{
			"time_taken": QueryMap{
				"gt": "100",
			},
		},
		`CREATE TABLE ` + tableName + `
		( "message" String, "timestamp" DateTime, "time_taken" UInt32 )
		ENGINE = Memory`,
		`"time_taken">100`,
	},
	{
		"DateTime64",
		QueryMap{
			"timestamp": QueryMap{
				"format": "strict_date_optional_time",
				"gte":    "2024-02-02T13:47:16",
				"lte":    "2024-02-09T13:47:16",
			},
		},
		`CREATE TABLE ` + tableName + `
		( "message" String, "timestamp" DateTime64(3, 'UTC') )
		ENGINE = Memory`,
		`("timestamp">=__quesma_from_unix_timestamp_ms(1706881636000) AND "timestamp"<=__quesma_from_unix_timestamp_ms(1707486436000))`,
	},
}

func Test_parseRange(t *testing.T) {
	s := schema.StaticRegistry{
		Tables: map[schema.IndexName]schema.Schema{
			"logs-generic-default": {
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
	for _, test := range parseRangeTests {
		t.Run(test.name, func(t *testing.T) {
			table, err := clickhouse.NewTable(test.createTableQuery, clickhouse.NewNoTimestampOnlyStringAttrCHConfig())
			if err != nil {
				t.Fatal(err)
			}
			assert.NoError(t, err)
			cw := ClickhouseQueryTranslator{Table: table, Ctx: context.Background(), Schema: s.Tables[schema.IndexName(tableName)]}

			simpleQuery := cw.parseRange(test.rangePartOfQuery)
			assert.Equal(t, test.expectedWhere, simpleQuery.WhereClauseAsString())
		})
	}
}
