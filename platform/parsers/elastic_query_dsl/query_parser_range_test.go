// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package elastic_query_dsl

import (
	"context"
	"github.com/QuesmaOrg/quesma/platform/database_common"
	"github.com/QuesmaOrg/quesma/platform/schema"
	"github.com/QuesmaOrg/quesma/platform/util"
	"github.com/stretchr/testify/assert"
	"testing"
)

type parseRangeTest struct {
	name             string
	rangePartOfQuery QueryMap
	table            database_common.Table
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
		database_common.Table{
			Name: tableName,
			Cols: map[string]*database_common.Column{
				"message":   {Name: "message", Type: database_common.NewBaseType("String")},
				"timestamp": {Name: "timestamp", Type: database_common.NewBaseType("DateTime64")},
			},
			Config: database_common.NewNoTimestampOnlyStringAttrCHConfig(),
		},
		`("timestamp">=fromUnixTimestamp64Milli(1706881636029) AND "timestamp"<=fromUnixTimestamp64Milli(1707486436029))`,
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
		database_common.Table{
			Name: tableName,
			Cols: map[string]*database_common.Column{
				"message":   {Name: "message", Type: database_common.NewBaseType("String")},
				"timestamp": {Name: "timestamp", Type: database_common.NewBaseType("DateTime")},
			},
			Config: database_common.NewNoTimestampOnlyStringAttrCHConfig(),
		},
		`("timestamp">=fromUnixTimestamp(1706881636) AND "timestamp"<=fromUnixTimestamp(1707486436))`,
	},
	{
		"numeric range",
		QueryMap{
			"time_taken": QueryMap{
				"gt": "100",
			},
		},
		database_common.Table{
			Name: tableName,
			Cols: map[string]*database_common.Column{
				"message":    {Name: "message", Type: database_common.NewBaseType("String")},
				"time_taken": {Name: "time_taken", Type: database_common.NewBaseType("UInt32")},
			},
			Config: database_common.NewNoTimestampOnlyStringAttrCHConfig(),
		},
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
		database_common.Table{
			Name: tableName,
			Cols: map[string]*database_common.Column{
				"message":   {Name: "message", Type: database_common.NewBaseType("String")},
				"timestamp": {Name: "timestamp", Type: database_common.NewBaseType("DateTime64")},
			},
			Config: database_common.NewNoTimestampOnlyStringAttrCHConfig(),
		},
		`("timestamp">=fromUnixTimestamp64Milli(1706881636000) AND "timestamp"<=fromUnixTimestamp64Milli(1707486436000))`,
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
	for i, test := range parseRangeTests {
		t.Run(util.PrettyTestName(test.name, i), func(t *testing.T) {
			cw := ClickhouseQueryTranslator{Table: &test.table, Ctx: context.Background(), Schema: s.Tables[schema.IndexName(tableName)]}

			simpleQuery := cw.parseRange(test.rangePartOfQuery)
			assert.Equal(t, test.expectedWhere, simpleQuery.WhereClauseAsString())
		})
	}
}
