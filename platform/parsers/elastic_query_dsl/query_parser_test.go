// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package elastic_query_dsl

import (
	"context"
	"fmt"
	"github.com/QuesmaOrg/quesma/platform/config"
	"github.com/QuesmaOrg/quesma/platform/database_common"
	"github.com/QuesmaOrg/quesma/platform/logger"
	"github.com/QuesmaOrg/quesma/platform/model"
	"github.com/QuesmaOrg/quesma/platform/model/typical_queries"
	"github.com/QuesmaOrg/quesma/platform/persistence"
	"github.com/QuesmaOrg/quesma/platform/schema"
	"github.com/QuesmaOrg/quesma/platform/testdata"
	"github.com/QuesmaOrg/quesma/platform/types"
	"github.com/QuesmaOrg/quesma/platform/util"
	"github.com/QuesmaOrg/quesma/platform/v2/core/diag"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TODO:
//  1. 14th test, "Query string". "(message LIKE '%%%' OR message LIKE '%logged%')", is it really
//     what should be? According to docs, I think so... Maybe test in Kibana?
//     OK, Kibana disagrees, it is indeed wrong.
func TestQueryParserStringAttrConfig(t *testing.T) {
	logger.InitSimpleLoggerForTestsWarnLevel()
	tableName := "logs-generic-default"
	table := database_common.Table{
		Name: tableName,
		Cols: map[string]*database_common.Column{
			"message":           {Name: "message", Type: database_common.NewBaseType("String")},
			"@timestamp":        {Name: "@timestamp", Type: database_common.NewBaseType("DateTime64")},
			"tsAsUInt64":        {Name: "tsAsUInt64", Type: database_common.NewBaseType("UInt64")},
			"attributes_values": {Name: "attributes_values", Type: database_common.NewBaseType("Map(String,String)")},
		},
		Config: database_common.NewNoTimestampOnlyStringAttrCHConfig(),
	}
	cfg := config.QuesmaConfiguration{IndexConfig: map[string]config.IndexConfiguration{}}

	cfg.IndexConfig["logs-generic-default"] = config.IndexConfiguration{}

	lm := database_common.NewEmptyLogManager(&cfg, nil, diag.NewPhoneHomeEmptyAgent(), database_common.NewTableDiscovery(&config.QuesmaConfiguration{}, nil, persistence.NewStaticJSONDatabase()))
	lm.AddTableIfDoesntExist(&table)
	s := schema.StaticRegistry{
		Tables: map[schema.IndexName]schema.Schema{
			"logs-generic-default": {
				Fields: map[schema.FieldName]schema.Field{
					"host.name":         {PropertyName: "host.name", InternalPropertyName: "host.name", Type: schema.QuesmaTypeObject},
					"type":              {PropertyName: "type", InternalPropertyName: "type", Type: schema.QuesmaTypeText},
					"task.enabled":      {PropertyName: "task.enabled", InternalPropertyName: "task_enabled", Type: schema.QuesmaTypeBoolean},
					"name":              {PropertyName: "name", InternalPropertyName: "name", Type: schema.QuesmaTypeText},
					"content":           {PropertyName: "content", InternalPropertyName: "content", Type: schema.QuesmaTypeText},
					"message":           {PropertyName: "message", InternalPropertyName: "message", Type: schema.QuesmaTypeText},
					"host_name.keyword": {PropertyName: "host_name.keyword", InternalPropertyName: "host_name.keyword", Type: schema.QuesmaTypeKeyword},
					"FlightDelay":       {PropertyName: "FlightDelay", InternalPropertyName: "FlightDelay", Type: schema.QuesmaTypeText},
					"Cancelled":         {PropertyName: "Cancelled", InternalPropertyName: "Cancelled", Type: schema.QuesmaTypeText},
					"FlightDelayMin":    {PropertyName: "FlightDelayMin", InternalPropertyName: "FlightDelayMin", Type: schema.QuesmaTypeText},
					"_id":               {PropertyName: "_id", InternalPropertyName: "_id", Type: schema.QuesmaTypeText},
					"tsAsUInt64":        {PropertyName: "tsAsUInt64", InternalPropertyName: "tsAsUInt64", Type: schema.QuesmaTypeInteger},
				},
			},
		},
	}
	cw := ClickhouseQueryTranslator{Table: &table, Ctx: context.Background(), Schema: s.Tables[schema.IndexName(tableName)]}

	for i, tt := range testdata.TestsSearch {
		t.Run(util.PrettyTestName(tt.Name, i), func(t *testing.T) {
			body, parseErr := types.ParseJSON(tt.QueryJson)
			assert.NoError(t, parseErr)
			plan, errQuery := cw.ParseQuery(body)
			queries := plan.Queries
			assert.NoError(t, errQuery, "no ParseQuery error")
			assert.True(t, len(queries) > 0, "len queries > 0")
			var simpleListQuery *model.Query
			for _, query := range queries {
				if _, hasHits := query.Type.(*typical_queries.Hits); hasHits && query.SelectCommand.IsWildcard() {
					simpleListQuery = query
				}
			}
			for _, query := range queries {
				util.AssertContainsSqlEqual(t, tt.WantedSql, model.AsString(query.SelectCommand.WhereClause))
			}
			if simpleListQuery != nil {
				assert.Equal(t, model.NewTableRef(testdata.TableName), simpleListQuery.SelectCommand.FromClause)
				assert.Equal(t, []model.Expr{model.NewWildcardExpr}, simpleListQuery.SelectCommand.Columns)
			}
		})
	}
}

func TestQueryParserNoFullTextFields(t *testing.T) {
	table := database_common.Table{
		Name:   tableName,
		Config: database_common.NewDefaultCHConfig(),
		Cols: map[string]*database_common.Column{
			"-@timestamp":  {Name: "-@timestamp", Type: database_common.NewBaseType("DateTime64")},
			"message$*%:;": {Name: "message$*%:;", Type: database_common.NewBaseType("String")},
			"-@bytes":      {Name: "-@bytes", Type: database_common.NewBaseType("Int64")},
		},
	}
	lm := database_common.NewEmptyLogManager(&config.QuesmaConfiguration{}, nil, diag.NewPhoneHomeEmptyAgent(), database_common.NewTableDiscovery(&config.QuesmaConfiguration{}, nil, persistence.NewStaticJSONDatabase()))
	lm.AddTableIfDoesntExist(&table)
	cfg := config.QuesmaConfiguration{IndexConfig: map[string]config.IndexConfiguration{}}

	cfg.IndexConfig["logs-generic-default"] = config.IndexConfiguration{}
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
	cw := ClickhouseQueryTranslator{Table: &table, Ctx: context.Background(), Schema: s.Tables[schema.IndexName(tableName)]}

	for i, tt := range testdata.TestsSearchNoFullTextFields {
		t.Run(util.PrettyTestName(tt.Name, i), func(t *testing.T) {
			body, parseErr := types.ParseJSON(tt.QueryJson)
			assert.NoError(t, parseErr)
			plan, errQuery := cw.ParseQuery(body)
			queries := plan.Queries
			assert.NoError(t, errQuery, "no error in ParseQuery")
			assert.True(t, len(queries) > 0, "len queries > 0")
			whereClause := model.AsString(queries[0].SelectCommand.WhereClause)
			assert.Contains(t, tt.WantedSql, whereClause, "contains wanted sql")

			var simpleListQuery *model.Query
			for _, query := range queries {
				if _, hasHits := query.Type.(*typical_queries.Hits); hasHits {
					simpleListQuery = query
				}
			}

			for _, wantedSQL := range tt.WantedSql {
				assert.Contains(t, whereClause, wantedSQL, "query contains wanted sql")
			}
			if simpleListQuery != nil {
				assert.Equal(t, model.NewTableRef(testdata.TableName), simpleListQuery.SelectCommand.FromClause)
				assert.Equal(t, []model.Expr{model.NewWildcardExpr}, simpleListQuery.SelectCommand.Columns)
			}
		})
	}
}

// TODO this test gives wrong results??
func TestQueryParserNoAttrsConfig(t *testing.T) {
	tableName := "logs-generic-default"
	table := database_common.Table{
		Name: tableName,
		Cols: map[string]*database_common.Column{
			"message":           {Name: "message", Type: database_common.NewBaseType("String")},
			"@timestamp":        {Name: "@timestamp", Type: database_common.NewBaseType("DateTime64")},
			"attributes_values": {Name: "attributes_values", Type: database_common.NewBaseType("Map(String,String)")},
		},
		Config: database_common.NewChTableConfigNoAttrs(),
	}
	cfg := config.QuesmaConfiguration{IndexConfig: map[string]config.IndexConfiguration{}}

	cfg.IndexConfig[tableName] = config.IndexConfiguration{}
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
	cw := ClickhouseQueryTranslator{Table: &table, Ctx: context.Background(), Schema: s.Tables["logs-generic-default"]}
	for i, tt := range testdata.TestsSearchNoAttrs {
		t.Run(util.PrettyTestName(tt.Name, i), func(t *testing.T) {
			body, parseErr := types.ParseJSON(tt.QueryJson)
			assert.NoError(t, parseErr)
			plan, errQuery := cw.ParseQuery(body)
			queries := plan.Queries
			assert.NoError(t, errQuery, "no error in ParseQuery")
			assert.True(t, len(queries) > 0, "len queries > 0")
			whereClause := model.AsString(queries[0].SelectCommand.WhereClause)
			assert.Contains(t, tt.WantedSql, whereClause)

			var simpleListQuery *model.Query
			for _, query := range queries {
				if _, hasHits := query.Type.(*typical_queries.Hits); hasHits {
					simpleListQuery = query
				}
			}

			if simpleListQuery != nil {
				assert.Equal(t, model.NewTableRef(testdata.TableName), simpleListQuery.SelectCommand.FromClause)
				assert.Equal(t, []model.Expr{model.NewWildcardExpr}, simpleListQuery.SelectCommand.Columns)
			}
		})
	}
}

func Test_parseSortFields(t *testing.T) {
	tests := []struct {
		name           string
		sortMap        any
		sortColumns    []model.OrderByExpr
		sortFieldNames []string
	}{
		{
			name: "compound",
			sortMap: []any{
				QueryMap{"@timestamp": QueryMap{"format": "strict_date_optional_time", "order": "desc", "unmapped_type": "boolean"}},
				QueryMap{"service.name": QueryMap{"order": "asc", "unmapped_type": "boolean"}},
				QueryMap{"no_order_field": QueryMap{"unmapped_type": "boolean"}},
				QueryMap{"_table_field_with_underscore": QueryMap{"order": "asc", "unmapped_type": "boolean"}}, // this should be accepted, as it exists in the table
				QueryMap{"_doc": QueryMap{"order": "desc", "unmapped_type": "boolean"}},                        // this should be discarded, as it doesn't exist in the table
			},
			sortColumns: []model.OrderByExpr{
				model.NewSortColumn("@timestamp", model.DescOrder),
				model.NewSortColumn("service.name", model.AscOrder),
				model.NewSortColumn("no_order_field", model.AscOrder),
				model.NewSortColumn("_table_field_with_underscore", model.AscOrder),
			},
			sortFieldNames: []string{"@timestamp", "service.name", "no_order_field", "_table_field_with_underscore", "_doc"},
		},
		{
			name:           "empty",
			sortMap:        []any{},
			sortColumns:    []model.OrderByExpr{},
			sortFieldNames: []string{},
		},
		{
			name: "map[string]string",
			sortMap: map[string]string{
				"timestamp": "desc",
				"_doc":      "desc",
			},
			sortColumns:    []model.OrderByExpr{model.NewSortColumn("timestamp", model.DescOrder)},
			sortFieldNames: []string{"timestamp", "_doc"},
		},
		{
			name: "map[string]interface{}",
			sortMap: map[string]interface{}{
				"timestamp": "desc",
				"_doc":      "desc",
			},
			sortColumns:    []model.OrderByExpr{model.NewSortColumn("timestamp", model.DescOrder)},
			sortFieldNames: []string{"timestamp", "_doc"},
		}, {
			name: "[]map[string]string",
			sortMap: []any{
				QueryMap{"@timestamp": "asc"},
				QueryMap{"_doc": "asc"},
			},
			sortColumns:    []model.OrderByExpr{model.NewSortColumn("@timestamp", model.AscOrder)},
			sortFieldNames: []string{"@timestamp", "_doc"},
		},
	}
	table := database_common.Table{
		Name: tableName,
		Cols: map[string]*database_common.Column{
			"@timestamp":                   {Name: "@timestamp", Type: database_common.NewBaseType("DateTime64")},
			"service.name":                 {Name: "service.name", Type: database_common.NewBaseType("String")},
			"no_order_field":               {Name: "no_order_field", Type: database_common.NewBaseType("String")},
			"_table_field_with_underscore": {Name: "_table_field_with_underscore", Type: database_common.NewBaseType("Int64")},
		},
		Config: database_common.NewChTableConfigNoAttrs(),
	}
	cw := ClickhouseQueryTranslator{Table: &table, Ctx: context.Background()}
	for i, tt := range tests {
		t.Run(util.PrettyTestName(tt.name, i), func(t *testing.T) {
			orderBy, sortFieldNames := cw.parseSortFields(tt.sortMap)
			assert.Equal(t, tt.sortColumns, orderBy)
			assert.ElementsMatch(t, tt.sortFieldNames, sortFieldNames)
		})
	}
}

func TestInvalidQueryRequests(t *testing.T) {
	t.Skip("Test in the making. Need 1-2 more PRs in 'Report errors in queries better' series.")
	table := database_common.Table{
		Cols: map[string]*database_common.Column{
			"@timestamp":                     {Name: "@timestamp", Type: database_common.NewBaseType("DateTime64")},
			"timestamp":                      {Name: "timestamp", Type: database_common.NewBaseType("DateTime64")},
			"order_date":                     {Name: "order_date", Type: database_common.NewBaseType("DateTime64")},
			"message":                        {Name: "message", Type: database_common.NewBaseType("String")},
			"bytes_gauge":                    {Name: "bytes_gauge", Type: database_common.NewBaseType("UInt64")},
			"customer_birth_date":            {Name: "customer_birth_date", Type: database_common.NewBaseType("DateTime")},
			"customer_birth_date_datetime64": {Name: "customer_birth_date_datetime64", Type: database_common.NewBaseType("DateTime64")},
		},
		Name:   tableName,
		Config: database_common.NewDefaultCHConfig(),
	}

	currentSchema := schema.Schema{
		Fields:             nil,
		Aliases:            nil,
		ExistsInDataSource: false,
		DatabaseName:       "",
	}

	cw := ClickhouseQueryTranslator{Table: &table, Ctx: context.Background(), Schema: currentSchema}

	for i, test := range testdata.InvalidAggregationTests {
		t.Run(util.PrettyTestName(test.TestName, i), func(t *testing.T) {
			if strings.Contains(strings.ToLower(test.TestName), "rate") {
				t.Skip("Unskip after merge of rate aggregation")
			}
			fmt.Println("i:", i, "test:", test.TestName)

			jsonp, err := types.ParseJSON(test.QueryRequestJson)
			assert.NoError(t, err)

			_, err = cw.PancakeParseAggregationJson(jsonp, false)
			assert.Error(t, err)
		})
	}
}
