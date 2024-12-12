// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package queryparser

import (
	"context"
	"fmt"
	"quesma/clickhouse"
	"quesma/model"
	"quesma/model/typical_queries"
	"quesma/persistence"
	"quesma/quesma/config"
	"quesma/quesma/types"
	"quesma/schema"
	"quesma/telemetry"
	"quesma/testdata"
	"quesma/util"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TODO:
//  1. 14th test, "Query string". "(message LIKE '%%%' OR message LIKE '%logged%')", is it really
//     what should be? According to docs, I think so... Maybe test in Kibana?
//     OK, Kibana disagrees, it is indeed wrong.
func TestQueryParserStringAttrConfig(t *testing.T) {
	tableName := "logs-generic-default"
	table, err := clickhouse.NewTable(`CREATE TABLE `+tableName+`
		( "message" String, "@timestamp" DateTime64(3, 'UTC') )
		ENGINE = Memory`,
		clickhouse.NewNoTimestampOnlyStringAttrCHConfig(),
	)
	if err != nil {
		t.Fatal(err)
	}
	cfg := config.QuesmaConfiguration{IndexConfig: map[string]config.IndexConfiguration{}}

	indexConfig := config.IndexConfiguration{
		Name: "logs-generic-default",
	}

	cfg.IndexConfig[indexConfig.Name] = indexConfig

	lm := clickhouse.NewEmptyLogManager(&cfg, nil, telemetry.NewPhoneHomeEmptyAgent(), clickhouse.NewTableDiscovery(&config.QuesmaConfiguration{}, nil, persistence.NewStaticJSONDatabase()))
	lm.AddTableIfDoesntExist(table)
	s := schema.StaticRegistry{
		Tables: map[schema.TableName]schema.Schema{
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
	cw := ClickhouseQueryTranslator{ClickhouseLM: lm, Table: table, Ctx: context.Background(), Config: &cfg, Schema: s.Tables[schema.TableName(tableName)]}

	for i, tt := range testdata.TestsSearch {
		t.Run(fmt.Sprintf("%s(%d)", tt.Name, i), func(t *testing.T) {
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
	table := clickhouse.Table{
		Name:   tableName,
		Config: clickhouse.NewDefaultCHConfig(),
		Cols: map[string]*clickhouse.Column{
			"-@timestamp":  {Name: "-@timestamp", Type: clickhouse.NewBaseType("DateTime64")},
			"message$*%:;": {Name: "message$*%:;", Type: clickhouse.NewBaseType("String")},
			"-@bytes":      {Name: "-@bytes", Type: clickhouse.NewBaseType("Int64")},
		},
		Created: true,
	}
	lm := clickhouse.NewEmptyLogManager(&config.QuesmaConfiguration{}, nil, telemetry.NewPhoneHomeEmptyAgent(), clickhouse.NewTableDiscovery(&config.QuesmaConfiguration{}, nil, persistence.NewStaticJSONDatabase()))
	lm.AddTableIfDoesntExist(&table)
	indexConfig := config.IndexConfiguration{
		Name: "logs-generic-default",
	}
	cfg := config.QuesmaConfiguration{IndexConfig: map[string]config.IndexConfiguration{}}

	cfg.IndexConfig[indexConfig.Name] = indexConfig
	s := schema.StaticRegistry{
		Tables: map[schema.TableName]schema.Schema{
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
	cw := ClickhouseQueryTranslator{ClickhouseLM: lm, Table: &table, Ctx: context.Background(), Config: &cfg, Schema: s.Tables[schema.TableName(tableName)]}

	for i, tt := range testdata.TestsSearchNoFullTextFields {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
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
	table, err := clickhouse.NewTable(`CREATE TABLE `+tableName+`
		( "message" String, "@timestamp" DateTime64(3, 'UTC') )
		ENGINE = Memory`,
		clickhouse.NewChTableConfigNoAttrs(),
	)
	if err != nil {
		t.Fatal(err)
	}
	indexConfig := config.IndexConfiguration{
		Name: "logs-generic-default",
	}
	cfg := config.QuesmaConfiguration{IndexConfig: map[string]config.IndexConfiguration{}}

	cfg.IndexConfig[indexConfig.Name] = indexConfig
	s := schema.StaticRegistry{
		Tables: map[schema.TableName]schema.Schema{
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
	lm := clickhouse.NewLogManager(util.NewSyncMapWith(tableName, table), &config.QuesmaConfiguration{})
	cw := ClickhouseQueryTranslator{ClickhouseLM: lm, Table: table, Ctx: context.Background(), Config: &cfg, Schema: s.Tables["logs-generic-default"]}
	for _, tt := range testdata.TestsSearchNoAttrs {
		t.Run(tt.Name, func(t *testing.T) {
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

// TODO this will be updated in the next PR
var tests = []string{
	`{
		"_source": {
			"excludes": []
		},
		"aggs": {
			"0": {
				"histogram": {
					"field": "FlightDelayMin",
					"interval": 1,
					"min_doc_count": 1
				}
			}
		},
		"fields": [
			{
				"field": "timestamp",
				"format": "date_time"
			}
		],
		"query": {
			"bool": {
				"filter": [
					{
						"range": {
							"timestamp": {
								"format": "strict_date_optional_time",
								"gte": "2024-02-02T13:47:16.029Z",
								"lte": "2024-02-09T13:47:16.029Z"
							}
						}
					}
				],
				"must": [],
				"must_not": [
					{
						"match_phrase": {
							"FlightDelayMin": {
								"query": 0
							}
						}
					}
				],
				"should": []
			}
		},
		"runtime_mappings": {
			"hour_of_day": {
				"script": {
					"source": "emit(doc['timestamp'].value.getHour());"
				},
				"type": "long"
			}
		},
		"script_fields": {},
		"size": 0,
		"stored_fields": [
			"*"
		],
		"track_total_hits": true
	}`,
	`{
		"_source": {
			"excludes": []
		},
		"aggs": {
			"0": {
				"aggs": {
					"1-bucket": {
						"filter": {
							"bool": {
								"filter": [
									{
										"bool": {
											"minimum_should_match": 1,
											"should": [
												{
													"match": {
														"FlightDelay": true
													}
												}
											]
										}
									}
								],
								"must": [],
								"must_not": [],
								"should": []
							}
						}
					},
					"3-bucket": {
						"filter": {
							"bool": {
								"filter": [
									{
										"bool": {
											"minimum_should_match": 1,
											"should": [
												{
													"match": {
														"Cancelled": true
													}
												}
											]
										}
									}
								],
								"must": [],
								"must_not": [],
								"should": []
							}
						}
					}
				},
				"terms": {
					"field": "OriginCityName",
					"order": {
						"_key": "asc"
					},
					"size": 1000
				}
			}
		},
		"fields": [
			{
				"field": "timestamp",
				"format": "date_time"
			}
		],
		"query": {
			"bool": {
				"filter": [
					{
						"range": {
							"timestamp": {
								"format": "strict_date_optional_time",
								"gte": "2024-02-02T13:47:16.029Z",
								"lte": "2024-02-09T13:47:16.029Z"
							}
						}
					}
				],
				"must": [],
				"must_not": [],
				"should": []
			}
		},
		"runtime_mappings": {
			"hour_of_day": {
				"script": {
					"source": "emit(doc['timestamp'].value.getHour());"
				},
				"type": "long"
			}
		},
		"script_fields": {},
		"size": 0,
		"stored_fields": [
			"*"
		],
		"track_total_hits": true
	}`,
}

// TODO this will be updated in the next PR
func TestNew(t *testing.T) {
	tableName := `"logs-generic-default"`
	table, err := clickhouse.NewTable(`CREATE TABLE `+tableName+`
		( "message" String, "timestamp" DateTime64(3, 'UTC') )
		ENGINE = Memory`,
		clickhouse.NewNoTimestampOnlyStringAttrCHConfig(),
	)
	if err != nil {
		t.Fatal(err)
	}
	lm := clickhouse.NewLogManager(util.NewSyncMapWith(tableName, table), &config.QuesmaConfiguration{})
	s := schema.StaticRegistry{
		Tables: map[schema.TableName]schema.Schema{
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

	cw := ClickhouseQueryTranslator{ClickhouseLM: lm, Table: table, Ctx: context.Background(), Schema: s.Tables[schema.TableName("logs-generic-default")]}
	for _, tt := range tests {
		t.Run("test", func(t *testing.T) {
			simpleQuery, _, _ := cw.ParseQueryAsyncSearch(tt)
			assert.True(t, simpleQuery.CanParse)
		})
	}
}

func Test_parseSortFields(t *testing.T) {
	tests := []struct {
		name        string
		sortMap     any
		sortColumns []model.OrderByExpr
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
		},
		{
			name:        "empty",
			sortMap:     []any{},
			sortColumns: []model.OrderByExpr{},
		},
		{
			name: "map[string]string",
			sortMap: map[string]string{
				"timestamp": "desc",
				"_doc":      "desc",
			},
			sortColumns: []model.OrderByExpr{model.NewSortColumn("timestamp", model.DescOrder)},
		},
		{
			name: "map[string]interface{}",
			sortMap: map[string]interface{}{
				"timestamp": "desc",
				"_doc":      "desc",
			},
			sortColumns: []model.OrderByExpr{model.NewSortColumn("timestamp", model.DescOrder)},
		}, {
			name: "[]map[string]string",
			sortMap: []any{
				QueryMap{"@timestamp": "asc"},
				QueryMap{"_doc": "asc"},
			},
			sortColumns: []model.OrderByExpr{model.NewSortColumn("@timestamp", model.AscOrder)},
		},
	}
	table, _ := clickhouse.NewTable(`CREATE TABLE `+tableName+`
		( "@timestamp" DateTime64(3, 'UTC'), "service.name" String, "no_order_field" String, "_table_field_with_underscore" Int64 )
		ENGINE = Memory`,
		clickhouse.NewChTableConfigNoAttrs(),
	)
	lm := clickhouse.NewLogManager(util.NewSyncMapWith(tableName, table), &config.QuesmaConfiguration{})
	cw := ClickhouseQueryTranslator{ClickhouseLM: lm, Table: table, Ctx: context.Background()}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.sortColumns, cw.parseSortFields(tt.sortMap))
		})
	}
}

func TestInvalidQueryRequests(t *testing.T) {
	t.Skip("Test in the making. Need 1-2 more PRs in 'Report errors in queries better' series.")
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

	lm := clickhouse.NewLogManager(util.NewSyncMapWith(tableName, &table), &config.QuesmaConfiguration{})
	currentSchema := schema.Schema{
		Fields:             nil,
		Aliases:            nil,
		ExistsInDataSource: false,
		DatabaseName:       "",
	}

	cw := ClickhouseQueryTranslator{ClickhouseLM: lm, Table: &table, Ctx: context.Background(), Schema: currentSchema}

	for i, test := range testdata.InvalidAggregationTests {
		t.Run(test.TestName+"("+strconv.Itoa(i)+")", func(t *testing.T) {
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
