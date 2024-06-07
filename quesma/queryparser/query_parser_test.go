package queryparser

import (
	"context"
	"mitmproxy/quesma/clickhouse"
	"mitmproxy/quesma/concurrent"
	"mitmproxy/quesma/model"
	"mitmproxy/quesma/queryparser/aexp"
	"mitmproxy/quesma/quesma/config"
	"mitmproxy/quesma/quesma/types"
	"mitmproxy/quesma/telemetry"
	"mitmproxy/quesma/testdata"
	"strconv"
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

	tsField := "@timestamp"
	indexConfig := config.IndexConfiguration{
		Name:           "logs-generic-default",
		Enabled:        true,
		FullTextFields: []string{"message"},
		TimestampField: &tsField,
	}

	cfg.IndexConfig[indexConfig.Name] = indexConfig

	lm := clickhouse.NewEmptyLogManager(cfg, nil, telemetry.NewPhoneHomeEmptyAgent())
	lm.AddTableIfDoesntExist(table)

	cw := ClickhouseQueryTranslator{ClickhouseLM: lm, Table: table, Ctx: context.Background()}

	for i, tt := range testdata.TestsSearch {
		t.Run(strconv.Itoa(i)+tt.Name, func(t *testing.T) {
			body, parseErr := types.ParseJSON(tt.QueryJson)
			assert.NoError(t, parseErr)
			simpleQuery, queryInfo, _, _ := cw.ParseQueryInternal(body)
			assert.True(t, simpleQuery.CanParse, "can parse")
			whereStmt := simpleQuery.WhereClauseAsString()
			assert.Contains(t, tt.WantedSql, whereStmt, "contains wanted sql")
			assert.Equal(t, tt.WantedQueryType, queryInfo.Typ, "equals to wanted query type")
			size := model.DefaultSizeListQuery
			if queryInfo.Size != 0 {
				size = queryInfo.Size
			}
			query := cw.BuildNRowsQuery("*", simpleQuery, size)

			for _, wantedSQL := range tt.WantedSql {
				assert.Contains(t, query.String(context.Background()), wantedSQL, "query contains wanted sql")
			}
			assert.True(t, query.CanParse, "can parse")
			assert.Equal(t, model.NewSelectColumnString(strconv.Quote(testdata.TableName)), query.FromClause)
			assert.Equal(t, []model.SelectColumn{{Expression: aexp.Wildcard}}, query.Columns)
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
	lm := clickhouse.NewEmptyLogManager(config.QuesmaConfiguration{}, nil, telemetry.NewPhoneHomeEmptyAgent())
	lm.AddTableIfDoesntExist(&table)
	cw := ClickhouseQueryTranslator{ClickhouseLM: lm, Table: &table, Ctx: context.Background()}

	for i, tt := range testdata.TestsSearchNoFullTextFields {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			body, parseErr := types.ParseJSON(tt.QueryJson)
			assert.NoError(t, parseErr)
			simpleQuery, queryInfo, _, _ := cw.ParseQueryInternal(body)
			assert.True(t, simpleQuery.CanParse, "can parse")
			whereStmt := simpleQuery.WhereClauseAsString()
			assert.Contains(t, tt.WantedSql, whereStmt, "contains wanted sql")
			assert.Equal(t, tt.WantedQueryType, queryInfo.Typ, "equals to wanted query type")

			query := cw.BuildNRowsQuery("*", simpleQuery, model.DefaultSizeListQuery)
			for _, wantedSQL := range tt.WantedSql {
				assert.Contains(t, query.String(context.Background()), wantedSQL, "query contains wanted sql")
			}
			assert.True(t, query.CanParse, "can parse")
			assert.Equal(t, model.NewSelectColumnString(strconv.Quote(testdata.TableName)), query.FromClause)
			assert.Equal(t, []model.SelectColumn{{Expression: aexp.Wildcard}}, query.Columns)
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
	lm := clickhouse.NewLogManager(concurrent.NewMapWith(tableName, table), config.QuesmaConfiguration{})
	cw := ClickhouseQueryTranslator{ClickhouseLM: lm, Table: table, Ctx: context.Background()}
	for _, tt := range testdata.TestsSearchNoAttrs {
		t.Run(tt.Name, func(t *testing.T) {
			body, parseErr := types.ParseJSON(tt.QueryJson)
			assert.NoError(t, parseErr)
			simpleQuery, queryInfo, _, _ := cw.ParseQueryInternal(body)
			assert.True(t, simpleQuery.CanParse)
			whereStmt := simpleQuery.WhereClauseAsString()
			assert.Contains(t, tt.WantedSql, whereStmt)
			assert.Equal(t, tt.WantedQueryType, queryInfo.Typ)

			query := cw.BuildNRowsQuery("*", simpleQuery, model.DefaultSizeListQuery)

			for _, wantedSQL := range tt.WantedSql {
				assert.Contains(t, query.String(context.Background()), wantedSQL, "query contains wanted sql")
			}
			assert.True(t, query.CanParse, "can parse")
			assert.Equal(t, model.NewSelectColumnString(strconv.Quote(testdata.TableName)), query.FromClause)
			assert.Equal(t, []model.SelectColumn{{Expression: aexp.Wildcard}}, query.Columns)
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
	lm := clickhouse.NewLogManager(concurrent.NewMapWith(tableName, table), config.QuesmaConfiguration{})
	cw := ClickhouseQueryTranslator{ClickhouseLM: lm, Table: table, Ctx: context.Background()}
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
		sortColumns []model.SelectColumn
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
			sortColumns: []model.SelectColumn{
				model.NewSortColumn("@timestamp", true),
				model.NewSortColumn("service.name", false),
				model.NewSortColumn("no_order_field", false),
				model.NewSortColumn("_table_field_with_underscore", false),
			},
		},
		{
			name:        "empty",
			sortMap:     []any{},
			sortColumns: []model.SelectColumn{},
		},
		{
			name: "map[string]string",
			sortMap: map[string]string{
				"timestamp": "desc",
				"_doc":      "desc",
			},
			sortColumns: []model.SelectColumn{
				model.NewSortColumn("timestamp", true),
			},
		},
		{
			name: "map[string]interface{}",
			sortMap: map[string]interface{}{
				"timestamp": "desc",
				"_doc":      "desc",
			},
			sortColumns: []model.SelectColumn{
				model.NewSortColumn("timestamp", true),
			},
		}, {
			name: "[]map[string]string",
			sortMap: []any{
				QueryMap{"@timestamp": "asc"},
				QueryMap{"_doc": "asc"},
			},
			sortColumns: []model.SelectColumn{
				model.NewSortColumn("@timestamp", false),
			},
		},
	}
	table, _ := clickhouse.NewTable(`CREATE TABLE `+tableName+`
		( "@timestamp" DateTime64(3, 'UTC'), "service.name" String, "no_order_field" String, "_table_field_with_underscore" Int64 )
		ENGINE = Memory`,
		clickhouse.NewChTableConfigNoAttrs(),
	)
	lm := clickhouse.NewLogManager(concurrent.NewMapWith(tableName, table), config.QuesmaConfiguration{})
	cw := ClickhouseQueryTranslator{ClickhouseLM: lm, Table: table, Ctx: context.Background()}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.sortColumns, cw.parseSortFields(tt.sortMap))
		})
	}
}
