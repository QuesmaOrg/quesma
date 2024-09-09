// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package quesma

import (
	"context"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"quesma/ab_testing"
	"quesma/clickhouse"
	"quesma/concurrent"
	"quesma/logger"
	"quesma/model"
	"quesma/queryparser"
	"quesma/quesma/config"
	"quesma/quesma/types"
	"quesma/quesma/ui"
	"quesma/schema"
	"quesma/telemetry"
	"quesma/testdata"
	"quesma/util"
	"strconv"
	"testing"
)

func TestSearchOpensearch(t *testing.T) {

	cfg := config.QuesmaConfiguration{IndexConfig: map[string]config.IndexConfiguration{tableName: {}}}
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

	s := schema.StaticRegistry{Tables: map[schema.TableName]schema.Schema{}}

	s.Tables[tableName] = schema.Schema{
		Fields: map[schema.FieldName]schema.Field{
			"-@timestamp":  {PropertyName: "-@timestamp", InternalPropertyName: "-@timestamp", Type: schema.TypeDate},
			"message$*%:;": {PropertyName: "message$*%:;", InternalPropertyName: "message$*%:;", Type: schema.TypeText},
			"-@bytes":      {PropertyName: "-@bytes", InternalPropertyName: "-@bytes", Type: schema.TypeLong},
		},
	}

	for i, tt := range testdata.OpensearchSearchTests {
		t.Run(strconv.Itoa(i)+tt.Name, func(t *testing.T) {
			db, mock := util.InitSqlMockWithPrettyPrint(t, false)
			defer db.Close()
			lm := clickhouse.NewLogManagerWithConnection(db, concurrent.NewMapWith(tableName, &table))
			managementConsole := ui.NewQuesmaManagementConsole(&cfg, nil, nil, make(<-chan logger.LogWithLevel, 50000), telemetry.NewPhoneHomeEmptyAgent(), nil)
			cw := queryparser.ClickhouseQueryTranslator{ClickhouseLM: lm, Table: &table, Ctx: context.Background(), SchemaRegistry: s, Config: &cfg}

			body, parseErr := types.ParseJSON(tt.QueryJson)
			assert.NoError(t, parseErr)
			plan, err := cw.ParseQuery(body)
			queries := plan.Queries
			assert.NoError(t, err, "no ParseQuery error")
			assert.True(t, len(queries) > 0, "len queries > 0")
			whereClause := model.AsString(queries[0].SelectCommand.WhereClause)
			assert.Contains(t, tt.WantedSql, whereClause, "contains wanted sql")

			for _, wantedRegex := range tt.WantedRegexes {
				mock.ExpectQuery(testdata.EscapeBrackets(wantedRegex)).WillReturnRows(sqlmock.NewRows([]string{"@timestamp", "host.name"}))
			}
			queryRunner := NewQueryRunner(lm, &cfg, nil, managementConsole, s, ab_testing.NewEmptySender())
			_, err2 := queryRunner.handleSearch(ctx, tableName, types.MustJSON(tt.QueryJson))
			assert.NoError(t, err2)

			if err = mock.ExpectationsWereMet(); err != nil {
				t.Fatal("there were unfulfilled expections:", err)
			}
		})
	}
}

// TestHighlighter tests if
// * highlighted are only full text fields
// * when searching for "text-to-highlight" highlighted are only "(prefix-)text-to-highlight(-suffix)", and not some other results
func TestHighlighter(t *testing.T) {
	query := `{
		"_source": {
			"excludes": []
		},
		"docvalue_fields": [
			{
				"field": "@timestamp",
				"format": "date_time"
			}
		],
		"highlight": {
			"fields": {
				"*": {}
			},
			"fragment_size": 2147483647,
			"post_tags": [
				"@/opensearch-dashboards-highlighted-field@"
			],
			"pre_tags": [
				"@opensearch-dashboards-highlighted-field@"
			]
		},
		"query": {
			"bool": {
				"filter": [
					{
						"bool": {
							"filter": [
								{
									"bool": {
										"minimum_should_match": 1,
										"should": [
											{
												"range": {
													"@timestamp": {
														"gte": "2024-04-09T08:53:43.429Z",
														"lte": "2024-04-09T08:53:43.429Z",
														"time_zone": "Europe/Warsaw"
													}
												}
											}
										]
									}
								},
								{
									"bool": {
										"minimum_should_match": 1,
										"should": [
											{
												"match": {
													"host.name": "text-to-highlight"
												}
											}
										]
									}
								}
							]
						}
					},
					{
						"range": {
							"@timestamp": {
								"format": "strict_date_optional_time",
								"gte": "2024-04-08T17:57:04.681Z",
								"lte": "2024-04-09T08:57:04.681Z"
							}
						}
					}
				],
				"must": [],
				"must_not": [],
				"should": []
			}
		},
		"script_fields": {},
		"size": 10,
		"track_total_hits": false,
		"sort": [
			{
				"@timestamp": {
					"order": "desc",
					"unmapped_type": "boolean"
				}
			}
		],
		"stored_fields": [
			"*"
		],
		"version": true
	}`
	cfg := config.QuesmaConfiguration{IndexConfig: map[string]config.IndexConfiguration{tableName: {}}}
	table := clickhouse.Table{
		Name:   tableName,
		Config: clickhouse.NewDefaultCHConfig(),
		Cols: map[string]*clickhouse.Column{
			"message$*%:;": {Name: "message$*%:;", Type: clickhouse.NewBaseType("String")},
			"host.name":    {Name: "host.name", Type: clickhouse.NewBaseType("String")},
			"@timestamp":   {Name: "@timestamp", Type: clickhouse.NewBaseType("DateTime64")},
		},
		Created: true,
	}
	s := schema.StaticRegistry{
		Tables: map[schema.TableName]schema.Schema{
			tableName: {
				Fields: map[schema.FieldName]schema.Field{
					"messeage$*%:;": {PropertyName: "message$*%:;", InternalPropertyName: "message$*%:;", Type: schema.TypeText},
					"host.name":     {PropertyName: "host.name", InternalPropertyName: "host.name", Type: schema.TypeObject},
					"@timestamp":    {PropertyName: "@timestamp", InternalPropertyName: "@timestamp", Type: schema.TypeDate},
				},
			},
		},
	}

	db, mock := util.InitSqlMockWithPrettyPrint(t, true)
	defer db.Close()
	lm := clickhouse.NewLogManagerWithConnection(db, concurrent.NewMapWith(tableName, &table))
	managementConsole := ui.NewQuesmaManagementConsole(&cfg, nil, nil, make(<-chan logger.LogWithLevel, 50000), telemetry.NewPhoneHomeEmptyAgent(), nil)

	mock.ExpectQuery("").WillReturnRows(sqlmock.NewRows([]string{"message$*%:;", "host.name", "@timestamp"}). // careful, it's not always in this order, order is nondeterministic
															AddRow("abcd", "abcd", "abcd").
															AddRow("prefix-text-to-highlight", "prefix-text-to-highlight", "prefix-text-to-highlight").
															AddRow("text-to-highlight-suffix", "text-to-highlight-suffix", "text-to-highlight-suffix").
															AddRow("text-to-highlight", "text-to-highlight", "text-to-highlight").
															AddRow("text", "text", "text"))

	queryRunner := NewQueryRunner(lm, &cfg, nil, managementConsole, s, ab_testing.NewEmptySender())
	response, err := queryRunner.handleSearch(ctx, tableName, types.MustJSON(query))
	assert.NoError(t, err)
	if err != nil {
		t.Fatal(err)
	}

	if err = mock.ExpectationsWereMet(); err != nil {
		t.Fatal("there were unfulfilled expections:", err)
	}

	responseAsMap, err := util.JsonToMap(string(response))
	assert.NoError(t, err)

	getIthHighlight := func(i int) model.JsonMap {
		return responseAsMap["hits"].(model.JsonMap)["hits"].([]interface{})[i].(model.JsonMap)["highlight"].(model.JsonMap)
	}

	assert.Equal(t, model.JsonMap{"host.name": []any{}}, getIthHighlight(0)) // no highlight
	assert.Equal(t, model.JsonMap{
		"host.name": []any{"@opensearch-dashboards-highlighted-field@text-to-highlight@/opensearch-dashboards-highlighted-field@"},
	}, getIthHighlight(1))
	assert.Equal(t, model.JsonMap{
		"host.name": []any{"@opensearch-dashboards-highlighted-field@text-to-highlight@/opensearch-dashboards-highlighted-field@"},
	}, getIthHighlight(2))
	assert.Equal(t, model.JsonMap{
		"host.name": []any{"@opensearch-dashboards-highlighted-field@text-to-highlight@/opensearch-dashboards-highlighted-field@"},
	}, getIthHighlight(3))
	assert.Equal(t, model.JsonMap{"host.name": []any{}}, getIthHighlight(4)) // no highlight
}
