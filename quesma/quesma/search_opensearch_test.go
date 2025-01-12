// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package quesma

import (
	"context"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"quesma/backend_connectors"
	"quesma/clickhouse"
	"quesma/model"
	"quesma/queryparser"
	"quesma/quesma/types"
	"quesma/schema"
	"quesma/testdata"
	"quesma/util"
	"strconv"
	"testing"
)

func TestSearchOpensearch(t *testing.T) {

	table := clickhouse.Table{
		Name:   tableName,
		Config: clickhouse.NewDefaultCHConfig(),
		Cols: map[string]*clickhouse.Column{
			"__timestamp":  {Name: "__timestamp", Type: clickhouse.NewBaseType("DateTime64")},
			"message_____": {Name: "message_____", Type: clickhouse.NewBaseType("String")},
			"__bytes":      {Name: "__bytes", Type: clickhouse.NewBaseType("Int64")},
		},
		Created: true,
	}

	s := &schema.StaticRegistry{Tables: map[schema.IndexName]schema.Schema{}}

	s.Tables[tableName] = schema.Schema{
		Fields: map[schema.FieldName]schema.Field{
			"-@timestamp":  {PropertyName: "-@timestamp", InternalPropertyName: "__timestamp", Type: schema.QuesmaTypeDate},
			"message$*%:;": {PropertyName: "message$*%:;", InternalPropertyName: "message_____", Type: schema.QuesmaTypeText},
			"-@bytes":      {PropertyName: "-@bytes", InternalPropertyName: "__bytes", Type: schema.QuesmaTypeLong},
		},
	}

	for i, tt := range testdata.OpensearchSearchTests {
		t.Run(strconv.Itoa(i)+tt.Name, func(t *testing.T) {
			conn, mock := util.InitSqlMockWithPrettySqlAndPrint(t, false)
			defer conn.Close()
			db := backend_connectors.NewClickHouseBackendConnectorWithConnection("", conn)

			queryRunner := NewQueryRunnerDefaultForTests(db, &DefaultConfig, tableName, util.NewSyncMapWith(tableName, &table), s)
			cw := queryparser.ClickhouseQueryTranslator{Table: &table, Ctx: context.Background(), Schema: s.Tables[tableName], Config: &DefaultConfig}

			body, parseErr := types.ParseJSON(tt.QueryJson)
			assert.NoError(t, parseErr)
			plan, err := cw.ParseQuery(body)
			queries := plan.Queries
			assert.NoError(t, err, "no ParseQuery error")
			assert.True(t, len(queries) > 0, "len queries > 0")
			whereClause := model.AsString(queries[0].SelectCommand.WhereClause)
			assert.Contains(t, tt.WantedSql, whereClause, "contains wanted sql")

			for _, wantedQuery := range tt.WantedQueries {
				mock.ExpectQuery(wantedQuery).WillReturnRows(sqlmock.NewRows([]string{"@timestamp", "host.name"}))
			}

			_, err2 := queryRunner.HandleSearch(ctx, tableName, types.MustJSON(tt.QueryJson))
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
	table := clickhouse.Table{
		Name:   tableName,
		Config: clickhouse.NewDefaultCHConfig(),
		Cols: map[string]*clickhouse.Column{
			"message_____": {Name: "message_____", Type: clickhouse.NewBaseType("String")},
			"host_name":    {Name: "host_name", Type: clickhouse.NewBaseType("String")},
			"_timestamp":   {Name: "_timestamp", Type: clickhouse.NewBaseType("DateTime64")},
		},
		Created: true,
	}
	fields := map[schema.FieldName]schema.Field{
		"messeage$*%:;": {PropertyName: "message$*%:;", InternalPropertyName: "message_____", Type: schema.QuesmaTypeText},
		"host.name":     {PropertyName: "host.name", InternalPropertyName: "host_name", Type: schema.QuesmaTypeObject},
		"@timestamp":    {PropertyName: "@timestamp", InternalPropertyName: "_timestamp", Type: schema.QuesmaTypeDate},
	}
	s := &schema.StaticRegistry{
		Tables: map[schema.IndexName]schema.Schema{
			tableName: schema.NewSchemaWithAliases(fields, map[schema.FieldName]schema.FieldName{}, true, "", nil),
		},
	}
	conn, mock := util.InitSqlMockWithPrettyPrint(t, true)
	defer conn.Close()
	db := backend_connectors.NewClickHouseBackendConnectorWithConnection("", conn)

	// careful, it's not always in this order, order is nondeterministic
	mock.ExpectQuery("").WillReturnRows(sqlmock.NewRows([]string{"message$*%:;", "host.name", "@timestamp"}).
		AddRow("abcd", "abcd", "abcd").
		AddRow("prefix-text-to-highlight", "prefix-text-to-highlight", "prefix-text-to-highlight").
		AddRow("text-to-highlight-suffix", "text-to-highlight-suffix", "text-to-highlight-suffix").
		AddRow("text-to-highlight", "text-to-highlight", "text-to-highlight").
		AddRow("text", "text", "text"))

	queryRunner := NewQueryRunnerDefaultForTests(db, &DefaultConfig, tableName, util.NewSyncMapWith(tableName, &table), s)
	response, err := queryRunner.HandleSearch(ctx, tableName, types.MustJSON(query))
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
		hits := responseAsMap["hits"].(model.JsonMap)["hits"]
		return hits.([]interface{})[i].(model.JsonMap)["highlight"].(model.JsonMap)
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
