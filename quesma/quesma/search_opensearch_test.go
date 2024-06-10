package quesma

import (
	"context"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"mitmproxy/quesma/clickhouse"
	"mitmproxy/quesma/concurrent"
	"mitmproxy/quesma/model"
	"mitmproxy/quesma/queryparser"
	"mitmproxy/quesma/quesma/config"
	"mitmproxy/quesma/quesma/types"
	"mitmproxy/quesma/quesma/ui"
	"mitmproxy/quesma/telemetry"
	"mitmproxy/quesma/testdata"
	"mitmproxy/quesma/tracing"
	"mitmproxy/quesma/util"
	"strconv"
	"testing"
)

func TestSearchOpensearch(t *testing.T) {
	cfg := config.QuesmaConfiguration{IndexConfig: map[string]config.IndexConfiguration{tableName: {Enabled: true}}}
	table := clickhouse.Table{
		Name:   tableName,
		Config: clickhouse.NewDefaultCHConfig(),
		Cols: map[string]*clickhouse.Column{
			"-@timestamp":  {Name: "-@timestamp", Type: clickhouse.NewBaseType("DateTime64")},
			"message$*%:;": {Name: "message$*%:;", Type: clickhouse.NewBaseType("String"), IsFullTextMatch: true},
			"-@bytes":      {Name: "-@bytes", Type: clickhouse.NewBaseType("Int64")},
		},
		Created: true,
	}

	for i, tt := range testdata.OpensearchSearchTests {
		t.Run(strconv.Itoa(i)+tt.Name, func(t *testing.T) {
			db, mock := util.InitSqlMockWithPrettyPrint(t)
			mock.MatchExpectationsInOrder(false)
			defer db.Close()
			lm := clickhouse.NewLogManagerWithConnection(db, concurrent.NewMapWith(tableName, &table))
			managementConsole := ui.NewQuesmaManagementConsole(cfg, nil, nil, make(<-chan tracing.LogWithLevel, 50000), telemetry.NewPhoneHomeEmptyAgent())
			cw := queryparser.ClickhouseQueryTranslator{ClickhouseLM: lm, Table: &table, Ctx: context.Background()}

			body, parseErr := types.ParseJSON(tt.QueryJson)
			assert.NoError(t, parseErr)
			simpleQuery, queryInfo, _, _ := cw.ParseQueryInternal(body)
			assert.True(t, simpleQuery.CanParse, "can parse")
			whereClause := simpleQuery.WhereClauseAsString()
			assert.Contains(t, tt.WantedSql, whereClause, "contains wanted sql")
			assert.Equal(t, tt.WantedQueryType, queryInfo.Typ, "equals to wanted query type")

			for _, wantedRegex := range tt.WantedRegexes {
				mock.ExpectQuery(testdata.EscapeBrackets(wantedRegex)).WillReturnRows(sqlmock.NewRows([]string{"@timestamp", "host.name"}))
			}
			queryRunner := NewQueryRunner(lm, cfg, nil, managementConsole)
			_, err := queryRunner.handleSearch(ctx, tableName, types.MustJSON(tt.QueryJson))
			assert.NoError(t, err)

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
	cfg := config.QuesmaConfiguration{IndexConfig: map[string]config.IndexConfiguration{tableName: {Enabled: true}}}
	table := clickhouse.Table{
		Name:   tableName,
		Config: clickhouse.NewDefaultCHConfig(),
		Cols: map[string]*clickhouse.Column{
			"message$*%:;": {Name: "message$*%:;", Type: clickhouse.NewBaseType("String"), IsFullTextMatch: true},
			"host.name":    {Name: "host.name", Type: clickhouse.NewBaseType("String")},
			"@timestamp":   {Name: "@timestamp", Type: clickhouse.NewBaseType("DateTime64")},
		},
		Created: true,
	}

	db, mock := util.InitSqlMockWithPrettyPrint(t)
	defer db.Close()
	lm := clickhouse.NewLogManagerWithConnection(db, concurrent.NewMapWith(tableName, &table))
	managementConsole := ui.NewQuesmaManagementConsole(cfg, nil, nil, make(<-chan tracing.LogWithLevel, 50000), telemetry.NewPhoneHomeEmptyAgent())

	mock.ExpectQuery("").WillReturnRows(sqlmock.NewRows([]string{"message$*%:;", "host.name", "@timestamp"}). // careful, it's not always in this order, order is nondeterministic
															AddRow("abcd", "abcd", "abcd").
															AddRow("prefix-text-to-highlight", "prefix-text-to-highlight", "prefix-text-to-highlight").
															AddRow("text-to-highlight-suffix", "text-to-highlight-suffix", "text-to-highlight-suffix").
															AddRow("text-to-highlight", "text-to-highlight", "text-to-highlight").
															AddRow("text", "text", "text"))

	queryRunner := NewQueryRunner(lm, cfg, nil, managementConsole)
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

	assert.Equal(t, model.JsonMap{"message$*%:;": []any{}}, getIthHighlight(0)) // no highlight
	assert.Equal(t, model.JsonMap{
		"message$*%:;": []any{"@opensearch-dashboards-highlighted-field@text-to-highlight@/opensearch-dashboards-highlighted-field@"},
	}, getIthHighlight(1))
	assert.Equal(t, model.JsonMap{
		"message$*%:;": []any{"@opensearch-dashboards-highlighted-field@text-to-highlight@/opensearch-dashboards-highlighted-field@"},
	}, getIthHighlight(2))
	assert.Equal(t, model.JsonMap{
		"message$*%:;": []any{"@opensearch-dashboards-highlighted-field@text-to-highlight@/opensearch-dashboards-highlighted-field@"},
	}, getIthHighlight(3))
	assert.Equal(t, model.JsonMap{"message$*%:;": []any{}}, getIthHighlight(4)) // no highlight
}
