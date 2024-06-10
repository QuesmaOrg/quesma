package quesma

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/k0kubun/pp"
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
	"strings"
	"testing"
)

const defaultAsyncSearchTimeout = 1000

func TestNoAsciiTableName(t *testing.T) {
	requestBody := ([]byte)(`{
		"query": {
			"match_all": {}
		}
	}`)
	tableName := `table-namea$한Иb}~`
	lm := clickhouse.NewLogManagerEmpty()
	queryTranslator := &queryparser.ClickhouseQueryTranslator{ClickhouseLM: lm, Table: clickhouse.NewEmptyTable(tableName), Ctx: ctx}
	simpleQuery, queryInfo, _ := queryTranslator.ParseQueryAsyncSearch(string(requestBody))
	assert.True(t, simpleQuery.CanParse)
	assert.Equal(t, "", simpleQuery.WhereClauseAsString())
	assert.Equal(t, model.ListAllFields, queryInfo.Typ)
	const Limit = 1000
	query := queryTranslator.BuildNRowsQuery("*", &simpleQuery, Limit)
	assert.True(t, query.CanParse)
	assert.Equal(t, fmt.Sprintf(`SELECT * FROM "%s" LIMIT %d`, tableName, Limit), query.String(ctx))
}

var ctx = context.WithValue(context.TODO(), tracing.RequestIdCtxKey, tracing.GetRequestId())

const tableName = `logs-generic-default`

func TestAsyncSearchHandler(t *testing.T) {
	cfg := config.QuesmaConfiguration{IndexConfig: map[string]config.IndexConfiguration{tableName: {Enabled: true}}}
	table := concurrent.NewMapWith(tableName, &clickhouse.Table{
		Name:   tableName,
		Config: clickhouse.NewDefaultCHConfig(),
		Cols: map[string]*clickhouse.Column{
			"@timestamp": {
				Name: "@timestamp",
				Type: clickhouse.NewBaseType("DateTime64"),
			},
			"message": {
				Name:            "message",
				Type:            clickhouse.NewBaseType("String"),
				IsFullTextMatch: true,
			},
			"host.name": {
				Name: "host.name",
				Type: clickhouse.NewBaseType("LowCardinality(String)"),
			},
			"properties::isreg": {
				Name: "properties::isreg",
				Type: clickhouse.NewBaseType("UInt8"),
			},
		},
		Created: true,
	})
	for i, tt := range testdata.TestsAsyncSearch {
		t.Run(strconv.Itoa(i)+tt.Name, func(t *testing.T) {
			db, mock := util.InitSqlMockWithPrettyPrint(t)
			mock.MatchExpectationsInOrder(false)
			defer db.Close()
			lm := clickhouse.NewLogManagerWithConnection(db, table)
			managementConsole := ui.NewQuesmaManagementConsole(cfg, nil, nil, make(<-chan tracing.LogWithLevel, 50000), telemetry.NewPhoneHomeEmptyAgent())

			for _, wantedRegex := range tt.WantedRegexes {
				if tt.WantedParseResult.Typ == model.ListAllFields {
					// Normally we always want to escape, but in ListAllFields (SELECT *) we have (permutation1|permutation2|...)
					// and we don't want to escape those ( and ) characters. So we don't escape [:WHERE], and escape [WHERE:]
					// Hackish, but fastest way to get it done.
					splitIndex := strings.Index(wantedRegex, "WHERE")
					if splitIndex != -1 {
						wantedRegex = wantedRegex[:splitIndex] + testdata.EscapeBrackets(wantedRegex[splitIndex:])
					}
				} else {
					wantedRegex = testdata.EscapeBrackets(wantedRegex)
				}
				mock.ExpectQuery(wantedRegex).WillReturnRows(sqlmock.NewRows([]string{"@timestamp", "host.name"}))
			}
			queryRunner := NewQueryRunner(lm, cfg, nil, managementConsole)
			_, err := queryRunner.handleAsyncSearch(ctx, tableName, types.MustJSON(tt.QueryJson), defaultAsyncSearchTimeout, true)
			assert.NoError(t, err)

			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal("there were unfulfilled expections:", err)
			}
		})
	}
}

func TestAsyncSearchHandlerSpecialCharacters(t *testing.T) {
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

	for i, tt := range testdata.AggregationTestsWithSpecialCharactersInFieldNames {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			db, mock := util.InitSqlMockWithPrettyPrint(t)
			mock.MatchExpectationsInOrder(false)
			defer db.Close()
			lm := clickhouse.NewLogManagerWithConnection(db, concurrent.NewMapWith(tableName, &table))
			managementConsole := ui.NewQuesmaManagementConsole(cfg, nil, nil, make(<-chan tracing.LogWithLevel, 50000), telemetry.NewPhoneHomeEmptyAgent())

			for _, expectedSql := range tt.ExpectedSQLs {
				mock.ExpectQuery(testdata.EscapeBrackets(expectedSql)).WillReturnRows(sqlmock.NewRows([]string{"@timestamp", "host.name"}))
			}

			queryRunner := NewQueryRunner(lm, cfg, nil, managementConsole)
			_, err := queryRunner.handleAsyncSearch(ctx, tableName, types.MustJSON(tt.QueryRequestJson), defaultAsyncSearchTimeout, true)
			assert.NoError(t, err)

			if err = mock.ExpectationsWereMet(); err != nil {
				t.Fatal("there were unfulfilled expections:", err)
			}
		})
	}
}

var table = concurrent.NewMapWith(tableName, &clickhouse.Table{
	Name:   tableName,
	Config: clickhouse.NewChTableConfigTimestampStringAttr(),
	Cols: map[string]*clickhouse.Column{
		// only one field because currently we have non-determinism in translating * -> all fields :( and can't regex that easily.
		// (TODO Maybe we can, don't want to waste time for this now https://stackoverflow.com/questions/3533408/regex-i-want-this-and-that-and-that-in-any-order)
		"message": {
			Name:            "message",
			Type:            clickhouse.NewBaseType("String"),
			IsFullTextMatch: true,
		},
	},
	Created: true,
})

func TestSearchHandler(t *testing.T) {
	cfg := config.QuesmaConfiguration{IndexConfig: map[string]config.IndexConfiguration{tableName: {Enabled: true}}}
	for _, tt := range testdata.TestsSearch {
		t.Run(tt.Name, func(t *testing.T) {
			db, mock := util.InitSqlMockWithPrettyPrint(t)
			mock.MatchExpectationsInOrder(false)
			defer db.Close()

			lm := clickhouse.NewLogManagerWithConnection(db, table)
			managementConsole := ui.NewQuesmaManagementConsole(cfg, nil, nil, make(<-chan tracing.LogWithLevel, 50000), telemetry.NewPhoneHomeEmptyAgent())
			for _, wantedRegex := range tt.WantedRegexes {
				mock.ExpectQuery(testdata.EscapeWildcard(testdata.EscapeBrackets(wantedRegex))).
					WillReturnRows(sqlmock.NewRows([]string{"@timestamp", "host.name"}))
			}
			queryRunner := NewQueryRunner(lm, cfg, nil, managementConsole)
			_, _ = queryRunner.handleSearch(ctx, tableName, types.MustJSON(tt.QueryJson))

			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal("there were unfulfilled expections:", err)
			}
		})
	}
}

// TODO this test gives wrong results??
func TestSearchHandlerNoAttrsConfig(t *testing.T) {
	cfg := config.QuesmaConfiguration{IndexConfig: map[string]config.IndexConfiguration{tableName: {Enabled: true}}}
	for _, tt := range testdata.TestsSearchNoAttrs {
		t.Run(tt.Name, func(t *testing.T) {
			db, mock := util.InitSqlMockWithPrettyPrint(t)
			defer db.Close()

			lm := clickhouse.NewLogManagerWithConnection(db, table)
			managementConsole := ui.NewQuesmaManagementConsole(cfg, nil, nil, make(<-chan tracing.LogWithLevel, 50000), telemetry.NewPhoneHomeEmptyAgent())
			for _, wantedRegex := range tt.WantedRegexes {
				mock.ExpectQuery(testdata.EscapeBrackets(wantedRegex)).WillReturnRows(sqlmock.NewRows([]string{"@timestamp", "host.name"}))
			}
			queryRunner := NewQueryRunner(lm, cfg, nil, managementConsole)
			_, _ = queryRunner.handleSearch(ctx, tableName, types.MustJSON(tt.QueryJson))

			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal("there were unfulfilled expections:", err)
			}
		})
	}
}

func TestAsyncSearchFilter(t *testing.T) {
	cfg := config.QuesmaConfiguration{IndexConfig: map[string]config.IndexConfiguration{tableName: {Enabled: true}}}
	for _, tt := range testdata.TestSearchFilter {
		t.Run(tt.Name, func(t *testing.T) {
			db, mock := util.InitSqlMockWithPrettyPrint(t)
			mock.MatchExpectationsInOrder(false)
			defer db.Close()

			lm := clickhouse.NewLogManagerWithConnection(db, table)
			managementConsole := ui.NewQuesmaManagementConsole(cfg, nil, nil, make(<-chan tracing.LogWithLevel, 50000), telemetry.NewPhoneHomeEmptyAgent())
			for _, wantedRegex := range tt.WantedRegexes {
				mock.ExpectQuery(testdata.EscapeBrackets(wantedRegex)).WillReturnRows(sqlmock.NewRows([]string{"@timestamp", "host.name"}))
			}
			queryRunner := NewQueryRunner(lm, cfg, nil, managementConsole)
			_, _ = queryRunner.handleAsyncSearch(ctx, tableName, types.MustJSON(tt.QueryJson), defaultAsyncSearchTimeout, true)
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal("there were unfulfilled expections:", err)
			}
		})
	}
}

// TestHandlingDateTimeFields tests handling DateTime, DateTime64 fields in queries, as well as our timestamp field.
// Unfortunately, it's not an 100% end-to-end test, which would test the full `handleAsyncSearch` function
// (testing of creating response is lacking), because of `sqlmock` limitation.
// It can't return uint64, thus creating response code panics because of that.
func TestHandlingDateTimeFields(t *testing.T) {
	cfg := config.QuesmaConfiguration{IndexConfig: map[string]config.IndexConfiguration{tableName: {Enabled: true}}}
	// I'm testing querying for all 3 types of fields that we support right now.
	const dateTimeTimestampField = "timestamp"
	const dateTime64TimestampField = "timestamp64"
	const dateTime64OurTimestampField = "@timestamp"
	table := clickhouse.Table{Name: tableName, Config: clickhouse.NewChTableConfigTimestampStringAttr(), Created: true,
		Cols: map[string]*clickhouse.Column{
			"timestamp":   {Name: "timestamp", Type: clickhouse.NewBaseType("DateTime")},
			"timestamp64": {Name: "timestamp64", Type: clickhouse.NewBaseType("DateTime64")},
		},
	}
	query := func(fieldName string) string {
		return `{
			"size": 0,
			"track_total_hits": false,
			"aggs": {"0": {"date_histogram": {"field": ` + strconv.Quote(fieldName) + `, "fixed_interval": "60s"}}},
			"query": {"bool": {"filter": [{"bool": {
				"filter": [{"range": {
					"timestamp": {
						"format": "strict_date_optional_time",
						"gte": "2024-01-29T15:36:36.491Z",
						"lte": "2024-01-29T18:11:36.491Z"
					}
				}}],
				"must": [{"range": {
					"timestamp64": {
						"format": "strict_date_optional_time",
						"gte": "2024-01-29T15:36:36.491Z",
						"lte": "2024-01-29T18:11:36.491Z"
					}
				}}],
				"must_not": [{"range": {
					"@timestamp": {
						"format": "strict_date_optional_time",
						"gte": "2024-01-29T15:36:36.491Z",
						"lte": "2024-01-29T18:11:36.491Z"
					}
				}}],
				"should": []
			}}]}}
		}`
	}
	expectedSelectStatementRegex := map[string]string{
		dateTimeTimestampField:      `SELECT toInt64(toUnixTimestamp("timestamp") / 60), count() FROM`,
		dateTime64TimestampField:    `SELECT toInt64(toUnixTimestamp64Milli("timestamp64") / 60000), count() FROM`,
		dateTime64OurTimestampField: `SELECT toInt64(toUnixTimestamp64Milli("@timestamp") / 60000), count() FROM`,
	}

	db, mock := util.InitSqlMockWithPrettyPrint(t)

	// queries can be run in parallel

	mock.MatchExpectationsInOrder(false)
	defer db.Close()
	lm := clickhouse.NewLogManagerWithConnection(db, concurrent.NewMapWith(tableName, &table))
	managementConsole := ui.NewQuesmaManagementConsole(cfg, nil, nil, make(<-chan tracing.LogWithLevel, 50000), telemetry.NewPhoneHomeEmptyAgent())

	for _, fieldName := range []string{dateTimeTimestampField, dateTime64TimestampField, dateTime64OurTimestampField} {

		mock.ExpectQuery(testdata.EscapeBrackets(expectedSelectStatementRegex[fieldName])).
			WillReturnRows(sqlmock.NewRows([]string{"key", "doc_count"}))

		// .AddRow(1000, uint64(10)).AddRow(1001, uint64(20))) // here rows should be added if uint64 were supported
		queryRunner := NewQueryRunner(lm, cfg, nil, managementConsole)
		response, err := queryRunner.handleAsyncSearch(ctx, tableName, types.MustJSON(query(fieldName)), defaultAsyncSearchTimeout, true)
		assert.NoError(t, err)

		var responseMap model.JsonMap
		err = json.Unmarshal(response, &responseMap)
		assert.NoError(t, err, "error unmarshalling search API response:")

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal("there were unfulfilled expections:", err)
		}
	}
}

// TestAsyncSearchFacets tests if results for facets are correctly returned
// (top 10 values, "other" value, min/max).
// Both `_search`, and `_async_search` handlers are tested.
func TestNumericFacetsQueries(t *testing.T) {
	cfg := config.QuesmaConfiguration{IndexConfig: map[string]config.IndexConfiguration{tableName: {Enabled: true}}}
	table := concurrent.NewMapWith(tableName, &clickhouse.Table{
		Name:   tableName,
		Config: clickhouse.NewDefaultCHConfig(),
		Cols: map[string]*clickhouse.Column{
			"int64-field": {
				Name: "int64-field",
				Type: clickhouse.NewBaseType("Int64"),
			},
			"float64-field": {
				Name: "float64-field",
				Type: clickhouse.NewBaseType("Float64"),
			},
		},
		Created: true,
	})
	handlers := []string{"handleSearch", "handleAsyncSearch"}
	for i, tt := range testdata.TestsNumericFacets {
		for _, handlerName := range handlers {
			t.Run(strconv.Itoa(i)+tt.Name, func(t *testing.T) {
				db, mock := util.InitSqlMockWithPrettyPrint(t)
				mock.MatchExpectationsInOrder(false)
				defer db.Close()
				lm := clickhouse.NewLogManagerWithConnection(db, table)
				managementConsole := ui.NewQuesmaManagementConsole(cfg, nil, nil, make(<-chan tracing.LogWithLevel, 50000), telemetry.NewPhoneHomeEmptyAgent())

				returnedBuckets := sqlmock.NewRows([]string{"", ""})
				for _, row := range tt.ResultRows {
					returnedBuckets.AddRow(row[0], row[1])
				}

				// count, present in all tests
				mock.ExpectQuery(`SELECT count\(\) FROM ` + strconv.Quote(tableName)).WillReturnRows(sqlmock.NewRows([]string{"count"}))
				// Don't care about the query's SQL in this test, it's thoroughly tested in different tests, thus ""
				mock.ExpectQuery("").WillReturnRows(returnedBuckets)

				queryRunner := NewQueryRunner(lm, cfg, nil, managementConsole)
				var response []byte
				var err error
				if handlerName == "handleSearch" {
					response, err = queryRunner.handleSearch(ctx, tableName, types.MustJSON(tt.QueryJson))
				} else if handlerName == "handleAsyncSearch" {
					response, err = queryRunner.handleAsyncSearch(ctx, tableName, types.MustJSON(tt.QueryJson), defaultAsyncSearchTimeout, true)
				}
				assert.NoError(t, err)

				if err := mock.ExpectationsWereMet(); err != nil {
					t.Fatal("there were unfulfilled expections:", err)
				}

				var responseMap model.JsonMap
				err = json.Unmarshal(response, &responseMap)
				assert.NoError(t, err, "error unmarshalling search API response:")

				var responsePart model.JsonMap
				if handlerName == "handleSearch" {
					responsePart = responseMap
				} else {
					responsePart = responseMap["response"].(model.JsonMap)
				}
				// check max
				assert.Equal(t, tt.MaxExpected, responsePart["aggregations"].(model.JsonMap)["sample"].(model.JsonMap)["max_value"].(model.JsonMap)["value"].(float64))
				// check min
				assert.Equal(t, tt.MinExpected, responsePart["aggregations"].(model.JsonMap)["sample"].(model.JsonMap)["min_value"].(model.JsonMap)["value"].(float64))
				// check hits count (in 3 different places)
				assert.Equal(t, tt.CountExpected, responsePart["aggregations"].(model.JsonMap)["sample"].(model.JsonMap)["sample_count"].(model.JsonMap)["value"].(float64))
				assert.Equal(t, tt.CountExpected, responsePart["aggregations"].(model.JsonMap)["sample"].(model.JsonMap)["doc_count"].(float64))
				// TODO restore line below when track_total_hits works!!
				// assert.Equal(t, tt.CountExpected, responsePart["hits"].(model.JsonMap)["total"].(model.JsonMap)["value"].(float64))
				// check sum_other_doc_count (sum of all doc_counts that are not in top 10 facets)
				assert.Equal(t, tt.SumOtherDocCountExpected, responsePart["aggregations"].(model.JsonMap)["sample"].(model.JsonMap)["top_values"].(model.JsonMap)["sum_other_doc_count"].(float64))
			})
		}
	}
}

func TestSearchTrackTotalCount(t *testing.T) {
	cfg := config.QuesmaConfiguration{IndexConfig: map[string]config.IndexConfiguration{tableName: {Enabled: true}}}

	test := func(t *testing.T, handlerName string, testcase testdata.FullSearchTestCase) {
		db, mock := util.InitSqlMockWithPrettyPrint(t)
		defer db.Close()
		lm := clickhouse.NewLogManagerWithConnection(db, table)
		managementConsole := ui.NewQuesmaManagementConsole(cfg, nil, nil, make(<-chan tracing.LogWithLevel, 50000), telemetry.NewPhoneHomeEmptyAgent())

		for _, sql := range testcase.ExpectedSQLs {
			mock.ExpectQuery(testdata.EscapeBrackets(sql)).WillReturnRows(sqlmock.NewRows([]string{"", ""}))
		}

		queryRunner := NewQueryRunner(lm, cfg, nil, managementConsole)

		var response []byte
		var err error
		if handlerName == "handleSearch" {
			response, err = queryRunner.handleSearch(ctx, tableName, types.MustJSON(testcase.QueryRequestJson))
		} else if handlerName == "handleAsyncSearch" {
			response, err = queryRunner.handleAsyncSearch(
				ctx, tableName, types.MustJSON(testcase.QueryRequestJson), defaultAsyncSearchTimeout, true)
		}
		assert.NoError(t, err)

		if err := mock.ExpectationsWereMet(); err != nil {
			assert.NoError(t, err, "there were unfulfilled expections:")
		}

		var responseMap model.JsonMap
		err = json.Unmarshal(response, &responseMap)
		if err != nil {
			pp.Println("Response", string(response))
		}
		assert.NoError(t, err, "error unmarshalling search API response:")

		var responsePart model.JsonMap
		if handlerName == "handleSearch" {
			responsePart = responseMap
		} else {
			responsePart = responseMap["response"].(model.JsonMap)
		}

		pp.Println(responsePart)
	}

	handlers := []string{"handleSearch", "handleAsyncSearch"}
	for i, tt := range testdata.FullSearchRequests {
		for _, handlerName := range handlers[:1] {
			t.Run(strconv.Itoa(i)+" "+tt.Name, func(t *testing.T) {
				test(t, handlerName, tt)
			})
		}
	}
}
