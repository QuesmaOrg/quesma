package quesma

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"mitmproxy/quesma/clickhouse"
	"mitmproxy/quesma/concurrent"
	"mitmproxy/quesma/model"
	"mitmproxy/quesma/queryparser"
	"mitmproxy/quesma/quesma/config"
	"mitmproxy/quesma/quesma/ui"
	"mitmproxy/quesma/testdata"
	"mitmproxy/quesma/tracing"
	"strconv"
	"testing"
)

func TestNoAsciiTableName(t *testing.T) {
	requestBody := ([]byte)(`{
		"query": {
			"match_all": {}
		}
	}`)
	tableName := `table-namea$한Иb}~`
	lm := clickhouse.NewLogManagerEmpty()
	queryTranslator := &queryparser.ClickhouseQueryTranslator{ClickhouseLM: lm, Table: clickhouse.NewEmptyTable(tableName)}
	simpleQuery, queryInfo, _ := queryTranslator.ParseQueryAsyncSearch(string(requestBody))
	assert.True(t, simpleQuery.CanParse)
	assert.Equal(t, "", simpleQuery.Sql.Stmt)
	assert.Equal(t, model.NewQueryInfoAsyncSearchNone(), queryInfo)

	query := queryTranslator.BuildSimpleSelectQuery(simpleQuery.Sql.Stmt)
	assert.True(t, query.CanParse)
	assert.Equal(t, fmt.Sprintf(`SELECT * FROM "%s" `, tableName), query.String())
}

var ctx = context.WithValue(context.TODO(), tracing.RequestIdCtxKey, "test")

const tableName = `logs-generic-default`

func TestAsyncSearchHandler(t *testing.T) {
	table := concurrent.NewMapWith(tableName, &clickhouse.Table{
		Name:   tableName,
		Config: clickhouse.NewDefaultCHConfig(),
		Cols: map[string]*clickhouse.Column{
			"@timestamp": {
				Name: "@timestamp",
				Type: clickhouse.NewBaseType("DateTime64"),
			},
			"message": {
				Name: "message",
				Type: clickhouse.NewBaseType("String"),
			},
			"host.name": {
				Name: "host.name",
				Type: clickhouse.NewBaseType("LowCardinality(String)"),
			},
		},
		Created: true,
	})

	for _, tt := range testdata.TestsAsyncSearch {
		t.Run(tt.Name, func(t *testing.T) {
			t.Skip("I'll fix after Thursday.")
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			assert.NoError(t, err)
			lm := clickhouse.NewLogManagerWithConnection(db, table)
			managementConsole := ui.NewQuesmaManagementConsole(config.Load(), nil, make(<-chan string, 50000))

			for _, regex := range tt.WantedRegexes {
				mock.ExpectQuery(testdata.EscapeBrackets(regex)).WillReturnRows(sqlmock.NewRows([]string{"@timestamp", "host.name"}))
			}
			_, err = handleAsyncSearch(ctx, tableName, []byte(tt.QueryJson), lm, managementConsole)
			assert.NoError(t, err)

			if err := mock.ExpectationsWereMet(); err != nil {
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
	for _, tt := range testdata.TestsSearch {
		t.Run(tt.Name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			assert.NoError(t, err)

			lm := clickhouse.NewLogManagerWithConnection(db, table)
			managementConsole := ui.NewQuesmaManagementConsole(config.Load(), nil, make(<-chan string, 50000))
			mock.ExpectQuery(testdata.EscapeBrackets(tt.WantedRegex)).WillReturnRows(sqlmock.NewRows([]string{"@timestamp", "host.name"}))
			_, _ = handleSearch(ctx, tableName, []byte(tt.QueryJson), lm, managementConsole)

			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal("there were unfulfilled expections:", err)
			}
		})
	}
}

// TODO this test gives wrong results??
func TestSearcHandlerNoAttrsConfig(t *testing.T) {
	for _, tt := range testdata.TestsSearchNoAttrs {
		t.Run(tt.Name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			assert.NoError(t, err)

			lm := clickhouse.NewLogManagerWithConnection(db, table)
			managementConsole := ui.NewQuesmaManagementConsole(config.Load(), nil, make(<-chan string, 50000))
			mock.ExpectQuery(testdata.EscapeBrackets(tt.WantedRegex)).WillReturnRows(sqlmock.NewRows([]string{"@timestamp", "host.name"}))
			_, _ = handleSearch(ctx, tableName, []byte(tt.QueryJson), lm, managementConsole)

			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal("there were unfulfilled expections:", err)
			}
		})
	}
}

func TestAsyncSearchFilter(t *testing.T) {
	for _, tt := range testdata.TestSearchFilter {
		t.Run(tt.Name, func(t *testing.T) {
			t.Skip("I'll fix after Thursday.")
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			assert.NoError(t, err)

			lm := clickhouse.NewLogManagerWithConnection(db, table)
			managementConsole := ui.NewQuesmaManagementConsole(config.Load(), nil, make(<-chan string, 50000))
			mock.ExpectQuery(testdata.EscapeBrackets(tt.WantedRegex)).WillReturnRows(sqlmock.NewRows([]string{"@timestamp", "host.name"}))
			_, _ = handleAsyncSearch(ctx, tableName, []byte(tt.QueryJson), lm, managementConsole)

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
	// I'm testing querying for all 3 types of fields that we support right now.
	t.Skip("I'll fix after Thursday.")
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
		dateTimeTimestampField:      "SELECT toInt64(toUnixTimestamp(`timestamp`)/60.000000), count() FROM",
		dateTime64TimestampField:    "SELECT toInt64(toUnixTimestamp64Milli(`timestamp64`)/60000), count() FROM",
		dateTime64OurTimestampField: "SELECT toInt64(toUnixTimestamp64Milli(`@timestamp`)/60000), count() FROM",
	}

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	lm := clickhouse.NewLogManagerWithConnection(db, concurrent.NewMapWith(tableName, &table))
	managementConsole := ui.NewQuesmaManagementConsole(config.Load(), nil, make(<-chan string, 50000))

	for _, fieldName := range []string{dateTimeTimestampField, dateTime64TimestampField, dateTime64OurTimestampField} {
		mock.ExpectQuery(testdata.EscapeBrackets(expectedSelectStatementRegex[fieldName])).
			WillReturnRows(sqlmock.NewRows([]string{"key", "doc_count"}))
		// .AddRow(1000, uint64(10)).AddRow(1001, uint64(20))) // here rows should be added if uint64 were supported
		response, err := handleAsyncSearch(ctx, tableName, []byte(query(fieldName)), lm, managementConsole)
		assert.NoError(t, err)

		var responseMap model.JsonMap
		err = json.Unmarshal(response, &responseMap)
		assert.NoError(t, err, "error unmarshalling search API response:")

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal("there were unfulfilled expections:", err)
		}
	}
}
