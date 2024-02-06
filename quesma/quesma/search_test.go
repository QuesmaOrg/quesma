package quesma

import (
	"context"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"mitmproxy/quesma/clickhouse"
	"mitmproxy/quesma/model"
	"mitmproxy/quesma/queryparser"
	"mitmproxy/quesma/quesma/config"
	"mitmproxy/quesma/testdata"
	"testing"
)

func TestNoAsciiTableName(t *testing.T) {
	requestBody := ([]byte)(`{
		"query": {
			"match_all": {}
		}
	}`)
	tableName := `"table-namea$한Иb}~"`
	lm := clickhouse.NewLogManagerEmpty()
	queryTranslator := &queryparser.ClickhouseQueryTranslator{ClickhouseLM: lm}
	simpleQuery, queryInfo := queryTranslator.WriteAsyncSearch(requestBody)
	assert.True(t, simpleQuery.CanParse)
	assert.Equal(t, "", simpleQuery.Sql.Stmt)
	assert.Equal(t, model.NewQueryInfoAsyncSearchNone(), queryInfo)

	query := queryTranslator.BuildSimpleSelectQuery(tableName, simpleQuery.Sql.Stmt)
	assert.True(t, query.CanParse)
	assert.Equal(t, `SELECT * FROM `+tableName+" ", query.String())
}

var ctx = context.WithValue(context.TODO(), RequestId{}, "test")

func TestAsyncSearchHandler(t *testing.T) {
	table := clickhouse.TableMap{
		queryparser.TableName: &clickhouse.Table{
			Name:   queryparser.TableName,
			Config: clickhouse.NewDefaultCHConfig(),
			Cols: map[string]*clickhouse.Column{
				"@timestamp": {
					Name: "@timestamp",
					Type: clickhouse.NewBaseType("DateTime"),
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
		},
	}

	for _, tt := range testdata.TestsAsyncSearch {
		t.Run(tt.Name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			assert.NoError(t, err)
			lm := clickhouse.NewLogManagerWithConnection(db, table, make(clickhouse.TableMap))
			managementConsole := NewQuesmaManagementConsole(config.Load())

			for _, regex := range tt.WantedRegexes {
				mock.ExpectQuery(testdata.EscapeBrackets(regex)).WillReturnRows(sqlmock.NewRows([]string{"@timestamp", "host.name"}))
			}
			_, _ = handleAsyncSearch(ctx, "index-placeholder", []byte(tt.QueryJson), lm, managementConsole)

			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal("there were unfulfilled expections:", err)
			}
		})
	}
}

var table = clickhouse.TableMap{
	queryparser.TableName: &clickhouse.Table{
		Name:   queryparser.TableName,
		Config: clickhouse.NewNoTimestampOnlyStringAttrCHConfig(),
		Cols: map[string]*clickhouse.Column{
			// only one field because currently we have non-determinism in translating * -> all fields :( and can't regex that easily.
			// (TODO Maybe we can, don't want to waste time for this now https://stackoverflow.com/questions/3533408/regex-i-want-this-and-that-and-that-in-any-order)
			"message": {
				Name: "message",
				Type: clickhouse.NewBaseType("String"),
			},
		},
		Created: true,
	},
}

func TestSearchHandler(t *testing.T) {
	for _, tt := range testdata.TestsSearch {
		t.Run(tt.Name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			assert.NoError(t, err)

			lm := clickhouse.NewLogManagerWithConnection(db, table, make(clickhouse.TableMap))
			managementConsole := NewQuesmaManagementConsole(config.Load())
			mock.ExpectQuery(testdata.EscapeBrackets(tt.WantedRegex)).WillReturnRows(sqlmock.NewRows([]string{"@timestamp", "host.name"}))
			_, _ = handleSearch(ctx, "index-placeholder", []byte(tt.QueryJson), lm, managementConsole)

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

			lm := clickhouse.NewLogManagerWithConnection(db, table, make(clickhouse.TableMap))
			managementConsole := NewQuesmaManagementConsole(config.Load())
			mock.ExpectQuery(testdata.EscapeBrackets(tt.WantedRegex)).WillReturnRows(sqlmock.NewRows([]string{"@timestamp", "host.name"}))
			_, _ = handleSearch(ctx, "index-placeholder", []byte(tt.QueryJson), lm, managementConsole)

			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal("there were unfulfilled expections:", err)
			}
		})
	}
}
