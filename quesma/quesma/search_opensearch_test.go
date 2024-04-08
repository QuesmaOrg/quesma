package quesma

import (
	"context"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"mitmproxy/quesma/clickhouse"
	"mitmproxy/quesma/concurrent"
	"mitmproxy/quesma/queryparser"
	"mitmproxy/quesma/quesma/config"
	"mitmproxy/quesma/quesma/ui"
	"mitmproxy/quesma/telemetry"
	"mitmproxy/quesma/testdata"
	"mitmproxy/quesma/tracing"
	"strconv"
	"testing"
)

func TestSearchOpensearch(t *testing.T) {
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
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			assert.NoError(t, err)
			lm := clickhouse.NewLogManagerWithConnection(db, concurrent.NewMapWith(tableName, &table))
			managementConsole := ui.NewQuesmaManagementConsole(config.Load(), nil, make(<-chan tracing.LogWithLevel, 50000), telemetry.NewPhoneHomeEmptyAgent())
			cw := queryparser.ClickhouseQueryTranslator{ClickhouseLM: lm, Table: &table, Ctx: context.Background()}

			simpleQuery, queryInfo, _ := cw.ParseQuery(tt.QueryJson)
			assert.True(t, simpleQuery.CanParse, "can parse")
			assert.Contains(t, tt.WantedSql, simpleQuery.Sql.Stmt, "contains wanted sql")
			assert.Equal(t, tt.WantedQueryType, queryInfo.Typ, "equals to wanted query type")

			for _, wantedRegex := range tt.WantedRegexes {
				mock.ExpectQuery(testdata.EscapeBrackets(wantedRegex)).WillReturnRows(sqlmock.NewRows([]string{"@timestamp", "host.name"}))
			}

			queryRunner := NewQueryRunner()
			_, err = queryRunner.handleSearch(ctx, tableName, []byte(tt.QueryJson), lm, managementConsole)
			assert.NoError(t, err)

			if err = mock.ExpectationsWereMet(); err != nil {
				t.Fatal("there were unfulfilled expections:", err)
			}
		})
	}
}
