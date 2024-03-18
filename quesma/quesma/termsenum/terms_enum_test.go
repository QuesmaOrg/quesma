package termsenum

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
	"mitmproxy/quesma/telemetry"
	"mitmproxy/quesma/tracing"
	"regexp"
	"testing"
)

const testTableName = "logs-generic-default"

var rawRequestBody = []byte(`{
  "field": "client_name",
  "string": "",
  "size": 13,
  "index_filter": {
    "bool": {
      "must": [
        {
          "range": {
            "epoch_time": {
              "format": "strict_date_optional_time",
              "gte": "2024-02-27T12:25:00.000Z",
              "lte": "2024-02-27T12:40:59.999Z"
            }
          }
        },
        {
          "terms": {
            "_tier": [
              "data_hot",
              "data_warm",
              "data_content",
              "data_cold"
            ]
          }
        }
      ]
    }
  }
}`)

var ctx = context.WithValue(context.TODO(), tracing.RequestIdCtxKey, "test")

func TestHandleTermsEnumRequest(t *testing.T) {
	table := &clickhouse.Table{
		Name:   testTableName,
		Config: clickhouse.NewDefaultCHConfig(),
		Cols: map[string]*clickhouse.Column{
			"epoch_time": {
				Name: "epoch_time",
				Type: clickhouse.NewBaseType("DateTime"),
			},
			"message": {
				Name: "message",
				Type: clickhouse.NewBaseType("String"),
			},
			"client_name": {
				Name: "client_name",
				Type: clickhouse.NewBaseType("LowCardinality(String)"),
			},
		},
		Created: true,
	}

	managementConsole := ui.NewQuesmaManagementConsole(config.Load(), nil, make(<-chan string, 50000), telemetry.NewPhoneHomeEmptyAgent())
	db, mock, _ := sqlmock.New()
	defer db.Close()
	lm := clickhouse.NewLogManagerWithConnection(db, concurrent.NewMapWith(testTableName, table))
	qt := &queryparser.ClickhouseQueryTranslator{ClickhouseLM: lm, Table: table}

	// Here we additionally verify that terms for `_tier` are **NOT** included in the SQL query
	expectedQuery1 := `SELECT DISTINCT "client_name" FROM "` + testTableName + `" WHERE "epoch_time">=parseDateTimeBestEffort('2024-02-27T12:25:00.000Z') AND "epoch_time"<=parseDateTimeBestEffort('2024-02-27T12:40:59.999Z') LIMIT 13`
	expectedQuery2 := `SELECT DISTINCT "client_name" FROM "` + testTableName + `" WHERE "epoch_time"<=parseDateTimeBestEffort('2024-02-27T12:40:59.999Z') AND "epoch_time">=parseDateTimeBestEffort('2024-02-27T12:25:00.000Z') LIMIT 13`

	// Once in a while `AND` conditions could be swapped, so we match both cases
	mock.ExpectQuery(fmt.Sprintf("%s|%s", regexp.QuoteMeta(expectedQuery1), regexp.QuoteMeta(expectedQuery2))).
		WillReturnRows(sqlmock.NewRows([]string{"client_name"}).AddRow("client_a").AddRow("client_b"))

	resp, err := handleTermsEnumRequest(ctx, rawRequestBody, qt, managementConsole)
	assert.NoError(t, err)

	var responseModel model.TermsEnumResponse
	if err = json.Unmarshal(resp, &responseModel); err != nil {
		t.Fatal("error unmarshalling terms enum API response:", err)
	}

	assert.ElementsMatch(t, []string{"client_a", "client_b"}, responseModel.Terms)
	assert.True(t, responseModel.Complete)

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal("there were unfulfilled expections:", err)
	}
}
