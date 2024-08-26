// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package terms_enum

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
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
	"quesma/tracing"
	"quesma/util"
	"regexp"
	"testing"
)

const testTableName = model.SingleTableNamePlaceHolder

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

	managementConsole := ui.NewQuesmaManagementConsole(config.QuesmaConfiguration{}, nil, nil, make(<-chan logger.LogWithLevel, 50000), telemetry.NewPhoneHomeEmptyAgent(), nil)
	db, mock := util.InitSqlMockWithPrettyPrint(t, true)
	defer db.Close()
	lm := clickhouse.NewLogManagerWithConnection(db, concurrent.NewMapWith(testTableName, table))
	s := schema.StaticRegistry{
		Tables: map[schema.TableName]schema.Schema{
			"tablename": {
				Fields: map[schema.FieldName]schema.Field{
					"host.name":         {PropertyName: "host.name", InternalPropertyName: "host.name", Type: schema.TypeObject},
					"type":              {PropertyName: "type", InternalPropertyName: "type", Type: schema.TypeText},
					"name":              {PropertyName: "name", InternalPropertyName: "name", Type: schema.TypeText},
					"content":           {PropertyName: "content", InternalPropertyName: "content", Type: schema.TypeText},
					"message":           {PropertyName: "message", InternalPropertyName: "message", Type: schema.TypeText},
					"host_name.keyword": {PropertyName: "host_name.keyword", InternalPropertyName: "host_name.keyword", Type: schema.TypeKeyword},
					"FlightDelay":       {PropertyName: "FlightDelay", InternalPropertyName: "FlightDelay", Type: schema.TypeText},
					"Cancelled":         {PropertyName: "Cancelled", InternalPropertyName: "Cancelled", Type: schema.TypeText},
					"FlightDelayMin":    {PropertyName: "FlightDelayMin", InternalPropertyName: "FlightDelayMin", Type: schema.TypeText},
					"_id":               {PropertyName: "_id", InternalPropertyName: "_id", Type: schema.TypeText},
				},
			},
		},
	}
	qt := &queryparser.ClickhouseQueryTranslator{ClickhouseLM: lm, Table: table, Ctx: context.Background(), SchemaRegistry: s}
	// Here we additionally verify that terms for `_tier` are **NOT** included in the SQL query
	expectedQuery1 := `SELECT DISTINCT "client_name" FROM ` + testTableName + ` WHERE ("epoch_time">=parseDateTimeBestEffort('2024-02-27T12:25:00.000Z') AND "epoch_time"<=parseDateTimeBestEffort('2024-02-27T12:40:59.999Z')) LIMIT 13`
	expectedQuery2 := `SELECT DISTINCT "client_name" FROM ` + testTableName + ` WHERE ("epoch_time"<=parseDateTimeBestEffort('2024-02-27T12:40:59.999Z') AND "epoch_time">=parseDateTimeBestEffort('2024-02-27T12:25:00.000Z')) LIMIT 13`

	// Once in a while `AND` conditions could be swapped, so we match both cases
	mock.ExpectQuery(fmt.Sprintf("%s|%s", regexp.QuoteMeta(expectedQuery1), regexp.QuoteMeta(expectedQuery2))).
		WillReturnRows(sqlmock.NewRows([]string{"client_name"}).AddRow("client_a").AddRow("client_b"))

	resp, err := handleTermsEnumRequest(ctx, types.MustJSON(string(rawRequestBody)), qt, managementConsole)
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
