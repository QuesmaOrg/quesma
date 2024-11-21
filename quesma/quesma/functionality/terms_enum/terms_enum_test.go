// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package terms_enum

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"quesma/clickhouse"
	"quesma/logger"
	"quesma/model"
	"quesma/queryparser"
	"quesma/quesma/config"
	"quesma/quesma/types"
	"quesma/quesma/ui"
	"quesma/schema"
	"quesma/table_resolver"
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
          "range": {
            "epoch_time_datetime64": {
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

func testHandleTermsEnumRequest(t *testing.T, requestBody []byte) {
	table := &clickhouse.Table{
		Name:   testTableName,
		Config: clickhouse.NewDefaultCHConfig(),
		Cols: map[string]*clickhouse.Column{
			"epoch_time": {
				Name: "epoch_time",
				Type: clickhouse.NewBaseType("DateTime"),
			},
			"epoch_time_datetime64": {
				Name: "epoch_time_datetime64",
				Type: clickhouse.NewBaseType("DateTime64"),
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
	tableResolver := table_resolver.NewEmptyTableResolver()
	managementConsole := ui.NewQuesmaManagementConsole(&config.QuesmaConfiguration{}, nil, nil, make(<-chan logger.LogWithLevel, 50000), telemetry.NewPhoneHomeEmptyAgent(), nil, tableResolver)
	db, mock := util.InitSqlMockWithPrettyPrint(t, true)
	defer db.Close()
	lm := clickhouse.NewLogManagerWithConnection(db, util.NewSyncMapWith(testTableName, table))
	s := schema.StaticRegistry{
		Tables: map[schema.TableName]schema.Schema{
			testTableName: {
				Fields: map[schema.FieldName]schema.Field{
					"client_name":       {PropertyName: "client_name", InternalPropertyName: "client_name", Type: schema.QuesmaTypeObject},
					"type":              {PropertyName: "type", InternalPropertyName: "type", Type: schema.QuesmaTypeText},
					"name":              {PropertyName: "name", InternalPropertyName: "name", Type: schema.QuesmaTypeText},
					"content":           {PropertyName: "content", InternalPropertyName: "content", Type: schema.QuesmaTypeText},
					"message":           {PropertyName: "message", InternalPropertyName: "message", Type: schema.QuesmaTypeText},
					"host_name.keyword": {PropertyName: "host_name.keyword", InternalPropertyName: "host_name.keyword", Type: schema.QuesmaTypeKeyword},
					"FlightDelay":       {PropertyName: "FlightDelay", InternalPropertyName: "FlightDelay", Type: schema.QuesmaTypeText},
					"Cancelled":         {PropertyName: "Cancelled", InternalPropertyName: "Cancelled", Type: schema.QuesmaTypeText},
					"FlightDelayMin":    {PropertyName: "FlightDelayMin", InternalPropertyName: "FlightDelayMin", Type: schema.QuesmaTypeText},
					"_id":               {PropertyName: "_id", InternalPropertyName: "_id", Type: schema.QuesmaTypeText},
				},
				Aliases: map[schema.FieldName]schema.FieldName{
					"client.name": "client_name",
				},
			},
		},
	}
	qt := &queryparser.ClickhouseQueryTranslator{ClickhouseLM: lm, Table: table, Ctx: context.Background(), Schema: s.Tables[schema.TableName(testTableName)]}
	// Here we additionally verify that terms for `_tier` are **NOT** included in the SQL query
	expectedQuery1 := `SELECT DISTINCT "client_name" FROM ` + testTableName + ` WHERE (("epoch_time">=fromUnixTimestamp(1709036700) AND "epoch_time"<=fromUnixTimestamp(1709037659)) AND ("epoch_time_datetime64">=fromUnixTimestamp64Milli(1709036700000) AND "epoch_time_datetime64"<=fromUnixTimestamp64Milli(1709037659999))) LIMIT 13`
	expectedQuery2 := `SELECT DISTINCT "client_name" FROM ` + testTableName + ` WHERE (("epoch_time">=fromUnixTimestamp(1709036700) AND "epoch_time"<=fromUnixTimestamp(1709037659)) AND ("epoch_time_datetime64">=fromUnixTimestamp64Milli(1709036700000) AND "epoch_time_datetime64"<=fromUnixTimestamp64Milli(1709037659999))) LIMIT 13`

	// Once in a while `AND` conditions could be swapped, so we match both cases
	mock.ExpectQuery(fmt.Sprintf("%s|%s", regexp.QuoteMeta(expectedQuery1), regexp.QuoteMeta(expectedQuery2))).
		WillReturnRows(sqlmock.NewRows([]string{"client_name"}).AddRow("client_a").AddRow("client_b"))

	resp, err := handleTermsEnumRequest(ctx, types.MustJSON(string(requestBody)), qt, managementConsole)
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

func TestHandleTermsEnumRequest(t *testing.T) {
	testHandleTermsEnumRequest(t, rawRequestBody)
}

// Basic test.
// "client.name" should be replaced by "client_name", and results should stay the same
func TestIfHandleTermsEnumUsesSchema(t *testing.T) {
	requestBodyWithAliasedField := bytes.ReplaceAll(rawRequestBody, []byte(`"field": "client_name"`), []byte(`"field": "client.name"`))
	testHandleTermsEnumRequest(t, requestBodyWithAliasedField)
}
