// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package frontend_connectors

import (
	"bytes"
	"context"
	"fmt"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/QuesmaOrg/quesma/platform/backend_connectors"
	"github.com/QuesmaOrg/quesma/platform/clickhouse"
	"github.com/QuesmaOrg/quesma/platform/config"
	"github.com/QuesmaOrg/quesma/platform/logger"
	"github.com/QuesmaOrg/quesma/platform/model"
	"github.com/QuesmaOrg/quesma/platform/parsers/elastic_query_dsl"
	"github.com/QuesmaOrg/quesma/platform/schema"
	"github.com/QuesmaOrg/quesma/platform/table_resolver"
	"github.com/QuesmaOrg/quesma/platform/ui"
	"github.com/QuesmaOrg/quesma/platform/util"
	"github.com/QuesmaOrg/quesma/platform/v2/core/diag"
	"github.com/QuesmaOrg/quesma/platform/v2/core/tracing"
	"github.com/QuesmaOrg/quesma/platform/v2/core/types"
	"github.com/goccy/go-json"
	"github.com/stretchr/testify/assert"
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

func testHandleTermsEnumRequest(t *testing.T, requestBody []byte, fieldName string) {
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
				Type: clickhouse.NewBaseType("Map(String, Nullable(String))"),
			},
			"map_name": {
				Name: "map_name",
				Type: clickhouse.NewBaseType("LowCardinality(String)"),
			},
		},
	}
	tableResolver := table_resolver.NewEmptyTableResolver()
	managementConsole := ui.NewQuesmaManagementConsole(&config.QuesmaConfiguration{}, nil, make(<-chan logger.LogWithLevel, 50000), diag.EmptyPhoneHomeRecentStatsProvider(), nil, tableResolver)
	conn, mock := util.InitSqlMockWithPrettyPrint(t, true)
	db := backend_connectors.NewClickHouseBackendConnectorWithConnection("", conn)
	defer db.Close()
	lm := clickhouse.NewLogManagerWithConnection(db, util.NewSyncMapWith(testTableName, table))
	s := schema.StaticRegistry{
		Tables: map[schema.IndexName]schema.Schema{
			testTableName: {
				Fields: map[schema.FieldName]schema.Field{
					"client_name":           {PropertyName: "client_name", InternalPropertyName: "client_name", Type: schema.QuesmaTypeObject},
					"type":                  {PropertyName: "type", InternalPropertyName: "type", Type: schema.QuesmaTypeText},
					"name":                  {PropertyName: "name", InternalPropertyName: "name", Type: schema.QuesmaTypeText},
					"content":               {PropertyName: "content", InternalPropertyName: "content", Type: schema.QuesmaTypeText},
					"message":               {PropertyName: "message", InternalPropertyName: "message", Type: schema.QuesmaTypeText},
					"host_name.keyword":     {PropertyName: "host_name.keyword", InternalPropertyName: "host_name.keyword", Type: schema.QuesmaTypeKeyword},
					"FlightDelay":           {PropertyName: "FlightDelay", InternalPropertyName: "FlightDelay", Type: schema.QuesmaTypeText},
					"Cancelled":             {PropertyName: "Cancelled", InternalPropertyName: "Cancelled", Type: schema.QuesmaTypeText},
					"FlightDelayMin":        {PropertyName: "FlightDelayMin", InternalPropertyName: "FlightDelayMin", Type: schema.QuesmaTypeText},
					"_id":                   {PropertyName: "_id", InternalPropertyName: "_id", Type: schema.QuesmaTypeText},
					"epoch_time":            {PropertyName: "epoch_time", InternalPropertyName: "epoch_time", Type: schema.QuesmaTypeDate},
					"epoch_time_datetime64": {PropertyName: "epoch_time_datetime64", InternalPropertyName: "epoch_time_datetime64", Type: schema.QuesmaTypeDate},
					"map_name":              {PropertyName: "map_name", InternalPropertyName: "map_name", InternalPropertyType: "Map(String, Nullable(String))", Type: schema.QuesmaTypeMap},
				},
				Aliases: map[schema.FieldName]schema.FieldName{
					"client.name": "client_name",
				},
			},
		},
	}
	ctx = context.WithValue(context.Background(), tracing.RequestIdCtxKey, "test")
	qt := &elastic_query_dsl.ClickhouseQueryTranslator{Table: table, Ctx: ctx, Schema: s.Tables[schema.IndexName(testTableName)]}
	// Here we additionally verify that terms for `_tier` are **NOT** included in the SQL query
	expectedQuery1 := fmt.Sprintf(`SELECT DISTINCT %s FROM %s WHERE (("epoch_time">=fromUnixTimestamp(1709036700) AND "epoch_time"<=fromUnixTimestamp(1709037659)) AND ("epoch_time_datetime64">=fromUnixTimestamp64Milli(1709036700000) AND "epoch_time_datetime64"<=fromUnixTimestamp64Milli(1709037659999))) LIMIT 13`, fieldName, testTableName)
	expectedQuery2 := fmt.Sprintf(`SELECT DISTINCT %s FROM %s WHERE (("epoch_time">=fromUnixTimestamp(1709036700) AND "epoch_time"<=fromUnixTimestamp(1709037659)) AND ("epoch_time_datetime64">=fromUnixTimestamp64Milli(1709036700000) AND "epoch_time_datetime64"<=fromUnixTimestamp64Milli(1709037659999))) LIMIT 13`, fieldName, testTableName)

	// Once in a while `AND` conditions could be swapped, so we match both cases
	mock.ExpectQuery(fmt.Sprintf("%s|%s", regexp.QuoteMeta(expectedQuery1), regexp.QuoteMeta(expectedQuery2))).
		WillReturnRows(sqlmock.NewRows([]string{"client_name"}).AddRow("client_a").AddRow("client_b"))

	const isFieldMapSyntaxEnabled = true // in most test cases it doesn't change anything and can be either. If it does, then we want it 'true'
	resp, err := handleTermsEnumRequest(ctx, types.MustJSON(string(requestBody)), lm, qt, isFieldMapSyntaxEnabled, managementConsole)
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
	testHandleTermsEnumRequest(t, rawRequestBody, `"client_name"`)
}

// Basic test.
// "client.name" should be replaced by "client_name", and results should stay the same
func TestIfHandleTermsEnumUsesSchema(t *testing.T) {
	requestBodyWithAliasedField := bytes.ReplaceAll(rawRequestBody, []byte(`"field": "client_name"`), []byte(`"field": "client.name"`))
	testHandleTermsEnumRequest(t, requestBodyWithAliasedField, `"client_name"`)
}

func TestIfHandleTermsEnumUsesSchemaForMapColumn(t *testing.T) {
	requestBodyWithAliasedField := bytes.ReplaceAll(rawRequestBody, []byte(`"field": "client_name"`), []byte(`"field": "map_name.key_name"`))
	testHandleTermsEnumRequest(t, requestBodyWithAliasedField, "arrayElement(\"map_name\",'key_name')")
}
