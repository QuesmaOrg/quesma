// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package quesma

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/k0kubun/pp"
	"github.com/stretchr/testify/assert"
	"quesma/ab_testing"
	"quesma/clickhouse"
	"quesma/concurrent"
	"quesma/logger"
	"quesma/model"
	"quesma/quesma/config"
	"quesma/quesma/types"
	"quesma/quesma/ui"
	"quesma/schema"
	"quesma/telemetry"
	"quesma/testdata"
	"quesma/tracing"
	"quesma/util"
	"strconv"
	"strings"
	"testing"
)

const defaultAsyncSearchTimeout = 1000

const tableName = model.SingleTableNamePlaceHolder

var ctx = context.WithValue(context.TODO(), tracing.RequestIdCtxKey, tracing.GetRequestId())

func TestAsyncSearchHandler(t *testing.T) {
	// logger.InitSimpleLoggerForTests()
	cfg := config.QuesmaConfiguration{IndexConfig: map[string]config.IndexConfiguration{tableName: {}}}

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
			"properties::isreg": {
				Name: "properties::isreg",
				Type: clickhouse.NewBaseType("UInt8"),
			},
		},
		Created: true,
	})
	s := schema.StaticRegistry{
		Tables: map[schema.TableName]schema.Schema{
			model.SingleTableNamePlaceHolder: {
				Fields: map[schema.FieldName]schema.Field{
					"host.name":         {PropertyName: "host.name", InternalPropertyName: "host.name", Type: schema.QuesmaTypeObject},
					"type":              {PropertyName: "type", InternalPropertyName: "type", Type: schema.QuesmaTypeKeyword},
					"name":              {PropertyName: "name", InternalPropertyName: "name", Type: schema.QuesmaTypeKeyword},
					"content":           {PropertyName: "content", InternalPropertyName: "content", Type: schema.QuesmaTypeKeyword},
					"message":           {PropertyName: "message", InternalPropertyName: "message", Type: schema.QuesmaTypeText},
					"host_name.keyword": {PropertyName: "host_name.keyword", InternalPropertyName: "host_name.keyword", Type: schema.QuesmaTypeKeyword},
					"FlightDelay":       {PropertyName: "FlightDelay", InternalPropertyName: "FlightDelay", Type: schema.QuesmaTypeKeyword},
					"Cancelled":         {PropertyName: "Cancelled", InternalPropertyName: "Cancelled", Type: schema.QuesmaTypeKeyword},
					"FlightDelayMin":    {PropertyName: "FlightDelayMin", InternalPropertyName: "FlightDelayMin", Type: schema.QuesmaTypeKeyword},
					"_id":               {PropertyName: "_id", InternalPropertyName: "_id", Type: schema.QuesmaTypeKeyword},
				},
			},
		},
	}

	for i, tt := range testdata.TestsAsyncSearch {
		t.Run(fmt.Sprintf("%s(%d)", tt.Name, i), func(t *testing.T) {
			db, mock := util.InitSqlMockWithPrettyPrint(t, false)
			defer db.Close()
			lm := clickhouse.NewLogManagerWithConnection(db, table)
			managementConsole := ui.NewQuesmaManagementConsole(&cfg, nil, nil, make(<-chan logger.LogWithLevel, 50000), telemetry.NewPhoneHomeEmptyAgent(), nil)

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
			queryRunner := NewQueryRunner(lm, &cfg, nil, managementConsole, s, ab_testing.NewEmptySender())
			_, err := queryRunner.handleAsyncSearch(ctx, tableName, types.MustJSON(tt.QueryJson), defaultAsyncSearchTimeout, true)
			assert.NoError(t, err)

			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal("there were unfulfilled expections:", err)
			}
		})
	}
}

func TestAsyncSearchHandlerSpecialCharacters(t *testing.T) {
	cfg := config.QuesmaConfiguration{IndexConfig: map[string]config.IndexConfiguration{tableName: {}}}
	table := clickhouse.Table{
		Name:   tableName,
		Config: clickhouse.NewDefaultCHConfig(),
		Cols: map[string]*clickhouse.Column{
			"-@timestamp":  {Name: "-@timestamp", Type: clickhouse.NewBaseType("DateTime64")},
			"message$*%:;": {Name: "message$*%:;", Type: clickhouse.NewBaseType("String")},
			"-@bytes":      {Name: "-@bytes", Type: clickhouse.NewBaseType("Int64")},
		},
		Created: true,
	}

	s := schema.StaticRegistry{
		Tables: map[schema.TableName]schema.Schema{
			"logs-generic-default": {
				Fields: map[schema.FieldName]schema.Field{
					"host.name":         {PropertyName: "host.name", InternalPropertyName: "host.name", Type: schema.QuesmaTypeObject},
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
			},
		},
	}
	for i, tt := range testdata.AggregationTestsWithSpecialCharactersInFieldNames {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			db, mock := util.InitSqlMockWithPrettyPrint(t, false)
			defer db.Close()
			lm := clickhouse.NewLogManagerWithConnection(db, concurrent.NewMapWith(tableName, &table))
			managementConsole := ui.NewQuesmaManagementConsole(&cfg, nil, nil, make(<-chan logger.LogWithLevel, 50000), telemetry.NewPhoneHomeEmptyAgent(), nil)

			for _, expectedSql := range tt.ExpectedSQLs {
				mock.ExpectQuery(testdata.EscapeBrackets(expectedSql)).WillReturnRows(sqlmock.NewRows([]string{"@timestamp", "host.name"}))
			}

			queryRunner := NewQueryRunner(lm, &cfg, nil, managementConsole, s, ab_testing.NewEmptySender())
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
			Name: "message",
			Type: clickhouse.NewBaseType("String"),
		},
	},
	Created: true,
})

func TestSearchHandler(t *testing.T) {
	cfg := config.QuesmaConfiguration{IndexConfig: map[string]config.IndexConfiguration{tableName: {}}}
	s := schema.StaticRegistry{
		Tables: map[schema.TableName]schema.Schema{
			model.SingleTableNamePlaceHolder: {
				Fields: map[schema.FieldName]schema.Field{
					"host.name":         {PropertyName: "host.name", InternalPropertyName: "host.name", Type: schema.QuesmaTypeObject},
					"type":              {PropertyName: "type", InternalPropertyName: "type", Type: schema.QuesmaTypeKeyword},
					"name":              {PropertyName: "name", InternalPropertyName: "name", Type: schema.QuesmaTypeKeyword},
					"content":           {PropertyName: "content", InternalPropertyName: "content", Type: schema.QuesmaTypeKeyword},
					"message":           {PropertyName: "message", InternalPropertyName: "message", Type: schema.QuesmaTypeText},
					"host_name.keyword": {PropertyName: "host_name.keyword", InternalPropertyName: "host_name.keyword", Type: schema.QuesmaTypeKeyword},
					"FlightDelay":       {PropertyName: "FlightDelay", InternalPropertyName: "FlightDelay", Type: schema.QuesmaTypeKeyword},
					"Cancelled":         {PropertyName: "Cancelled", InternalPropertyName: "Cancelled", Type: schema.QuesmaTypeKeyword},
					"FlightDelayMin":    {PropertyName: "FlightDelayMin", InternalPropertyName: "FlightDelayMin", Type: schema.QuesmaTypeKeyword},
					"_id":               {PropertyName: "_id", InternalPropertyName: "_id", Type: schema.QuesmaTypeKeyword},
				},
			},
		},
	}
	for i, tt := range testdata.TestsSearch {
		t.Run(fmt.Sprintf("%s(%d)", tt.Name, i), func(t *testing.T) {
			db, mock := util.InitSqlMockWithPrettyPrint(t, false)
			defer db.Close()

			lm := clickhouse.NewLogManagerWithConnection(db, table)
			managementConsole := ui.NewQuesmaManagementConsole(&cfg, nil, nil, make(<-chan logger.LogWithLevel, 50000), telemetry.NewPhoneHomeEmptyAgent(), nil)
			for _, wantedRegex := range tt.WantedRegexes {

				// This test reuses test cases suited for query generator
				// In this case pipeline transformations are triggered by query runner.
				// So we should have a different expectation here.

				// HACK. we change expectations here
				wantedRegex = strings.ReplaceAll(wantedRegex, model.FullTextFieldNamePlaceHolder, "message")

				mock.ExpectQuery(testdata.EscapeWildcard(testdata.EscapeBrackets(wantedRegex))).
					WillReturnRows(sqlmock.NewRows([]string{"@timestamp", "host.name"}))
			}
			queryRunner := NewQueryRunner(lm, &cfg, nil, managementConsole, s, ab_testing.NewEmptySender())
			_, _ = queryRunner.handleSearch(ctx, tableName, types.MustJSON(tt.QueryJson))

			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal("there were unfulfilled expections:", err)
			}
		})
	}
}

// TODO this test gives wrong results??
func TestSearchHandlerNoAttrsConfig(t *testing.T) {
	cfg := config.QuesmaConfiguration{IndexConfig: map[string]config.IndexConfiguration{tableName: {}}}
	s := schema.StaticRegistry{
		Tables: map[schema.TableName]schema.Schema{
			"logs-generic-default": {
				Fields: map[schema.FieldName]schema.Field{
					"host.name":         {PropertyName: "host.name", InternalPropertyName: "host.name", Type: schema.QuesmaTypeObject},
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
			},
		},
	}
	for _, tt := range testdata.TestsSearchNoAttrs {
		t.Run(tt.Name, func(t *testing.T) {
			db, mock := util.InitSqlMockWithPrettyPrint(t, false)
			defer db.Close()

			lm := clickhouse.NewLogManagerWithConnection(db, table)
			managementConsole := ui.NewQuesmaManagementConsole(&cfg, nil, nil, make(<-chan logger.LogWithLevel, 50000), telemetry.NewPhoneHomeEmptyAgent(), nil)
			for _, wantedRegex := range tt.WantedRegexes {
				mock.ExpectQuery(testdata.EscapeBrackets(wantedRegex)).WillReturnRows(sqlmock.NewRows([]string{"@timestamp", "host.name"}))
			}
			queryRunner := NewQueryRunner(lm, &cfg, nil, managementConsole, s, ab_testing.NewEmptySender())
			_, _ = queryRunner.handleSearch(ctx, tableName, types.MustJSON(tt.QueryJson))

			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal("there were unfulfilled expections:", err)
			}
		})
	}
}

func TestAsyncSearchFilter(t *testing.T) {
	cfg := config.QuesmaConfiguration{IndexConfig: map[string]config.IndexConfiguration{tableName: {}}}
	s := schema.StaticRegistry{
		Tables: map[schema.TableName]schema.Schema{
			"logs-generic-default": {
				Fields: map[schema.FieldName]schema.Field{
					"host.name":         {PropertyName: "host.name", InternalPropertyName: "host.name", Type: schema.QuesmaTypeObject},
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
			},
		},
	}
	for _, tt := range testdata.TestSearchFilter {
		t.Run(tt.Name, func(t *testing.T) {
			db, mock := util.InitSqlMockWithPrettyPrint(t, false)
			defer db.Close()

			lm := clickhouse.NewLogManagerWithConnection(db, table)
			managementConsole := ui.NewQuesmaManagementConsole(&cfg, nil, nil, make(<-chan logger.LogWithLevel, 50000), telemetry.NewPhoneHomeEmptyAgent(), nil)
			for _, wantedRegex := range tt.WantedRegexes {
				mock.ExpectQuery(testdata.EscapeBrackets(wantedRegex)).WillReturnRows(sqlmock.NewRows([]string{"@timestamp", "host.name"}))
			}
			queryRunner := NewQueryRunner(lm, &cfg, nil, managementConsole, s, ab_testing.NewEmptySender())
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
	cfg := config.QuesmaConfiguration{IndexConfig: map[string]config.IndexConfiguration{tableName: {}}}
	s := schema.StaticRegistry{
		Tables: map[schema.TableName]schema.Schema{
			"logs-generic-default": {
				Fields: map[schema.FieldName]schema.Field{
					"host.name":         {PropertyName: "host.name", InternalPropertyName: "host.name", Type: schema.QuesmaTypeObject},
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
			},
		},
	}
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

	db, mock := util.InitSqlMockWithPrettyPrint(t, false)

	defer db.Close()
	lm := clickhouse.NewLogManagerWithConnection(db, concurrent.NewMapWith(tableName, &table))
	managementConsole := ui.NewQuesmaManagementConsole(&cfg, nil, nil, make(<-chan logger.LogWithLevel, 50000), telemetry.NewPhoneHomeEmptyAgent(), nil)

	for _, fieldName := range []string{dateTimeTimestampField, dateTime64TimestampField, dateTime64OurTimestampField} {

		mock.ExpectQuery(testdata.EscapeBrackets(expectedSelectStatementRegex[fieldName])).
			WillReturnRows(sqlmock.NewRows([]string{"key", "doc_count"}))

		// .AddRow(1000, uint64(10)).AddRow(1001, uint64(20))) // here rows should be added if uint64 were supported
		queryRunner := NewQueryRunner(lm, &cfg, nil, managementConsole, s, ab_testing.NewEmptySender())
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
	cfg := config.QuesmaConfiguration{IndexConfig: map[string]config.IndexConfiguration{tableName: {}}}
	s := schema.StaticRegistry{
		Tables: map[schema.TableName]schema.Schema{
			"logs-generic-default": {
				Fields: map[schema.FieldName]schema.Field{
					"host.name":         {PropertyName: "host.name", InternalPropertyName: "host.name", Type: schema.QuesmaTypeObject},
					"type":              {PropertyName: "type", InternalPropertyName: "type", Type: schema.QuesmaTypeText},
					"name":              {PropertyName: "name", InternalPropertyName: "name", Type: schema.QuesmaTypeText},
					"content":           {PropertyName: "content", InternalPropertyName: "content", Type: schema.QuesmaTypeText},
					"message":           {PropertyName: "message", InternalPropertyName: "message", Type: schema.QuesmaTypeText},
					"host.name.keyword": {PropertyName: "host.name.keyword", InternalPropertyName: "host.name.keyword", Type: schema.QuesmaTypeKeyword},
					"FlightDelay":       {PropertyName: "FlightDelay", InternalPropertyName: "FlightDelay", Type: schema.QuesmaTypeText},
					"Cancelled":         {PropertyName: "Cancelled", InternalPropertyName: "Cancelled", Type: schema.QuesmaTypeText},
					"FlightDelayMin":    {PropertyName: "FlightDelayMin", InternalPropertyName: "FlightDelayMin", Type: schema.QuesmaTypeText},
				},
			},
		},
	}
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
				db, mock := util.InitSqlMockWithPrettyPrint(t, false)
				defer db.Close()
				lm := clickhouse.NewLogManagerWithConnection(db, table)
				managementConsole := ui.NewQuesmaManagementConsole(&cfg, nil, nil, make(<-chan logger.LogWithLevel, 50000), telemetry.NewPhoneHomeEmptyAgent(), nil)

				returnedBuckets := sqlmock.NewRows([]string{"", ""})
				for _, row := range tt.ResultRows {
					returnedBuckets.AddRow(row[0], row[1])
				}

				// count, present in all tests
				mock.ExpectQuery(`SELECT count\(\) FROM ` + tableName).WillReturnRows(sqlmock.NewRows([]string{"count"}))
				// Don't care about the query's SQL in this test, it's thoroughly tested in different tests, thus ""
				mock.ExpectQuery("").WillReturnRows(returnedBuckets)

				queryRunner := NewQueryRunner(lm, &cfg, nil, managementConsole, s, ab_testing.NewEmptySender())
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

	cfg := config.QuesmaConfiguration{IndexConfig: map[string]config.IndexConfiguration{tableName: {}}}
	s := schema.StaticRegistry{Tables: map[schema.TableName]schema.Schema{}}

	s.Tables[tableName] = schema.Schema{
		Fields: map[schema.FieldName]schema.Field{
			"host.name":         {PropertyName: "host.name", InternalPropertyName: "host.name", Type: schema.QuesmaTypeObject},
			"type":              {PropertyName: "type", InternalPropertyName: "type", Type: schema.QuesmaTypeText},
			"name":              {PropertyName: "name", InternalPropertyName: "name", Type: schema.QuesmaTypeText},
			"content":           {PropertyName: "content", InternalPropertyName: "content", Type: schema.QuesmaTypeText},
			"message":           {PropertyName: "message", InternalPropertyName: "message", Type: schema.QuesmaTypeText},
			"host.name.keyword": {PropertyName: "host.name.keyword", InternalPropertyName: "host.name.keyword", Type: schema.QuesmaTypeKeyword},
			"FlightDelay":       {PropertyName: "FlightDelay", InternalPropertyName: "FlightDelay", Type: schema.QuesmaTypeText},
			"Cancelled":         {PropertyName: "Cancelled", InternalPropertyName: "Cancelled", Type: schema.QuesmaTypeText},
			"FlightDelayMin":    {PropertyName: "FlightDelayMin", InternalPropertyName: "FlightDelayMin", Type: schema.QuesmaTypeText},
		},
	}

	var table = concurrent.NewMapWith(tableName, &clickhouse.Table{
		Name:   tableName,
		Config: clickhouse.NewChTableConfigTimestampStringAttr(),
		Cols: map[string]*clickhouse.Column{
			// only one field because currently we have non-determinism in translating * -> all fields :( and can't regex that easily.
			// (TODO Maybe we can, don't want to waste time for this now https://stackoverflow.com/questions/3533408/regex-i-want-this-and-that-and-that-in-any-order)
			"message": {
				Name: "message",
				Type: clickhouse.NewBaseType("String"),
			},
		},
		Created: true,
	})

	test := func(t *testing.T, handlerName string, testcase testdata.FullSearchTestCase) {
		db, mock := util.InitSqlMockWithPrettyPrint(t, false)
		defer db.Close()
		lm := clickhouse.NewLogManagerWithConnection(db, table)
		managementConsole := ui.NewQuesmaManagementConsole(&cfg, nil, nil, make(<-chan logger.LogWithLevel, 50000), telemetry.NewPhoneHomeEmptyAgent(), nil)

		for i, sql := range testcase.ExpectedSQLs {
			rows := sqlmock.NewRows([]string{testcase.ExpectedSQLResults[i][0].Cols[0].ColName})
			for _, row := range testcase.ExpectedSQLResults[i] {
				rows.AddRow(row.Cols[0].Value)
			}
			mock.ExpectQuery(testdata.EscapeBrackets(sql)).WillReturnRows(rows)
		}

		queryRunner := NewQueryRunner(lm, &cfg, nil, managementConsole, s, ab_testing.NewEmptySender())

		var response []byte
		var err error

		if handlerName == "handleSearch" {
			response, err = queryRunner.handleSearch(ctx, tableName, types.MustJSON(testcase.QueryRequestJson))
		} else if handlerName == "handleAsyncSearch" {
			response, err = queryRunner.handleAsyncSearch(
				ctx, tableName, types.MustJSON(testcase.QueryRequestJson), defaultAsyncSearchTimeout, true)
		}
		if err != nil {
			t.Fatal(err)
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

		assert.NotNil(t, testcase.ExpectedResponse, "ExpectedResponse is nil")
		expectedResponseMap, err := util.JsonToMap(testcase.ExpectedResponse)
		assert.NoError(t, err, "error unmarshalling expected response:")

		actualMinusExpected, expectedMinusActual := util.MapDifference(responsePart,
			expectedResponseMap, []string{}, true, true)
		acceptableDifference := []string{"took", "_shards", "timed_out"}

		pp.Println("expected", expectedResponseMap)
		pp.Println("actual", responsePart)

		assert.True(t, util.AlmostEmpty(actualMinusExpected, acceptableDifference), "actualMinusExpected: %v", actualMinusExpected)
		assert.True(t, util.AlmostEmpty(expectedMinusActual, acceptableDifference), "expectedMinusActual: %v", expectedMinusActual)
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
