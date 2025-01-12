// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package quesma

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/goccy/go-json"
	"github.com/k0kubun/pp"
	"github.com/stretchr/testify/assert"
	"quesma/backend_connectors"
	"quesma/clickhouse"
	"quesma/logger"
	"quesma/model"
	"quesma/quesma/config"
	"quesma/quesma/types"
	"quesma/schema"
	"quesma/testdata"
	"quesma/util"
	tracing "quesma_v2/core/tracing"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

const defaultAsyncSearchTimeout = 1000

const tableName = model.SingleTableNamePlaceHolder

var DefaultConfig = config.QuesmaConfiguration{
	IndexConfig: map[string]config.IndexConfiguration{
		tableName: {
			QueryTarget: []string{config.ClickhouseTarget}, IngestTarget: []string{config.ClickhouseTarget},
		},
	},
}

var ctx = context.WithValue(context.TODO(), tracing.RequestIdCtxKey, tracing.GetRequestId())

func TestAsyncSearchHandler(t *testing.T) {
	// logger.InitSimpleLoggerForTests()

	table := util.NewSyncMapWith(tableName, &clickhouse.Table{
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
			"host_name": {
				Name: "host_name",
				Type: clickhouse.NewBaseType("LowCardinality(String)"),
			},
			"properties_isreg": {
				Name: "properties_isreg",
				Type: clickhouse.NewBaseType("UInt8"),
			},
			"event_dataset": {
				Name: "event_dataset",
				Type: clickhouse.NewBaseType("String"),
			},
		},
		Created: true,
	})
	fields := map[schema.FieldName]schema.Field{
		"@timestamp":        {PropertyName: "@timestamp", InternalPropertyName: "@timestamp", Type: schema.QuesmaTypeDate},
		"message":           {PropertyName: "message", InternalPropertyName: "message", Type: schema.QuesmaTypeText},
		"host.name":         {PropertyName: "host.name", InternalPropertyName: "host_name", Type: schema.QuesmaTypeObject},
		"properties::isreg": {PropertyName: "properties::isreg", InternalPropertyName: "properties_isreg", Type: schema.QuesmaTypeInteger},
		"event.dataset":     {PropertyName: "event.dataset", InternalPropertyName: "event_dataset", Type: schema.QuesmaTypeKeyword},
	}
	s := &schema.StaticRegistry{
		Tables: map[schema.IndexName]schema.Schema{
			model.SingleTableNamePlaceHolder: schema.NewSchemaWithAliases(fields, map[schema.FieldName]schema.FieldName{}, true, ""),
		},
	}

	for i, tt := range testdata.TestsAsyncSearch {
		t.Run(fmt.Sprintf("%s(%d)", tt.Name, i), func(t *testing.T) {
			conn, mock := util.InitSqlMockWithPrettySqlAndPrint(t, false)
			db := backend_connectors.NewClickHouseBackendConnectorWithConnection("", conn)
			defer conn.Close()

			for _, query := range tt.WantedQuery {
				mock.ExpectQuery(query).WillReturnRows(sqlmock.NewRows([]string{"@timestamp", "host.name"}))
			}

			queryRunner := NewQueryRunnerDefaultForTests(db, &DefaultConfig, tableName, table, s)
			_, err := queryRunner.HandleAsyncSearch(ctx, tableName, types.MustJSON(tt.QueryJson), defaultAsyncSearchTimeout, true)
			assert.NoError(t, err)

			if err = mock.ExpectationsWereMet(); err != nil {
				t.Fatal("there were unfulfilled expections:", err)
			}
		})
	}
}

func TestAsyncSearchHandlerSpecialCharacters(t *testing.T) {
	table := clickhouse.Table{
		Name:   tableName,
		Config: clickhouse.NewDefaultCHConfig(),
		Cols: map[string]*clickhouse.Column{
			"__timestamp":  {Name: "__timestamp", Type: clickhouse.NewBaseType("DateTime64")},
			"message_____": {Name: "message_____", Type: clickhouse.NewBaseType("String")},
			"__bytes":      {Name: "__bytes", Type: clickhouse.NewBaseType("Int64")},
		},
		Created: true,
	}
	fields := map[schema.FieldName]schema.Field{
		"host.name":         {PropertyName: "host.name", InternalPropertyName: "host_name", Type: schema.QuesmaTypeObject},
		"type":              {PropertyName: "type", InternalPropertyName: "type", Type: schema.QuesmaTypeText},
		"name":              {PropertyName: "name", InternalPropertyName: "name", Type: schema.QuesmaTypeText},
		"content":           {PropertyName: "content", InternalPropertyName: "content", Type: schema.QuesmaTypeText},
		"message":           {PropertyName: "message", InternalPropertyName: "message", Type: schema.QuesmaTypeText},
		"host_name.keyword": {PropertyName: "host_name.keyword", InternalPropertyName: "host_name_keyword", Type: schema.QuesmaTypeKeyword},
		"FlightDelay":       {PropertyName: "FlightDelay", InternalPropertyName: "flightdelay", Type: schema.QuesmaTypeText},
		"Cancelled":         {PropertyName: "Cancelled", InternalPropertyName: "cancelled", Type: schema.QuesmaTypeText},
		"FlightDelayMin":    {PropertyName: "FlightDelayMin", InternalPropertyName: "flightdelaymin", Type: schema.QuesmaTypeText},
		"_id":               {PropertyName: "_id", InternalPropertyName: "_id", Type: schema.QuesmaTypeText},
		"message$*%:;":      {PropertyName: "message$*%:;", InternalPropertyName: "message_____", Type: schema.QuesmaTypeText},
		"-@timestamp":       {PropertyName: "-@timestamp", InternalPropertyName: "__timestamp", Type: schema.QuesmaTypeDate},
		"-@bytes":           {PropertyName: "-@bytes", InternalPropertyName: "__bytes", Type: schema.QuesmaTypeInteger},
	}

	s := &schema.StaticRegistry{
		Tables: map[schema.IndexName]schema.Schema{
			tableName: schema.NewSchemaWithAliases(fields, map[schema.FieldName]schema.FieldName{}, true, ""),
		},
	}

	for i, tt := range testdata.AggregationTestsWithSpecialCharactersInFieldNames {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			conn, mock := util.InitSqlMockWithPrettySqlAndPrint(t, false)
			defer conn.Close()
			db := backend_connectors.NewClickHouseBackendConnectorWithConnection("", conn)

			mock.ExpectQuery(tt.ExpectedPancakeSQL).WillReturnRows(sqlmock.NewRows([]string{"@timestamp", "host.name"}))

			queryRunner := NewQueryRunnerDefaultForTests(db, &DefaultConfig, tableName, util.NewSyncMapWith(tableName, &table), s)
			_, err := queryRunner.HandleAsyncSearch(ctx, tableName, types.MustJSON(tt.QueryRequestJson), defaultAsyncSearchTimeout, true)
			assert.NoError(t, err)

			if err = mock.ExpectationsWereMet(); err != nil {
				t.Fatal("there were unfulfilled expections:", err)
			}
		})
	}
}

var table = util.NewSyncMapWith(tableName, &clickhouse.Table{
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

	tab := &clickhouse.Table{
		Name:   tableName,
		Config: clickhouse.NewChTableConfigTimestampStringAttr(),
		Cols: map[string]*clickhouse.Column{
			"message": {
				Name: "message",
				Type: clickhouse.NewBaseType("String"),
			},
			"type": {
				Name: "type",
				Type: clickhouse.NewBaseType("String"),
			},
			"task_enabled": {
				Name: "task_enabled",
				Type: clickhouse.NewBaseType("String"),
			},
			"@timestamp": {
				Name: "@timestamp",
				Type: clickhouse.NewBaseType("DateTime64"),
			},
			"user_id": {
				Name: "user_id",
				Type: clickhouse.NewBaseType("String"),
			},
			"tags": {
				Name: "tags",
				Type: clickhouse.NewBaseType("String"),
			},
			"age": {
				Name: "age",
				Type: clickhouse.NewBaseType("UInt8"),
			},
			"host_name": {
				Name: "host_name",
				Type: clickhouse.NewBaseType("String"),
			},
			"status": {
				Name: "status",
				Type: clickhouse.NewBaseType("String"),
			},
			"namespace": {
				Name: "namespace",
				Type: clickhouse.NewBaseType("String"),
			},
			"namespaces": {
				Name: "namespaces",
				Type: clickhouse.NewBaseType("String"),
			},
			"cliIP": {
				Name: "cliIP",
				Type: clickhouse.NewBaseType("String"),
			},
			"field": {
				Name: "field",
				Type: clickhouse.NewBaseType("String"),
			},
			"exception-list-agnostic_list_id": {
				Name: "exception-list-agnostic_list_id",
				Type: clickhouse.NewBaseType("String"),
			},
			"task_taskType": {
				Name: "task.taskType",
				Type: clickhouse.NewBaseType("String"),
			},
			"alert_actions_actionRef": {
				Name: "alert_actions_actionRef",
				Type: clickhouse.NewBaseType("String"),
			},
			"user": {
				Name: "user",
				Type: clickhouse.NewBaseType("String"),
			},
			"references_type": {
				Name: "references_type",
				Type: clickhouse.NewBaseType("String"),
			},
			"stream_namespace": {
				Name: "stream_namespace",
				Type: clickhouse.NewBaseType("String"),
			},
			"service_name": {
				Name: "service_name",
				Type: clickhouse.NewBaseType("String"),
			},
		},
		Created: true,
	}

	tableMap := util.NewSyncMapWith(tableName, tab)

	fields := map[schema.FieldName]schema.Field{
		"message":                         {PropertyName: "message", InternalPropertyName: "message", Type: schema.QuesmaTypeText},
		"type":                            {PropertyName: "type", InternalPropertyName: "type", Type: schema.QuesmaTypeKeyword},
		"task.enabled":                    {PropertyName: "task.enabled", InternalPropertyName: "task_enabled", Type: schema.QuesmaTypeBoolean},
		"@timestamp":                      {PropertyName: "@timestamp", InternalPropertyName: "@timestamp", Type: schema.QuesmaTypeDate},
		"user.id":                         {PropertyName: "user.id", InternalPropertyName: "user_id", Type: schema.QuesmaTypeKeyword},
		"tags":                            {PropertyName: "tags", InternalPropertyName: "tags", Type: schema.QuesmaTypeKeyword},
		"age":                             {PropertyName: "age", InternalPropertyName: "age", Type: schema.QuesmaTypeInteger},
		"host.name":                       {PropertyName: "host.name", InternalPropertyName: "host_name", Type: schema.QuesmaTypeObject},
		"status":                          {PropertyName: "status", InternalPropertyName: "status", Type: schema.QuesmaTypeKeyword},
		"namespace":                       {PropertyName: "namespace", InternalPropertyName: "namespace", Type: schema.QuesmaTypeKeyword},
		"namespaces":                      {PropertyName: "namespaces", InternalPropertyName: "namespaces", Type: schema.QuesmaTypeKeyword},
		"cliIP":                           {PropertyName: "cliIP", InternalPropertyName: "cliIP", Type: schema.QuesmaTypeKeyword},
		"field":                           {PropertyName: "field", InternalPropertyName: "field", Type: schema.QuesmaTypeKeyword},
		"exception-list-agnostic.list_id": {PropertyName: "exception-list-agnostic.list_id", InternalPropertyName: "exception-list-agnostic_list_id", Type: schema.QuesmaTypeKeyword},
		"task.taskType":                   {PropertyName: "task.taskType", InternalPropertyName: "task_taskType", Type: schema.QuesmaTypeKeyword},
		"alert.actions.actionRef":         {PropertyName: "alert.actions.actionRef", InternalPropertyName: "alert_actions_actionRef", Type: schema.QuesmaTypeKeyword},
		"user":                            {PropertyName: "user", InternalPropertyName: "user", Type: schema.QuesmaTypeKeyword},
		"references.type":                 {PropertyName: "references.type", InternalPropertyName: "references_type", Type: schema.QuesmaTypeKeyword},
		"stream.namespace":                {PropertyName: "stream.namespace", InternalPropertyName: "stream_namespace", Type: schema.QuesmaTypeKeyword},
		"service.name":                    {PropertyName: "service.name", InternalPropertyName: "service_name", Type: schema.QuesmaTypeKeyword},
	}

	var selectColumns []string
	for k := range tab.Cols {
		selectColumns = append(selectColumns, strconv.Quote(k))
	}
	sort.Strings(selectColumns)

	testSuiteSelectPlaceHolder := "SELECT \"message\""
	selectCMD := fmt.Sprintf("SELECT %s ", strings.Join(selectColumns, ", "))

	s := &schema.StaticRegistry{
		Tables: map[schema.IndexName]schema.Schema{
			model.SingleTableNamePlaceHolder: schema.NewSchemaWithAliases(fields, map[schema.FieldName]schema.FieldName{}, true, ""),
		},
	}

	for i, tt := range testdata.TestsSearch {
		t.Run(fmt.Sprintf("%s(%d)", tt.Name, i), func(t *testing.T) {
			var conn *sql.DB
			var mock sqlmock.Sqlmock
			if len(tt.WantedRegexes) > 0 {
				conn, mock = util.InitSqlMockWithPrettyPrint(t, false)
			} else {
				conn, mock = util.InitSqlMockWithPrettySqlAndPrint(t, false)
			}
			defer conn.Close()
			db := backend_connectors.NewClickHouseBackendConnectorWithConnection("", conn)

			if len(tt.WantedRegexes) > 0 {
				for _, wantedRegex := range tt.WantedRegexes {

					// This test reuses test cases suited for query generator
					// In this case pipeline transformations are triggered by query runner.
					// So we should have a different expectation here.

					// HACK. we change expectations here
					wantedRegex = strings.ReplaceAll(wantedRegex, model.FullTextFieldNamePlaceHolder, "message")

					if tt.WantedQueryType == model.ListAllFields && strings.HasPrefix(wantedRegex, testSuiteSelectPlaceHolder) {

						wantedRegex = strings.ReplaceAll(wantedRegex, testSuiteSelectPlaceHolder, selectCMD)

					}

					mock.ExpectQuery(testdata.EscapeWildcard(testdata.EscapeBrackets(wantedRegex))).
						WillReturnRows(sqlmock.NewRows([]string{"@timestamp", "host.name"}))
				}
			} else {
				for _, query := range tt.WantedQueries {
					mock.ExpectQuery(query).WillReturnRows(sqlmock.NewRows([]string{"@timestamp", "host.name"}))
				}
			}

			queryRunner := NewQueryRunnerDefaultForTests(db, &DefaultConfig, tableName, tableMap, s)
			_, err := queryRunner.HandleSearch(ctx, tableName, types.MustJSON(tt.QueryJson))
			assert.NoError(t, err)

			if err = mock.ExpectationsWereMet(); err != nil {
				t.Fatal("there were unfulfilled expections:", err)
			}
		})
	}
}

func TestSearchHandlerRuntimeMappings(t *testing.T) {
	fields := map[schema.FieldName]schema.Field{
		"@timestamp": {PropertyName: "@timestamp", InternalPropertyName: "@timestamp", Type: schema.QuesmaTypeDate},
		"message":    {PropertyName: "message", InternalPropertyName: "message", Type: schema.QuesmaTypeKeyword},
	}

	var table = util.NewSyncMapWith(tableName, &clickhouse.Table{
		Name:   tableName,
		Config: clickhouse.NewChTableConfigTimestampStringAttr(),
		Cols: map[string]*clickhouse.Column{
			"@timestamp": {
				Name: "@timestamp",
				Type: clickhouse.NewBaseType("DateTime64"),
			},
			"message": {
				Name: "message",
				Type: clickhouse.NewBaseType("String"),
			},
		},
		Created: true,
	})

	s := &schema.StaticRegistry{
		Tables: map[schema.IndexName]schema.Schema{
			model.SingleTableNamePlaceHolder: schema.NewSchemaWithAliases(fields, map[schema.FieldName]schema.FieldName{}, true, ""),
		},
	}
	for i, tt := range testdata.TestSearchRuntimeMappings {
		t.Run(fmt.Sprintf("%s(%d)", tt.Name, i), func(t *testing.T) {
			var conn *sql.DB
			var mock sqlmock.Sqlmock
			if len(tt.WantedRegexes) > 0 {
				conn, mock = util.InitSqlMockWithPrettyPrint(t, false)
			} else {
				conn, mock = util.InitSqlMockWithPrettySqlAndPrint(t, false)
			}
			defer conn.Close()
			db := backend_connectors.NewClickHouseBackendConnectorWithConnection("", conn)

			if len(tt.WantedRegexes) > 0 {
				for _, wantedRegex := range tt.WantedRegexes {
					mock.ExpectQuery(testdata.EscapeWildcard(testdata.EscapeBrackets(wantedRegex))).
						WillReturnRows(sqlmock.NewRows([]string{"@timestamp", "message"}))
				}
			} else {
				for _, query := range tt.WantedQueries {
					mock.ExpectQuery(query).WillReturnRows(sqlmock.NewRows([]string{"@timestamp", "message"}))
				}
			}

			queryRunner := NewQueryRunnerDefaultForTests(db, &DefaultConfig, tableName, table, s)
			_, err := queryRunner.HandleSearch(ctx, tableName, types.MustJSON(tt.QueryJson))
			assert.NoError(t, err)

			if err = mock.ExpectationsWereMet(); err != nil {
				t.Fatal("there were unfulfilled expections:", err)
			}
		})
	}
}

// TODO this test gives wrong results??
func TestSearchHandlerNoAttrsConfig(t *testing.T) {

	var table = util.NewSyncMapWith(tableName, &clickhouse.Table{
		Name:   tableName,
		Config: clickhouse.NewChTableConfigNoAttrs(),
		Cols: map[string]*clickhouse.Column{
			"message":    {Name: "message", Type: clickhouse.NewBaseType("String")},
			"@timestamp": {Name: "@timestamp", Type: clickhouse.NewBaseType("DateTime64")},
		},
		Created: true,
	})

	s := &schema.StaticRegistry{
		Tables: map[schema.IndexName]schema.Schema{
			tableName: {
				Fields: map[schema.FieldName]schema.Field{
					"message":    {PropertyName: "message", InternalPropertyName: "message", Type: schema.QuesmaTypeText},
					"@timestamp": {PropertyName: "@timestamp", InternalPropertyName: "@timestamp", Type: schema.QuesmaTypeTimestamp},
				},
			},
		},
	}

	for _, tt := range testdata.TestsSearchNoAttrs {
		t.Run(tt.Name, func(t *testing.T) {
			conn, mock := util.InitSqlMockWithPrettyPrint(t, false)
			defer conn.Close()
			db := backend_connectors.NewClickHouseBackendConnectorWithConnection("", conn)

			for _, wantedRegex := range tt.WantedRegexes {
				mock.ExpectQuery(testdata.EscapeBrackets(wantedRegex)).WillReturnRows(sqlmock.NewRows([]string{"@timestamp", "host.name"}))
			}

			queryRunner := NewQueryRunnerDefaultForTests(db, &DefaultConfig, tableName, table, s)
			_, _ = queryRunner.HandleSearch(ctx, tableName, types.MustJSON(tt.QueryJson))

			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal("there were unfulfilled expections:", err)
			}
		})
	}
}

func TestAsyncSearchFilter(t *testing.T) {
	table := &clickhouse.Table{
		Name:   tableName,
		Config: clickhouse.NewChTableConfigTimestampStringAttr(),
		Cols: map[string]*clickhouse.Column{
			"message": {
				Name: "message",
				Type: clickhouse.NewBaseType("String"),
			},
			"@timestamp": {
				Name: "@timestamp",
				Type: clickhouse.NewBaseType("DateTime64"),
			},
			clickhouse.AttributesValuesColumn: {
				Name: clickhouse.AttributesValuesColumn,
				Type: clickhouse.BaseType{Name: "Map(String, String)"},
			},
		},
		Created: true,
	}
	s := &schema.StaticRegistry{
		Tables: map[schema.IndexName]schema.Schema{
			tableName: {
				Fields: map[schema.FieldName]schema.Field{
					"message":    {PropertyName: "message", InternalPropertyName: "message", Type: schema.QuesmaTypeText},
					"@timestamp": {PropertyName: "@timestamp", InternalPropertyName: "@timestamp", Type: schema.QuesmaTypeDate},
				},
			},
		},
	}
	for _, tt := range testdata.TestSearchFilter {
		t.Run(tt.Name, func(t *testing.T) {
			var conn *sql.DB
			var mock sqlmock.Sqlmock
			if len(tt.WantedRegexes) > 0 {
				conn, mock = util.InitSqlMockWithPrettyPrint(t, false)
			} else {
				conn, mock = util.InitSqlMockWithPrettySqlAndPrint(t, false)
			}
			db := backend_connectors.NewClickHouseBackendConnectorWithConnection("", conn)
			defer conn.Close()

			if len(tt.WantedRegexes) > 0 {
				for _, wantedRegex := range tt.WantedRegexes {
					mock.ExpectQuery(testdata.EscapeBrackets(wantedRegex)).WillReturnRows(sqlmock.NewRows([]string{"@timestamp", "host.name"}))
				}
			} else {
				for _, query := range tt.WantedQueries {
					mock.ExpectQuery(query).WillReturnRows(sqlmock.NewRows([]string{"@timestamp", "host.name"}))
				}
			}

			queryRunner := NewQueryRunnerDefaultForTests(db, &DefaultConfig, tableName, util.NewSyncMapWith(tableName, table), s)
			_, _ = queryRunner.HandleAsyncSearch(ctx, tableName, types.MustJSON(tt.QueryJson), defaultAsyncSearchTimeout, true)
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
	const dateTimeTimestampField = "timestamp"
	const dateTime64TimestampField = "timestamp64"
	const dateTime64OurTimestampField = "@timestamp"

	s := &schema.StaticRegistry{
		Tables: map[schema.IndexName]schema.Schema{
			tableName: {
				Fields: map[schema.FieldName]schema.Field{
					"host.name":                 {PropertyName: "host.name", InternalPropertyName: "host.name", Type: schema.QuesmaTypeObject},
					"type":                      {PropertyName: "type", InternalPropertyName: "type", Type: schema.QuesmaTypeText},
					"name":                      {PropertyName: "name", InternalPropertyName: "name", Type: schema.QuesmaTypeText},
					"content":                   {PropertyName: "content", InternalPropertyName: "content", Type: schema.QuesmaTypeText},
					"message":                   {PropertyName: "message", InternalPropertyName: "message", Type: schema.QuesmaTypeText},
					"host_name.keyword":         {PropertyName: "host_name.keyword", InternalPropertyName: "host_name.keyword", Type: schema.QuesmaTypeKeyword},
					"FlightDelay":               {PropertyName: "FlightDelay", InternalPropertyName: "FlightDelay", Type: schema.QuesmaTypeText},
					"Cancelled":                 {PropertyName: "Cancelled", InternalPropertyName: "Cancelled", Type: schema.QuesmaTypeText},
					"FlightDelayMin":            {PropertyName: "FlightDelayMin", InternalPropertyName: "FlightDelayMin", Type: schema.QuesmaTypeText},
					"_id":                       {PropertyName: "_id", InternalPropertyName: "_id", Type: schema.QuesmaTypeText},
					dateTimeTimestampField:      {PropertyName: dateTimeTimestampField, InternalPropertyName: dateTimeTimestampField, Type: schema.QuesmaTypeDate},
					dateTime64TimestampField:    {PropertyName: dateTime64TimestampField, InternalPropertyName: dateTime64TimestampField, Type: schema.QuesmaTypeDate},
					dateTime64OurTimestampField: {PropertyName: dateTime64OurTimestampField, InternalPropertyName: dateTime64OurTimestampField, Type: schema.QuesmaTypeDate},
				},
			},
		},
	}

	table := clickhouse.Table{Name: tableName, Config: clickhouse.NewChTableConfigTimestampStringAttr(), Created: true,
		Cols: map[string]*clickhouse.Column{
			"timestamp":   {Name: "timestamp", Type: clickhouse.NewBaseType("DateTime")},
			"timestamp64": {Name: "timestamp64", Type: clickhouse.NewBaseType("DateTime64")},
			"@timestamp":  {Name: "@timestamp", Type: clickhouse.NewBaseType("DateTime64")},
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
	expectedSelectStatement := map[string]string{
		dateTimeTimestampField: `SELECT toInt64(toUnixTimestamp("timestamp") / 60) AS "aggr__0__key_0",
									  count(*) AS "aggr__0__count"
									FROM __quesma_table_name
									WHERE ((("timestamp64">=fromUnixTimestamp64Milli(1706542596491) AND
									  "timestamp64"<=fromUnixTimestamp64Milli(1706551896491)) AND ("timestamp">=
									  fromUnixTimestamp(1706542596) AND "timestamp"<=fromUnixTimestamp(1706551896)))
									  AND NOT (("@timestamp">=fromUnixTimestamp64Milli(1706542596491) AND
									  "@timestamp"<=fromUnixTimestamp64Milli(1706551896491)))) 
									GROUP BY toInt64(toUnixTimestamp("timestamp") / 60) AS "aggr__0__key_0"
									ORDER BY "aggr__0__key_0" ASC`,
		dateTime64TimestampField: `SELECT toInt64(toUnixTimestamp64Milli("timestamp64") / 60000) AS
									  "aggr__0__key_0", count(*) AS "aggr__0__count"
									FROM __quesma_table_name
									WHERE ((("timestamp64">=fromUnixTimestamp64Milli(1706542596491) AND
									  "timestamp64"<=fromUnixTimestamp64Milli(1706551896491)) AND ("timestamp">=
									  fromUnixTimestamp(1706542596) AND "timestamp"<=fromUnixTimestamp(1706551896)))
									  AND NOT (("@timestamp">=fromUnixTimestamp64Milli(1706542596491) AND
									  "@timestamp"<=fromUnixTimestamp64Milli(1706551896491))))
									GROUP BY toInt64(toUnixTimestamp64Milli("timestamp64") / 60000) AS
									  "aggr__0__key_0"
									ORDER BY "aggr__0__key_0" ASC`,
		dateTime64OurTimestampField: `SELECT toInt64(toUnixTimestamp64Milli("@timestamp") / 60000) AS "aggr__0__key_0"
									  , count(*) AS "aggr__0__count"
									FROM __quesma_table_name
									WHERE ((("timestamp64">=fromUnixTimestamp64Milli(1706542596491) AND
									  "timestamp64"<=fromUnixTimestamp64Milli(1706551896491)) AND ("timestamp">=
									  fromUnixTimestamp(1706542596) AND "timestamp"<=fromUnixTimestamp(1706551896)))
									  AND NOT (("@timestamp">=fromUnixTimestamp64Milli(1706542596491) AND
									  "@timestamp"<=fromUnixTimestamp64Milli(1706551896491))))
									GROUP BY toInt64(toUnixTimestamp64Milli("@timestamp") / 60000) AS
									  "aggr__0__key_0"
									ORDER BY "aggr__0__key_0" ASC`,
	}

	logger.InitSimpleLoggerForTestsWarnLevel()
	conn, mock := util.InitSqlMockWithPrettySqlAndPrint(t, false)
	defer conn.Close()
	db := backend_connectors.NewClickHouseBackendConnectorWithConnection("", conn)

	for _, fieldName := range []string{dateTimeTimestampField, dateTime64TimestampField, dateTime64OurTimestampField} {

		mock.ExpectQuery(expectedSelectStatement[fieldName]).
			WillReturnRows(sqlmock.NewRows([]string{"key", "doc_count"}))

		// .AddRow(1000, uint64(10)).AddRow(1001, uint64(20))) // here rows should be added if uint64 were supported
		queryRunner := NewQueryRunnerDefaultForTests(db, &DefaultConfig, tableName, util.NewSyncMapWith(tableName, &table), s)
		response, err := queryRunner.HandleAsyncSearch(ctx, tableName, types.MustJSON(query(fieldName)), defaultAsyncSearchTimeout, true)
		assert.NoError(t, err)

		var responseMap model.JsonMap
		err = json.Unmarshal(response, &responseMap)
		assert.NoError(t, err, "error unmarshalling search API response:")

		if err = mock.ExpectationsWereMet(); err != nil {
			t.Fatal("there were unfulfilled expections:", err)
		}
	}
}

// TestAsyncSearchFacets tests if results for facets are correctly returned
// (top 10 values, "other" value, min/max).
// Both `_search`, and `_async_search` handlers are tested.
func TestNumericFacetsQueries(t *testing.T) {
	s := &schema.StaticRegistry{
		Tables: map[schema.IndexName]schema.Schema{
			tableName: {
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
					"int64-field":       {PropertyName: "int64-field", InternalPropertyName: "int64-field", Type: schema.QuesmaTypeInteger},
					"float64-field":     {PropertyName: "float64-field", InternalPropertyName: "float64-field", Type: schema.QuesmaTypeFloat},
				},
			},
		},
	}
	table := util.NewSyncMapWith(tableName, &clickhouse.Table{
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
				conn, mock := util.InitSqlMockWithPrettySqlAndPrint(t, false)
				defer conn.Close()
				db := backend_connectors.NewClickHouseBackendConnectorWithConnection("", conn)

				colNames := make([]string, 0, len(tt.NewResultRows[0].Cols))
				for _, col := range tt.NewResultRows[0].Cols {
					colNames = append(colNames, col.ColName)
				}
				returnedBuckets := sqlmock.NewRows(colNames)
				for _, row := range tt.NewResultRows {
					values := make([]driver.Value, 0, len(row.Cols))
					for _, col := range row.Cols {
						values = append(values, col.Value)
					}
					returnedBuckets.AddRow(values...)
				}
				mock.ExpectQuery(tt.ExpectedSql).WillReturnRows(returnedBuckets)

				queryRunner := NewQueryRunnerDefaultForTests(db, &DefaultConfig, tableName, table, s)
				var response []byte
				var err error
				if handlerName == "handleSearch" {
					response, err = queryRunner.HandleSearch(ctx, tableName, types.MustJSON(tt.QueryJson))
				} else if handlerName == "handleAsyncSearch" {
					response, err = queryRunner.HandleAsyncSearch(ctx, tableName, types.MustJSON(tt.QueryJson), defaultAsyncSearchTimeout, true)
				}
				assert.NoError(t, err)

				if err = mock.ExpectationsWereMet(); err != nil {
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

				acceptableDifference := []string{"probability", "seed", "bg_count", "doc_count_error_upper_bound", "__quesma_total_count"}
				expectedJson := types.MustJSON(tt.ResultJson)["response"].(model.JsonMap)

				// Eventually we should remove two below lines
				expectedJson = expectedJson["aggregations"].(model.JsonMap)
				responsePart = responsePart["aggregations"].(model.JsonMap)

				actualMinusExpected, expectedMinusActual := util.MapDifference(responsePart,
					expectedJson, acceptableDifference, true, true)
				if len(actualMinusExpected) != 0 {
					pp.Println("ACTUAL diff", actualMinusExpected)
				}
				if len(expectedMinusActual) != 0 {
					pp.Println("EXPECTED diff", expectedMinusActual)
				}
				//pp.Println("ACTUAL", pancakeJson)
				//pp.Println("EXPECTED", expectedAggregationsPart)
				assert.True(t, util.AlmostEmpty(actualMinusExpected, acceptableDifference))
				assert.True(t, util.AlmostEmpty(expectedMinusActual, acceptableDifference))

			})
		}
	}
}

func TestSearchTrackTotalCount(t *testing.T) {

	s := &schema.StaticRegistry{Tables: map[schema.IndexName]schema.Schema{}}

	s.Tables[tableName] = schema.Schema{
		Fields: map[schema.FieldName]schema.Field{
			"message": {PropertyName: "message", InternalPropertyName: "message", Type: schema.QuesmaTypeText},
		},
	}

	var table = util.NewSyncMapWith(tableName, &clickhouse.Table{
		Name:   tableName,
		Config: clickhouse.NewChTableConfigTimestampStringAttr(),
		Cols: map[string]*clickhouse.Column{
			// only one field because currently we have non-determinism in translating * -> all fields :( and can't regex that easily.
			// (TODO Maybe we can, don't want to waste time for this now https://stackoverflow.com/questions/3533408/regex-i-want-this-and-that-and-that-in-any-order)
			"message": {Name: "message", Type: clickhouse.NewBaseType("String")},
		},
		Created: true,
	})

	test := func(handlerName string, testcase testdata.FullSearchTestCase) {
		conn, mock := util.InitSqlMockWithPrettySqlAndPrint(t, false)
		defer conn.Close()
		db := backend_connectors.NewClickHouseBackendConnectorWithConnection("", conn)

		for i, expectedSQL := range testcase.ExpectedSQLs {
			rows := sqlmock.NewRows([]string{testcase.ExpectedSQLResults[i][0].Cols[0].ColName})
			for _, row := range testcase.ExpectedSQLResults[i] {
				rows.AddRow(row.Cols[0].Value)
			}
			mock.ExpectQuery(expectedSQL).WillReturnRows(rows)
		}

		queryRunner := NewQueryRunnerDefaultForTests(db, &DefaultConfig, tableName, table, s)

		var response []byte
		var err error

		if handlerName == "handleSearch" {
			response, err = queryRunner.HandleSearch(ctx, tableName, types.MustJSON(testcase.QueryRequestJson))
		} else if handlerName == "handleAsyncSearch" {
			response, err = queryRunner.HandleAsyncSearch(
				ctx, tableName, types.MustJSON(testcase.QueryRequestJson), defaultAsyncSearchTimeout, true)
		}
		assert.NoError(t, err)

		if err = mock.ExpectationsWereMet(); err != nil {
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
				test(handlerName, tt)
			})
		}
	}
}

func TestFullQueryTestWIP(t *testing.T) {
	t.Skip(`We need to stop "unit" testing aggregation queries, because e.g. transformations aren't performed in tests whatsoever. Tests pass, but in real world things sometimes break. It's WIP.`)
	s := &schema.StaticRegistry{Tables: map[schema.IndexName]schema.Schema{}}

	s.Tables[tableName] = schema.Schema{
		Fields: map[schema.FieldName]schema.Field{
			"message": {PropertyName: "message", InternalPropertyName: "message", Type: schema.QuesmaTypeText},
		},
	}

	var table = util.NewSyncMapWith(tableName, &clickhouse.Table{
		Name:   tableName,
		Config: clickhouse.NewChTableConfigTimestampStringAttr(),
		Cols: map[string]*clickhouse.Column{
			// only one field because currently we have non-determinism in translating * -> all fields :( and can't regex that easily.
			// (TODO Maybe we can, don't want to waste time for this now https://stackoverflow.com/questions/3533408/regex-i-want-this-and-that-and-that-in-any-order)
			"message": {Name: "message", Type: clickhouse.NewBaseType("String")},
		},
		Created: true,
	})

	test := func(handlerName string, testcase testdata.FullSearchTestCase) {
		conn, mock := util.InitSqlMockWithPrettySqlAndPrint(t, false)
		defer conn.Close()
		db := backend_connectors.NewClickHouseBackendConnectorWithConnection("", conn)

		for i, expectedSQL := range testcase.ExpectedSQLs {
			rows := sqlmock.NewRows([]string{testcase.ExpectedSQLResults[i][0].Cols[0].ColName})
			for _, row := range testcase.ExpectedSQLResults[i] {
				rows.AddRow(row.Cols[0].Value)
			}
			mock.ExpectQuery(expectedSQL).WillReturnRows(rows)
		}

		queryRunner := NewQueryRunnerDefaultForTests(db, &DefaultConfig, tableName, table, s)

		var response []byte
		var err error

		if handlerName == "handleSearch" {
			response, err = queryRunner.HandleSearch(ctx, tableName, types.MustJSON(testcase.QueryRequestJson))
		} else if handlerName == "handleAsyncSearch" {
			response, err = queryRunner.HandleAsyncSearch(
				ctx, tableName, types.MustJSON(testcase.QueryRequestJson), defaultAsyncSearchTimeout, true)
		}
		assert.NoError(t, err)

		if err = mock.ExpectationsWereMet(); err != nil {
			t.Fatal("there were unfulfilled expections:", err)
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
				test(handlerName, tt)
			})
		}
	}
}

// TestSearchAfterParameter_sortByJustTimestamp simulates user viewing hits in Discover view in Kibana.
// For simplicity nr of hits is vastly reduced, from e.g. usual 500 to 3, but that shouldn't change the logic at all.
// Rows in DB are as follows (sorted by @timestamp DESC):
// (t, m1); (t, m2); (t, m3); (t, m4); (t, m5); (t, m6); (t, m7) (7 rows with same timestamp 't')
// (t-1s, m8); (t-1s, m9); (t-1s, m10) (3 rows with same timestamp 't-1s')
// and now unique timestamps for rest of rows
// (t-2s, m11); (t-3s, m12); (t-4s, m13); (t-5s, m14); (t-6s, m15); (t-7s, m16); (t-8s, m17)
//
// We send 4 requests, simulating user scrolling through hits.
// Some requests contain internal fields like _doc, or some other params like "unmapped_type".
// For now we ignore them, and it's also tested if that's indeed the case.
func TestSearchAfterParameter_sortByJustTimestamp(t *testing.T) {
	fields := map[schema.FieldName]schema.Field{
		"message":    {PropertyName: "message", InternalPropertyName: "message", Type: schema.QuesmaTypeText},
		"@timestamp": {PropertyName: "@timestamp", InternalPropertyName: "@timestamp", Type: schema.QuesmaTypeDate},
	}
	Schema := schema.NewSchema(fields, true, "")
	staticRegistry := schema.NewStaticRegistry(
		map[schema.IndexName]schema.Schema{tableName: Schema},
		map[string]schema.Table{},
		map[schema.FieldEncodingKey]schema.EncodedFieldName{},
	)
	tab := util.NewSyncMapWith(tableName, &clickhouse.Table{
		Name:   tableName,
		Config: clickhouse.NewChTableConfigTimestampStringAttr(),
		Cols: map[string]*clickhouse.Column{
			"message":    {Name: "message", Type: clickhouse.NewBaseType("String")},
			"@timestamp": {Name: "@timestamp", Type: clickhouse.NewBaseType("DateTime64")},
		},
		Created: true,
	})

	someTime := time.Date(2024, 1, 29, 18, 11, 36, 491000000, time.UTC) // 1706551896491 in UnixMilli
	sub := func(secondsFromSomeTime int) time.Time {
		return someTime.Add(time.Second * time.Duration(-secondsFromSomeTime))
	}
	iterations := []struct {
		request                     string
		expectedSQL                 string
		resultRowsFromDB            [][]any
		basicAndFastSortFieldPerHit []int64
	}{
		{
			request:                     `{"size": 3, "track_total_hits": false, "sort": [{"@timestamp": {"order": "desc"}}]}`,
			expectedSQL:                 `SELECT "@timestamp", "message" FROM __quesma_table_name ORDER BY "@timestamp" DESC LIMIT 3`,
			resultRowsFromDB:            [][]any{{someTime, "m1"}, {someTime, "m2"}, {someTime, "m3"}},
			basicAndFastSortFieldPerHit: []int64{someTime.UnixMilli(), someTime.UnixMilli(), someTime.UnixMilli()},
		},
		{
			request: `
				{
					"search_after": [1706551896491],
					"size": 3,
					"track_total_hits": false,
					"sort": [
						{"@timestamp": {"order": "desc", "format": "strict_date_optional_time", "unmapped_type": "boolean"}},
						{"_doc": {"unmapped_type": "boolean", "order": "desc"}}
					]
				}`,
			expectedSQL:                 `SELECT "@timestamp", "message" FROM __quesma_table_name WHERE fromUnixTimestamp64Milli(1706551896491)>"@timestamp" ORDER BY "@timestamp" DESC LIMIT 3`,
			resultRowsFromDB:            [][]any{{sub(1), "m8"}, {sub(2), "m9"}, {sub(3), "m10"}},
			basicAndFastSortFieldPerHit: []int64{sub(1).UnixMilli(), sub(2).UnixMilli(), sub(3).UnixMilli()},
		},
		{
			request:                     `{"search_after": [1706551896488], "size": 3, "track_total_hits": false, "sort": [{"@timestamp": {"order": "desc"}}]}`,
			expectedSQL:                 `SELECT "@timestamp", "message" FROM __quesma_table_name WHERE fromUnixTimestamp64Milli(1706551896488)>"@timestamp" ORDER BY "@timestamp" DESC LIMIT 3`,
			resultRowsFromDB:            [][]any{{sub(4), "m11"}, {sub(5), "m12"}, {sub(6), "m13"}},
			basicAndFastSortFieldPerHit: []int64{sub(4).UnixMilli(), sub(5).UnixMilli(), sub(6).UnixMilli()},
		},
		{
			request:                     `{"search_after": [1706551896485], "size": 3, "track_total_hits": false, "sort": [{"@timestamp": {"order": "desc"}}]}`,
			expectedSQL:                 `SELECT "@timestamp", "message" FROM __quesma_table_name WHERE fromUnixTimestamp64Milli(1706551896485)>"@timestamp" ORDER BY "@timestamp" DESC LIMIT 3`,
			resultRowsFromDB:            [][]any{{sub(7), "m14"}, {sub(8), "m15"}, {sub(9), "m16"}},
			basicAndFastSortFieldPerHit: []int64{sub(7).UnixMilli(), sub(8).UnixMilli(), sub(9).UnixMilli()},
		},
	}

	test := func(strategy searchAfterStrategy, dateTimeType string, handlerName string) {
		conn, mock := util.InitSqlMockWithPrettySqlAndPrint(t, false)
		defer conn.Close()
		db := backend_connectors.NewClickHouseBackendConnectorWithConnection("", conn)
		queryRunner := NewQueryRunnerDefaultForTests(db, &DefaultConfig, tableName, tab, staticRegistry)

		for _, iteration := range iterations {
			rows := sqlmock.NewRows([]string{"@timestamp", "message"})
			for _, row := range iteration.resultRowsFromDB {
				rows.AddRow(row[0], row[1])
			}
			mock.ExpectQuery(iteration.expectedSQL).WillReturnRows(rows)

			var (
				response                  []byte
				err                       error
				responseMap, responsePart model.JsonMap
			)
			switch handlerName {
			case "handleSearch":
				response, err = queryRunner.HandleSearch(ctx, tableName, types.MustJSON(iteration.request))
			case "handleAsyncSearch":
				response, err = queryRunner.HandleAsyncSearch(ctx, tableName, types.MustJSON(iteration.request), defaultAsyncSearchTimeout, true)
			default:
				t.Fatalf("Unknown handler name: %s", handlerName)
			}
			assert.NoError(t, err)
			err = json.Unmarshal(response, &responseMap)
			assert.NoError(t, err)
			if handlerName == "handleSearch" {
				responsePart = responseMap
			} else {
				responsePart = responseMap["response"].(model.JsonMap)
			}

			hits := responsePart["hits"].(model.JsonMap)["hits"].([]any)
			assert.Len(t, hits, len(iteration.resultRowsFromDB))
			for i, hit := range hits {
				sortField := hit.(model.JsonMap)["sort"].([]any)
				assert.Len(t, sortField, 1)
				assert.Equal(t, float64(iteration.basicAndFastSortFieldPerHit[i]), sortField[0].(float64))
			}
		}

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal("there were unfulfilled expections:", err)
		}
	}

	handlers := []string{"handleSearch", "handleAsyncSearch"}
	for _, strategy := range []searchAfterStrategy{searchAfterStrategyFactory(basicAndFast)} {
		for _, handlerName := range handlers {
			t.Run("TestSearchAfterParameter: "+handlerName, func(t *testing.T) {
				test(strategy, "todo_add_2_cases_for_datetime_and_datetime64_after_fixing_it", handlerName)
			})
		}
	}
}

// TestSearchAfterParameter_sortByJustOneStringField simulates user viewing hits in Discover view in Kibana.
// For simplicity nr of hits is vastly reduced, from e.g. usual 500 to 3, but that shouldn't change the logic at all.
// Rows in DB are as follows (sorted by message ASC):
// - 5x "m1",
// - 2x "m2",
// - 1x "m3's", (TODO add some special character, so m3 -> m3's after https://github.com/QuesmaOrg/quesma/pull/1114)
// - 1x "m4", "m5", ...
//
// We send 4 requests, simulating user scrolling through hits.
func TestSearchAfterParameter_sortByJustOneStringField(t *testing.T) {
	fields := map[schema.FieldName]schema.Field{
		"message": {PropertyName: "message", InternalPropertyName: "message", Type: schema.QuesmaTypeText},
	}
	Schema := schema.NewSchema(fields, true, "")
	staticRegistry := schema.NewStaticRegistry(
		map[schema.IndexName]schema.Schema{tableName: Schema},
		map[string]schema.Table{},
		map[schema.FieldEncodingKey]schema.EncodedFieldName{},
	)
	tab := util.NewSyncMapWith(tableName, &clickhouse.Table{
		Name:   tableName,
		Config: clickhouse.NewChTableConfigNoAttrs(),
		Cols: map[string]*clickhouse.Column{
			"message": {Name: "message", Type: clickhouse.NewBaseType("String")},
		},
		Created: true,
	})

	iterations := []struct {
		request          string
		expectedSQL      string
		resultRowsFromDB []any
	}{
		{
			request:          `{"size": 3, "track_total_hits": false, "sort": [{"message": {"order": "asc"}}]}`,
			expectedSQL:      `SELECT "message" FROM __quesma_table_name ORDER BY "message" ASC LIMIT 3`,
			resultRowsFromDB: []any{"m1", "m1", "m1"},
		},
		{
			request:          `{"search_after": ["m1"], "size": 3, "track_total_hits": false, "sort": [{"message": {"order": "asc"}}]}`,
			expectedSQL:      `SELECT "message" FROM __quesma_table_name WHERE "message">'m1' ORDER BY "message" ASC LIMIT 3`,
			resultRowsFromDB: []any{"m2", "m2", "m3"},
		},
		{
			request:          `{"search_after": ["m3"], "size": 3, "track_total_hits": false, "sort": [{"message": {"order": "asc"}}]}`,
			expectedSQL:      `SELECT "message" FROM __quesma_table_name WHERE "message">'m3' ORDER BY "message" ASC LIMIT 3`,
			resultRowsFromDB: []any{"m4", "m5", "m6"},
		},
		{
			request:          `{"search_after": ["m6"], "size": 3, "track_total_hits": false, "sort": [{"message": {"order": "asc"}}]}`,
			expectedSQL:      `SELECT "message" FROM __quesma_table_name WHERE "message">'m6' ORDER BY "message" ASC LIMIT 3`,
			resultRowsFromDB: []any{"m7", "m8", "m9"},
		},
	}

	test := func(strategy searchAfterStrategy, dateTimeType string, handlerName string) {
		conn, mock := util.InitSqlMockWithPrettySqlAndPrint(t, false)
		defer conn.Close()
		db := backend_connectors.NewClickHouseBackendConnectorWithConnection("", conn)
		queryRunner := NewQueryRunnerDefaultForTests(db, &DefaultConfig, tableName, tab, staticRegistry)

		for _, iteration := range iterations {
			rows := sqlmock.NewRows([]string{"message"})
			for _, row := range iteration.resultRowsFromDB {
				rows.AddRow(row)
			}
			mock.ExpectQuery(iteration.expectedSQL).WillReturnRows(rows)

			var (
				response                  []byte
				err                       error
				responseMap, responsePart model.JsonMap
			)
			switch handlerName {
			case "handleSearch":
				response, err = queryRunner.HandleSearch(ctx, tableName, types.MustJSON(iteration.request))
			case "handleAsyncSearch":
				response, err = queryRunner.HandleAsyncSearch(ctx, tableName, types.MustJSON(iteration.request), defaultAsyncSearchTimeout, true)
			default:
				t.Fatalf("Unknown handler name: %s", handlerName)
			}
			assert.NoError(t, err)
			err = json.Unmarshal(response, &responseMap)
			assert.NoError(t, err)
			if handlerName == "handleSearch" {
				responsePart = responseMap
			} else {
				responsePart = responseMap["response"].(model.JsonMap)
			}

			hits := responsePart["hits"].(model.JsonMap)["hits"].([]any)
			assert.Len(t, hits, len(iteration.resultRowsFromDB))
			for i, hit := range hits {
				sortField := hit.(model.JsonMap)["sort"].([]any)
				assert.Len(t, sortField, 1)
				assert.Equal(t, iteration.resultRowsFromDB[i], sortField[0].(string))
			}
		}

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal("there were unfulfilled expections:", err)
		}
	}

	handlers := []string{"handleSearch", "handleAsyncSearch"}
	for _, strategy := range []searchAfterStrategy{searchAfterStrategyFactory(basicAndFast)} {
		for _, handlerName := range handlers {
			t.Run("TestSearchAfterParameter: "+handlerName, func(t *testing.T) {
				test(strategy, "todo_add_2_cases_for_datetime_and_datetime64_after_fixing_it", handlerName)
			})
		}
	}
}

// TestSearchAfterParameter_sortByMultipleFields simulates user viewing hits in Discover view in Kibana.
// For simplicity nr of hits is vastly reduced, from e.g. usual 500 to 3, but that shouldn't change the logic at all.
// Rows in DB are as follows (properly sorted)
// (@timestamp, message, bicep_size):
// (t, m1, 1); (t, m2, 2); (t, m3, 3);
// (t, m4, 4); (t, m5, 5); (t, m5, 0); (t, m5, 0);
// (t-1s, m6, 0); (t-1s, m7, 0); (t-1s, m8, 0);
// (t-1s, m9, 0); (t-2s, m10, 0); (t-3s, m11, 0);
//
// We send 4 requests, simulating user scrolling through hits.
func TestSearchAfterParameter_sortByMultipleFields(t *testing.T) {
	fields := map[schema.FieldName]schema.Field{
		"message":    {PropertyName: "message", InternalPropertyName: "message", Type: schema.QuesmaTypeText},
		"bicep_size": {PropertyName: "bicep_size", InternalPropertyName: "bicep_size", Type: schema.QuesmaTypeInteger},
		"@timestamp": {PropertyName: "@timestamp", InternalPropertyName: "@timestamp", Type: schema.QuesmaTypeDate},
	}
	Schema := schema.NewSchema(fields, true, "")
	staticRegistry := schema.NewStaticRegistry(
		map[schema.IndexName]schema.Schema{tableName: Schema},
		map[string]schema.Table{},
		map[schema.FieldEncodingKey]schema.EncodedFieldName{},
	)
	tab := util.NewSyncMapWith(tableName, &clickhouse.Table{
		Name:   tableName,
		Config: clickhouse.NewChTableConfigTimestampStringAttr(),
		Cols: map[string]*clickhouse.Column{
			"message":    {Name: "message", Type: clickhouse.NewBaseType("String")},
			"bicep_size": {Name: "bicep_size", Type: clickhouse.NewBaseType("Int64")},
			"@timestamp": {Name: "@timestamp", Type: clickhouse.NewBaseType("DateTime64")},
		},
		Created: true,
	})

	someTime := time.Date(2024, 1, 29, 18, 11, 36, 491000000, time.UTC) // 1706551896491 in UnixMilli
	sub := func(secondsFromSomeTime int) time.Time {
		return someTime.Add(time.Second * time.Duration(-secondsFromSomeTime))
	}
	iterations := []struct {
		request                     string
		expectedSQL                 string
		resultRowsFromDB            [][]any
		basicAndFastSortFieldPerHit [][]any
	}{
		{
			request:     `{"size": 3, "track_total_hits": false, "sort": [{"@timestamp": {"order": "desc"}}, {"message": {"order": "asc"}}, {"bicep_size": {"order": "desc"}}]}`,
			expectedSQL: `SELECT "@timestamp", "bicep_size", "message" FROM __quesma_table_name ORDER BY "@timestamp" DESC, "message" ASC, "bicep_size" DESC LIMIT 3`,
			resultRowsFromDB: [][]any{
				{someTime, int64(1), "m1"},
				{someTime, int64(2), "m2"},
				{someTime, int64(3), "m3"},
			},
			basicAndFastSortFieldPerHit: [][]any{
				{someTime.UnixMilli(), "m1", int64(1)},
				{someTime.UnixMilli(), "m2", int64(2)},
				{someTime.UnixMilli(), "m3", int64(3)},
			},
		},
		{
			request:     `{"search_after": [1706551896491, "m3", 3], "size": 3, "track_total_hits": false,  "sort": [{"@timestamp": {"order": "desc"}}, {"message": {"order": "asc"}}, {"bicep_size": {"order": "desc"}}]}`,
			expectedSQL: `SELECT "@timestamp", "bicep_size", "message" FROM __quesma_table_name WHERE tuple(fromUnixTimestamp64Milli(1706551896491), "message", 3)>tuple("@timestamp", 'm3', "bicep_size") ORDER BY "@timestamp" DESC, "message" ASC, "bicep_size" DESC LIMIT 3`,
			resultRowsFromDB: [][]any{
				{someTime, int64(4), "m4"},
				{someTime, int64(5), "m5"},
				{someTime, int64(0), "m5"},
			},
			basicAndFastSortFieldPerHit: [][]any{
				{someTime.UnixMilli(), "m4", int64(4)},
				{someTime.UnixMilli(), "m5", int64(5)},
				{someTime.UnixMilli(), "m5", int64(0)},
			},
		},
		{
			request:     `{"search_after": [1706551896491, "m5", 0], "size": 3, "track_total_hits": false,  "sort": [{"@timestamp": {"order": "desc"}}, {"message": {"order": "asc"}}, {"bicep_size": {"order": "desc"}}]}`,
			expectedSQL: `SELECT "@timestamp", "bicep_size", "message" FROM __quesma_table_name WHERE tuple(fromUnixTimestamp64Milli(1706551896491), "message", 0)>tuple("@timestamp", 'm5', "bicep_size") ORDER BY "@timestamp" DESC, "message" ASC, "bicep_size" DESC LIMIT 3`,
			resultRowsFromDB: [][]any{
				{sub(1), int64(0), "m6"},
				{sub(1), int64(0), "m7"},
				{sub(1), int64(0), "m8"},
			},
			basicAndFastSortFieldPerHit: [][]any{
				{sub(1).UnixMilli(), "m6", int64(0)},
				{sub(1).UnixMilli(), "m7", int64(0)},
				{sub(1).UnixMilli(), "m8", int64(0)},
			},
		},
		{
			request:     `{"search_after": [1706551896491, "m8", 0], "size": 3, "track_total_hits": false,  "sort": [{"@timestamp": {"order": "desc"}}, {"message": {"order": "asc"}}, {"bicep_size": {"order": "desc"}}]}`,
			expectedSQL: `SELECT "@timestamp", "bicep_size", "message" FROM __quesma_table_name WHERE tuple(fromUnixTimestamp64Milli(1706551896491), "message", 0)>tuple("@timestamp", 'm8', "bicep_size") ORDER BY "@timestamp" DESC, "message" ASC, "bicep_size" DESC LIMIT 3`,
			resultRowsFromDB: [][]any{
				{sub(1), int64(0), "m9"},
				{sub(2), int64(0), "m10"},
				{sub(3), int64(0), "m11"},
			},
			basicAndFastSortFieldPerHit: [][]any{
				{sub(1).UnixMilli(), "m9", int64(0)},
				{sub(2).UnixMilli(), "m10", int64(0)},
				{sub(3).UnixMilli(), "m11", int64(0)},
			},
		},
	}

	test := func(strategy searchAfterStrategy, dateTimeType string, handlerName string) {
		conn, mock := util.InitSqlMockWithPrettySqlAndPrint(t, false)
		defer conn.Close()
		db := backend_connectors.NewClickHouseBackendConnectorWithConnection("", conn)
		queryRunner := NewQueryRunnerDefaultForTests(db, &DefaultConfig, tableName, tab, staticRegistry)

		for _, iteration := range iterations {
			rows := sqlmock.NewRows([]string{"@timestamp", "bicep_size", "message"})
			for _, row := range iteration.resultRowsFromDB {
				rows.AddRow(row[0], row[1], row[2])
			}
			mock.ExpectQuery(iteration.expectedSQL).WillReturnRows(rows)

			var (
				response                  []byte
				err                       error
				responseMap, responsePart model.JsonMap
			)
			switch handlerName {
			case "handleSearch":
				response, err = queryRunner.HandleSearch(ctx, tableName, types.MustJSON(iteration.request))
			case "handleAsyncSearch":
				response, err = queryRunner.HandleAsyncSearch(ctx, tableName, types.MustJSON(iteration.request), defaultAsyncSearchTimeout, true)
			default:
				t.Fatalf("Unknown handler name: %s", handlerName)
			}
			assert.NoError(t, err)
			err = json.Unmarshal(response, &responseMap)
			assert.NoError(t, err)
			if handlerName == "handleSearch" {
				responsePart = responseMap
			} else {
				responsePart = responseMap["response"].(model.JsonMap)
			}

			hits := responsePart["hits"].(model.JsonMap)["hits"].([]any)
			assert.Len(t, hits, len(iteration.resultRowsFromDB))
			for i, hit := range hits {
				sortField := hit.(model.JsonMap)["sort"].([]any)
				assert.Len(t, sortField, 3)
				assert.Equal(t, float64(iteration.basicAndFastSortFieldPerHit[i][0].(int64)), sortField[0].(float64))
				assert.Equal(t, iteration.basicAndFastSortFieldPerHit[i][1].(string), sortField[1].(string))
				assert.Equal(t, float64(iteration.basicAndFastSortFieldPerHit[i][2].(int64)), sortField[2].(float64))
			}
		}

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal("there were unfulfilled expections:", err)
		}
	}

	handlers := []string{"handleSearch", "handleAsyncSearch"}
	for _, strategy := range []searchAfterStrategy{searchAfterStrategyFactory(basicAndFast)} {
		for _, handlerName := range handlers {
			t.Run("TestSearchAfterParameter: "+handlerName, func(t *testing.T) {
				test(strategy, "todo_add_2_cases_for_datetime_and_datetime64_after_fixing_it", handlerName)
			})
		}
	}
}

// TestSearchAfterParameter_sortByNoField simulates user viewing hits in Discover view in Kibana.
// For simplicity nr of hits is vastly reduced, from e.g. usual 500 to 3, but that shouldn't change the logic at all.
//
// When data view has no timestamp field, by default Kibana will send sort: [{"_score": {"order": "desc"}}] request.
// And hits response will have no sort field.
// This test checks this behaviour.
func TestSearchAfterParameter_sortByNoField(t *testing.T) {
	fields := map[schema.FieldName]schema.Field{
		"message":    {PropertyName: "message", InternalPropertyName: "message", Type: schema.QuesmaTypeText},
		"bicep_size": {PropertyName: "bicep_size", InternalPropertyName: "bicep_size", Type: schema.QuesmaTypeInteger},
		"@timestamp": {PropertyName: "@timestamp", InternalPropertyName: "@timestamp", Type: schema.QuesmaTypeDate},
	}
	Schema := schema.NewSchema(fields, true, "")
	staticRegistry := schema.NewStaticRegistry(
		map[schema.IndexName]schema.Schema{tableName: Schema},
		map[string]schema.Table{},
		map[schema.FieldEncodingKey]schema.EncodedFieldName{},
	)
	tab := util.NewSyncMapWith(tableName, &clickhouse.Table{
		Name:   tableName,
		Config: clickhouse.NewChTableConfigTimestampStringAttr(),
		Cols: map[string]*clickhouse.Column{
			"message":    {Name: "message", Type: clickhouse.NewBaseType("String")},
			"bicep_size": {Name: "bicep_size", Type: clickhouse.NewBaseType("Int64")},
			"@timestamp": {Name: "@timestamp", Type: clickhouse.NewBaseType("DateTime64")},
		},
		Created: true,
	})

	someTime := time.Date(2024, 1, 29, 18, 11, 36, 491000000, time.UTC) // 1706551896491 in UnixMilli
	iterations := []struct {
		request          string
		expectedSQL      string
		resultRowsFromDB [][]any
	}{
		{
			request:     `{"size": 3, "track_total_hits": false, "sort": [{"_score": {"order": "desc"}}]}`,
			expectedSQL: `SELECT "@timestamp", "bicep_size", "message" FROM __quesma_table_name LIMIT 3`,
			resultRowsFromDB: [][]any{
				{someTime, int64(1), "m1"},
				{someTime, int64(2), "m2"},
				{someTime, int64(3), "m3"},
			},
		},
	}

	test := func(strategy searchAfterStrategy, dateTimeType string, handlerName string) {
		conn, mock := util.InitSqlMockWithPrettySqlAndPrint(t, false)
		defer conn.Close()
		db := backend_connectors.NewClickHouseBackendConnectorWithConnection("", conn)
		queryRunner := NewQueryRunnerDefaultForTests(db, &DefaultConfig, tableName, tab, staticRegistry)

		for _, iteration := range iterations {
			rows := sqlmock.NewRows([]string{"@timestamp", "bicep_size", "message"})
			for _, row := range iteration.resultRowsFromDB {
				rows.AddRow(row[0], row[1], row[2])
			}
			mock.ExpectQuery(iteration.expectedSQL).WillReturnRows(rows)

			var (
				response                  []byte
				err                       error
				responseMap, responsePart model.JsonMap
			)
			switch handlerName {
			case "handleSearch":
				response, err = queryRunner.HandleSearch(ctx, tableName, types.MustJSON(iteration.request))
			case "handleAsyncSearch":
				response, err = queryRunner.HandleAsyncSearch(ctx, tableName, types.MustJSON(iteration.request), defaultAsyncSearchTimeout, true)
			default:
				t.Fatalf("Unknown handler name: %s", handlerName)
			}
			assert.NoError(t, err)
			err = json.Unmarshal(response, &responseMap)
			assert.NoError(t, err)
			if handlerName == "handleSearch" {
				responsePart = responseMap
			} else {
				responsePart = responseMap["response"].(model.JsonMap)
			}

			hits := responsePart["hits"].(model.JsonMap)["hits"].([]any)
			assert.Len(t, hits, len(iteration.resultRowsFromDB))
			for _, hit := range hits {
				_, exists := hit.(model.JsonMap)["sort"]
				assert.False(t, exists)
			}
		}

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal("there were unfulfilled expections:", err)
		}
	}

	handlers := []string{"handleSearch", "handleAsyncSearch"}
	for _, strategy := range []searchAfterStrategy{searchAfterStrategyFactory(basicAndFast)} {
		for _, handlerName := range handlers {
			t.Run("TestSearchAfterParameter: "+handlerName, func(t *testing.T) {
				test(strategy, "todo_add_2_cases_for_datetime_and_datetime64_after_fixing_it", handlerName)
			})
		}
	}
}
