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
	"quesma/clickhouse"
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
			db, mock := util.InitSqlMockWithPrettySqlAndPrint(t, false)
			defer db.Close()

			for _, query := range tt.WantedQuery {
				mock.ExpectQuery(query).WillReturnRows(sqlmock.NewRows([]string{"@timestamp", "host.name"}))
			}

			queryRunner := NewQueryRunnerDefaultForTests(db, &DefaultConfig, tableName, table, s)
			_, err := queryRunner.HandleAsyncSearch(ctx, tableName, types.MustJSON(tt.QueryJson), defaultAsyncSearchTimeout, true)
			assert.NoError(t, err)

			if err := mock.ExpectationsWereMet(); err != nil {
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
			db, mock := util.InitSqlMockWithPrettySqlAndPrint(t, false)
			defer db.Close()

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
			var db *sql.DB
			var mock sqlmock.Sqlmock
			if len(tt.WantedRegexes) > 0 {
				db, mock = util.InitSqlMockWithPrettyPrint(t, false)
			} else {
				db, mock = util.InitSqlMockWithPrettySqlAndPrint(t, false)
			}
			defer db.Close()

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
			var db *sql.DB
			var mock sqlmock.Sqlmock
			if len(tt.WantedRegexes) > 0 {
				db, mock = util.InitSqlMockWithPrettyPrint(t, false)
			} else {
				db, mock = util.InitSqlMockWithPrettySqlAndPrint(t, false)
			}
			defer db.Close()

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
			db, mock := util.InitSqlMockWithPrettyPrint(t, false)
			defer db.Close()

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
			var db *sql.DB
			var mock sqlmock.Sqlmock
			if len(tt.WantedRegexes) > 0 {
				db, mock = util.InitSqlMockWithPrettyPrint(t, false)
			} else {
				db, mock = util.InitSqlMockWithPrettySqlAndPrint(t, false)
			}
			defer db.Close()

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

	db, mock := util.InitSqlMockWithPrettySqlAndPrint(t, false)
	defer db.Close()

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
				db, mock := util.InitSqlMockWithPrettySqlAndPrint(t, false)
				defer db.Close()

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

	test := func(t *testing.T, handlerName string, testcase testdata.FullSearchTestCase) {
		db, mock := util.InitSqlMockWithPrettySqlAndPrint(t, false)
		defer db.Close()

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

	test := func(t *testing.T, handlerName string, testcase testdata.FullSearchTestCase) {
		db, mock := util.InitSqlMockWithPrettySqlAndPrint(t, false)
		defer db.Close()

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
