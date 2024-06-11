package clickhouse

import (
	"context"
	"github.com/stretchr/testify/assert"
	"mitmproxy/quesma/model"
	"slices"
	"strconv"
	"testing"
)

var tables = []struct {
	createTableStr    string
	allFields         []string               // names of all fields in the table
	exampleFieldValue map[string]interface{} // example value for each field
}{
	{`CREATE TABLE table ( "message" String, "timestamp" DateTime64(3, 'UTC') ) ENGINE = Memory`,
		[]string{"message", "timestamp"},
		map[string]interface{}{"message": "a", "timestamp": "2020-01-01 00:00:00", "count()": int64(0), "toString(count())": "0"},
	},
	{
		`CREATE TABLE table ( "message" LowCardinality(String), "timestamp" DateTime64() DEFAULT now64(), "great" Bool, "json" JSON, "int" Int64) ENGINE = Memory`,
		[]string{"message", "timestamp", "great", "json", "int"},
		map[string]interface{}{"message": "a", "timestamp": "2020-01-01 00:00:00", "great": true, "json": "{}", "int": int64(0), "count()": int64(0), "toString(count())": "0"},
	},
	{`CREATE TABLE table ( "tuple" Tuple ("s" String, "i" Int64), "message" String, "timestamp" DateTime64) ENGINE = Memory`,
		[]string{"tuple", "message", "timestamp"},
		map[string]interface{}{"tuple": map[string]interface{}{"s": "a", "i": int64(0)}, "message": "a", "timestamp": "2020-01-01 00:00:00", "count()": int64(0), "toString(count())": "0"},
	}, {
		`CREATE TABLE table ( "tuple" Tuple ("tuple1" Tuple("i" Int64, "s" String), "i" Int64), "message" LowCardinality(String) CODEC("nocodec"), "timestamp" DateTime64) ENGINE = Memory`,
		[]string{"tuple", "message", "timestamp"},
		map[string]interface{}{"tuple": map[string]interface{}{"tuple1": map[string]interface{}{"i": int64(0), "s": "a"}, "i": int64(0)}, "message": "a", "timestamp": "2020-01-01 00:00:00", "count()": int64(0), "toString(count())": "0"},
	},
}

var queries = []struct {
	query  *model.Query
	answer []string
}{
	{
		&model.Query{}, // empty query
		[]string{},
	},
	{
		&model.Query{Columns: []model.SelectColumn{{Expression: model.NewWildcardExpr}}},
		[]string{"all"},
	},
	{
		&model.Query{Columns: []model.SelectColumn{{Expression: model.NewWildcardExpr}, {Expression: model.NewCountFunc()}}},
		[]string{"all", "count()"},
	},
	{
		&model.Query{Columns: []model.SelectColumn{{Expression: model.NewWildcardExpr}, {Expression: model.NewCountFunc()}}, WhereClause: model.NewInfixExpr(model.NewColumnRef("message"), "=", model.NewLiteral("hello"))}, // select fields + where clause
		[]string{"all", "count()"},
	},
	{
		&model.Query{Columns: []model.SelectColumn{{Expression: model.NewTableColumnExpr("message")}, {Expression: model.NewTableColumnExpr("timestamp")}}},
		[]string{"message", "timestamp"},
	},
	{
		&model.Query{Columns: []model.SelectColumn{{Expression: model.NewTableColumnExpr("message")}, {Expression: model.NewTableColumnExpr("non-existent")}}},
		[]string{"message"},
	},
	{
		&model.Query{Columns: []model.SelectColumn{{Expression: model.NewTableColumnExpr("non-existent")}}},
		[]string{},
	},
	{
		&model.Query{Columns: []model.SelectColumn{{Expression: model.NewTableColumnExpr("message")}, {Expression: model.NewTableColumnExpr("timestamp")}}},
		[]string{"message", "timestamp"},
	},
	//{ // we don't support such a query. Supporting it would slow down query's code, and this query seems pointless
	//	&model.Query{Fields: []string{"*", "message"}},
	//	[]string{"all"},
	//},
}

func assertSlicesEqual(t *testing.T, expected, actual []string) {
	assert.Equal(t, len(expected), len(actual))
	for i := range actual {
		assert.Contains(t, expected, actual[i])
	}
}

func Test_extractColumns(t *testing.T) {
	configs := []*ChTableConfig{
		NewChTableConfigNoAttrs(),
		NewChTableConfigFourAttrs(),
	}
	for configIdx, config := range configs {
		for i, tt := range tables {
			table, err := NewTable(tt.createTableStr, config)
			assert.NoError(t, err)
			addOurFieldsToCreateTableQuery(tt.createTableStr, config, table)

			// add attributes to expected values if we're in Attrs config case
			if len(config.attributes) > 0 {
				for _, a := range config.attributes {
					tt.allFields = append(tt.allFields, a.KeysArrayName, a.ValuesArrayName)
					tt.exampleFieldValue[a.KeysArrayName] = []string{"a", "b"}
					switch a.Type {
					case NewBaseType("String"):
						tt.exampleFieldValue[a.ValuesArrayName] = []string{"a", "b"}
					case NewBaseType("Int64"):
						tt.exampleFieldValue[a.ValuesArrayName] = []int64{1, 2}
					case NewBaseType("Bool"):
						tt.exampleFieldValue[a.ValuesArrayName] = []bool{true, false}
					case NewBaseType("DateTime64"):
						tt.exampleFieldValue[a.ValuesArrayName] = []string{"2020-01-01 00:00:00", "2025-01-01 00:00:00"}
					}
				}
			}

			for j, q := range queries {
				t.Run("Test_extractColumns, case config["+strconv.Itoa(configIdx)+"], createTableStr["+strconv.Itoa(i)+"], queries["+strconv.Itoa(j)+"]", func(t *testing.T) {
					colNames, err := table.extractColumns(q.query, false)
					if slices.Contains(q.query.Columns, model.SelectColumn{Expression: model.NewTableColumnExpr("non-existent")}) {
						assert.Error(t, err)
						return
					} else {
						assert.NoError(t, err)
					}

					// assert column names are OK
					if len(q.answer) >= 1 && q.answer[0] == "all" {
						assertSlicesEqual(t, tt.allFields, colNames)
					} else {
						assertSlicesEqual(t, q.answer, colNames)
					}

					// assert types are OK (we can assign some example value to it)
					// we can't just check types as all of them are of type reflect.Value, not string, int, etc.
					rowToScan := make([]interface{}, len(colNames))
					for k, colName := range colNames {
						if rowToScan[k] != nil { // nil = we have tuple, we don't support tuples yet
							rowToScan[k] = tt.exampleFieldValue[colName]
							assert.Equal(t, tt.exampleFieldValue[colName], rowToScan[k])
						}
					}
				})
			}
		}
	}
}

func TestGetDateTimeType(t *testing.T) {
	ctx := context.Background()
	table, err := NewTable(`CREATE TABLE table (
		"timestamp1" DateTime,
		"timestamp2" DateTime('UTC'),
		"timestamp64_1" DateTime64,
		"timestamp64_2" DateTime64(3, 'UTC') ) ENGINE = Memory`, NewChTableConfigTimestampStringAttr())
	assert.NoError(t, err)
	assert.Equal(t, DateTime, table.GetDateTimeType(ctx, "timestamp1"))
	assert.Equal(t, DateTime, table.GetDateTimeType(ctx, "timestamp2"))
	assert.Equal(t, DateTime64, table.GetDateTimeType(ctx, "timestamp64_1"))
	assert.Equal(t, DateTime64, table.GetDateTimeType(ctx, "timestamp64_2"))
	assert.Equal(t, DateTime64, table.GetDateTimeType(ctx, timestampFieldName)) // default, created by us
	assert.Equal(t, Invalid, table.GetDateTimeType(ctx, "non-existent"))
}
