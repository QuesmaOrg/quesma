// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package clickhouse

import (
	"context"
	"github.com/stretchr/testify/assert"
	"quesma/model"
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
		&model.Query{SelectCommand: model.SelectCommand{}}, // empty query
		[]string{},
	},
	{
		&model.Query{SelectCommand: model.SelectCommand{Columns: []model.Expr{model.NewWildcardExpr}}},
		[]string{"all"},
	},
	{
		&model.Query{SelectCommand: model.SelectCommand{Columns: []model.Expr{model.NewWildcardExpr, model.NewCountFunc()}}},
		[]string{"all", "count()"},
	},
	{
		&model.Query{SelectCommand: model.SelectCommand{Columns: []model.Expr{model.NewWildcardExpr, model.NewCountFunc(), model.NewInfixExpr(model.NewColumnRef("message"), "=", model.NewLiteral("hello"))}}}, // select fields + where clause
		[]string{"all", "count()"},
	},
	{
		&model.Query{SelectCommand: model.SelectCommand{Columns: []model.Expr{model.NewColumnRef("message"), model.NewColumnRef("timestamp")}}},
		[]string{"message", "timestamp"},
	},
	{
		&model.Query{SelectCommand: model.SelectCommand{Columns: []model.Expr{model.NewColumnRef("message"), model.NewColumnRef("non-existent")}}},
		[]string{"message"},
	},
	{
		&model.Query{SelectCommand: model.SelectCommand{Columns: []model.Expr{model.NewColumnRef("non-existent")}}},
		[]string{},
	},
	{
		&model.Query{SelectCommand: model.SelectCommand{Columns: []model.Expr{model.NewColumnRef("message"), model.NewColumnRef("timestamp")}}},
		[]string{"message", "timestamp"},
	},
	//{ // we don't support such a query. Supporting it would slow down query's code, and this query seems pointless
	//	&model.Query{SelectCommand: model.SelectCommand{Columns: []string{"*", "message"}}},
	//	[]string{"all"},
	//},
}

func assertSlicesEqual(t *testing.T, expected, actual []string) {
	assert.Equal(t, len(expected), len(actual))
	for i := range actual {
		assert.Contains(t, expected, actual[i])
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
