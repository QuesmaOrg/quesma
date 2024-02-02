package queryparser

import (
	"mitmproxy/quesma/clickhouse"
	"mitmproxy/quesma/testdata"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TODO:
//  1. 14th test, "Query string". "(message LIKE '%%%' OR message LIKE '%logged%')", is it really
//     what should be? According to docs, I think so... Maybe test in Kibana?
//     OK, Kibana disagrees, it is indeed wrong.
func TestQueryParserStringAttrConfig(t *testing.T) {
	testTable, err := clickhouse.NewTable(`CREATE TABLE `+TableName+`
		( "message" String, "timestamp" DateTime64(3, 'UTC') )
		ENGINE = Memory`,
		clickhouse.NewNoTimestampOnlyStringAttrCHConfig(),
	)
	if err != nil {
		t.Fatal(err)
	}
	lm := clickhouse.NewLogManager(clickhouse.TableMap{TableName: testTable}, make(clickhouse.TableMap))
	cw := ClickhouseQueryTranslator{lm}
	for _, tt := range testdata.TestsSearch {
		t.Run(tt.Name, func(t *testing.T) {
			simpleQuery, queryType := cw.ParseQuery(tt.QueryJson)
			assert.True(t, simpleQuery.CanParse)
			assert.Contains(t, tt.WantedSql, simpleQuery.Sql.Stmt)
			assert.Equal(t, tt.WantedQueryType, queryType)

			query := cw.BuildSimpleSelectQuery(TableName, simpleQuery.Sql.Stmt)
			assert.Contains(t, tt.WantedQuery, *query)
		})
	}
}

// TODO this test gives wrong results??
func TestQueryParserNoAttrsConfig(t *testing.T) {
	testTable, err := clickhouse.NewTable(`CREATE TABLE `+TableName+`
		( "message" String, "timestamp" DateTime64(3, 'UTC') )
		ENGINE = Memory`,
		clickhouse.NewChTableConfigNoAttrs(),
	)
	if err != nil {
		t.Fatal(err)
	}
	lm := clickhouse.NewLogManager(clickhouse.TableMap{TableName: testTable}, make(clickhouse.TableMap))
	cw := ClickhouseQueryTranslator{lm}
	for _, tt := range testdata.TestsSearchNoAttrs {
		t.Run(tt.Name, func(t *testing.T) {
			simpleQuery, queryType := cw.ParseQuery(tt.QueryJson)
			assert.True(t, simpleQuery.CanParse)
			assert.Contains(t, tt.WantedSql, simpleQuery.Sql.Stmt)
			assert.Equal(t, tt.WantedQueryType, queryType)

			query := cw.BuildSimpleSelectQuery(TableName, simpleQuery.Sql.Stmt)
			assert.Contains(t, tt.WantedQuery, *query)
		})
	}
}

func TestFilterNonEmpty(t *testing.T) {
	tests := []struct {
		array    []Statement
		filtered []Statement
	}{
		{
			[]Statement{NewSimpleStatement(""), NewSimpleStatement("")},
			[]Statement{},
		},
		{
			[]Statement{NewSimpleStatement(""), NewSimpleStatement("a"), NewCompoundStatement("")},
			[]Statement{NewSimpleStatement("a")},
		},
		{
			[]Statement{NewCompoundStatement("a"), NewSimpleStatement("b"), NewCompoundStatement("c")},
			[]Statement{NewCompoundStatement("a"), NewSimpleStatement("b"), NewCompoundStatement("c")},
		},
	}
	for i, tt := range tests {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			assert.Equal(t, tt.filtered, filterNonEmpty(tt.array))
		})
	}
}

func TestOrAndAnd(t *testing.T) {
	tests := []struct {
		stmts []Statement
		want  Statement
	}{
		{
			[]Statement{NewSimpleStatement("a"), NewSimpleStatement("b"), NewSimpleStatement("c")},
			NewCompoundStatement("a AND b AND c"),
		},
		{
			[]Statement{NewSimpleStatement("a"), NewSimpleStatement(""), NewCompoundStatement(""), NewCompoundStatement("b")},
			NewCompoundStatement("a AND (b)"),
		},
		{
			[]Statement{NewSimpleStatement(""), NewSimpleStatement(""), NewSimpleStatement("a"), NewCompoundStatement(""), NewSimpleStatement(""), NewCompoundStatement("")},
			NewSimpleStatement("a"),
		},
		{
			[]Statement{NewSimpleStatement(""), NewSimpleStatement(""), NewSimpleStatement(""), NewSimpleStatement("")},
			NewSimpleStatement(""),
		},
		{
			[]Statement{NewCompoundStatement("a AND b"), NewCompoundStatement("c AND d"), NewCompoundStatement("e AND f")},
			NewCompoundStatement("(a AND b) AND (c AND d) AND (e AND f)"),
		},
	}
	// copy, because and() and or() modify the slice
	for i, tt := range tests {
		t.Run("AND "+strconv.Itoa(i), func(t *testing.T) {
			b := make([]Statement, len(tt.stmts))
			copy(b, tt.stmts)
			assert.Equal(t, tt.want, and(b))
		})
	}
	for i, tt := range tests {
		t.Run("OR "+strconv.Itoa(i), func(t *testing.T) {
			tt.want.Stmt = strings.ReplaceAll(tt.want.Stmt, "AND", "OR")
			for i := range tt.stmts {
				tt.stmts[i].Stmt = strings.ReplaceAll(tt.stmts[i].Stmt, "AND", "OR")
			}
			assert.Equal(t, tt.want, or(tt.stmts))
		})
	}
}
