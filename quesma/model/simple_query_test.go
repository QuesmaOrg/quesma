package model

import (
	"github.com/stretchr/testify/assert"
	"strconv"
	"strings"
	"testing"
)

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
			[]Statement{NewSimpleStatement(""), NewSimpleStatement("a"), NewCompoundStatementNoFieldName("")},
			[]Statement{NewSimpleStatement("a")},
		},
		{
			[]Statement{NewCompoundStatementNoFieldName("a"), NewSimpleStatement("b"), NewCompoundStatement("c", "d")},
			[]Statement{NewCompoundStatementNoFieldName("a"), NewSimpleStatement("b"), NewCompoundStatement("c", "d")},
		},
	}
	for i, tt := range tests {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			assert.Equal(t, tt.filtered, FilterNonEmpty(tt.array))
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
			NewCompoundStatementNoFieldName("a AND b AND c"),
		},
		{
			[]Statement{NewSimpleStatement("a"), NewSimpleStatement(""), NewCompoundStatementNoFieldName(""), NewCompoundStatementNoFieldName("b")},
			NewCompoundStatementNoFieldName("a AND (b)"),
		},
		{
			[]Statement{NewSimpleStatement(""), NewSimpleStatement(""), NewSimpleStatement("a"), NewCompoundStatementNoFieldName(""), NewSimpleStatement(""), NewCompoundStatementNoFieldName("")},
			NewSimpleStatement("a"),
		},
		{
			[]Statement{NewSimpleStatement(""), NewSimpleStatement(""), NewSimpleStatement(""), NewSimpleStatement("")},
			NewSimpleStatement(""),
		},
		{
			[]Statement{NewCompoundStatementNoFieldName("a AND b"), NewCompoundStatementNoFieldName("c AND d"), NewCompoundStatement("e AND f", "field")},
			NewCompoundStatement("(a AND b) AND (c AND d) AND (e AND f)", "field"),
		},
	}
	// copy, because and() and or() modify the slice
	for i, tt := range tests {
		t.Run("AND "+strconv.Itoa(i), func(t *testing.T) {
			b := make([]Statement, len(tt.stmts))
			copy(b, tt.stmts)
			assert.Equal(t, tt.want, And(b))
		})
	}
	for i, tt := range tests {
		t.Run("OR "+strconv.Itoa(i), func(t *testing.T) {
			tt.want.Stmt = strings.ReplaceAll(tt.want.Stmt, "AND", "OR")
			for i := range tt.stmts {
				tt.stmts[i].Stmt = strings.ReplaceAll(tt.stmts[i].Stmt, "AND", "OR")
			}
			assert.Equal(t, tt.want, Or(tt.stmts))
		})
	}
}
