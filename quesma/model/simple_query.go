package model

import (
	"context"
	"mitmproxy/quesma/logger"
)

type SimpleQuery struct {
	Sql        Statement
	CanParse   bool
	FieldName  string
	SortFields []string
}

func NewSimpleQuery(sql Statement, canParse bool) SimpleQuery {
	return SimpleQuery{Sql: sql, CanParse: canParse}
}

func NewSimpleQueryWithFieldName(sql Statement, canParse bool, fieldName string) SimpleQuery {
	return SimpleQuery{Sql: sql, CanParse: canParse, FieldName: fieldName}
}

func (sq *SimpleQuery) CombineWheresWith(ctx context.Context, sq2 SimpleQuery) {
	sq.Sql = And([]Statement{sq.Sql, sq2.Sql})
	sq.CanParse = sq.CanParse && sq2.CanParse
	if len(sq.FieldName) > 0 && len(sq2.FieldName) > 0 && sq.FieldName != sq2.FieldName {
		logger.WarnWithCtx(ctx).Msgf("combining 2 where clauses with different field names: %s, %s, where queries: %v %v", sq.FieldName, sq2.FieldName, sq, sq2)
	}
	if len(sq.FieldName) == 0 && len(sq2.FieldName) > 0 {
		sq.FieldName = sq2.FieldName
	}
}

type Statement struct {
	Stmt       string
	IsCompound bool // "a" -> not compound, "a AND b" -> compound. Used to not make unnecessary brackets (not always, but usually)
	FieldName  string
}

func NewSimpleStatement(stmt string) Statement {
	return Statement{Stmt: stmt, IsCompound: false}
}

func NewCompoundStatement(stmt, fieldName string) Statement {
	return Statement{Stmt: stmt, IsCompound: true, FieldName: fieldName}
}

func NewCompoundStatementNoFieldName(stmt string) Statement {
	return Statement{Stmt: stmt, IsCompound: true}
}

// Added to the generated SQL where the query is fine, but we're sure no rows will match it
var AlwaysFalseStatement = NewSimpleStatement("false")

func And(andStmts []Statement) Statement {
	return combineStatements(andStmts, "AND")
}

func Or(orStmts []Statement) Statement {
	return combineStatements(orStmts, "OR")
}

func FilterNonEmpty(slice []Statement) []Statement {
	i := 0
	for _, el := range slice {
		if len(el.Stmt) > 0 {
			slice[i] = el
			i++
		}
	}
	return slice[:i]
}

// sep = "AND" or "OR"
func combineStatements(stmts []Statement, sep string) Statement {
	stmts = FilterNonEmpty(stmts)
	if len(stmts) > 1 {
		stmts = quoteWithBracketsIfCompound(stmts)
		var fieldName string
		sql := ""
		for i, stmt := range stmts {
			sql += stmt.Stmt
			if i < len(stmts)-1 {
				sql += " " + sep + " "
			}
			if stmt.FieldName != "" {
				fieldName = stmt.FieldName
			}
		}
		return NewCompoundStatement(sql, fieldName)
	}
	if len(stmts) == 1 {
		return stmts[0]
	}
	return NewSimpleStatement("")
}

// used to combine statements with AND/OR
// [a, b, a AND b] ==> ["a", "b", "(a AND b)"]
func quoteWithBracketsIfCompound(slice []Statement) []Statement {
	for i := range slice {
		if slice[i].IsCompound {
			slice[i].Stmt = "(" + slice[i].Stmt + ")"
		}
	}
	return slice
}
