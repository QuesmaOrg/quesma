package model

import (
	"context"
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/queryparser/where_clause"
)

type SimpleQuery struct {
	WhereClause where_clause.Statement
	// deprecated
	Sql        Statement // This will be removed as soon as WhereClause is used everywhere
	CanParse   bool
	FieldName  string
	SortFields []SortField
}

var asString = where_clause.StringRenderer{}

func (s *SimpleQuery) WhereClauseAsString() string {
	if s.WhereClause == nil {
		return ""
	}
	return (s.WhereClause).Accept(&asString).(string)
}

func NewSimpleQuery(whereClause where_clause.Statement, canParse bool) SimpleQuery {
	return SimpleQuery{WhereClause: whereClause, CanParse: canParse}
}

func NewSimpleQueryWithFieldName(whereClause where_clause.Statement, canParse bool, fieldName string) SimpleQuery {
	return SimpleQuery{WhereClause: whereClause, CanParse: canParse, FieldName: fieldName}
}

// deprecated
type Statement struct {
	// deprecated
	Stmt           string                 // We're moving to the new WhereStatement which should also remove the need for IsCompound and FieldName
	WhereStatement where_clause.Statement // New, better and bold version
	IsCompound     bool                   // "a" -> not compound, "a AND b" -> compound. Used to not make unnecessary brackets (not always, but usually)
	FieldName      string
}

//func NewSimpleStatement(stmt string) Statement {
//	return Statement{Stmt: stmt, IsCompound: false}
//}
//
//func NewCompoundStatement(stmt, fieldName string) Statement {
//	return Statement{Stmt: stmt, IsCompound: true, FieldName: fieldName}
//}
//
//func NewCompoundStatementNoFieldName(stmt string) Statement {
//	return Statement{Stmt: stmt, IsCompound: true}
//}

// Added to the generated SQL where the query is fine, but we're sure no rows will match it
//var AlwaysFalseStatement = NewSimpleStatement("false")

func And(andStmts []where_clause.Statement) where_clause.Statement {
	return combineStatements(andStmts, "AND")
}

func Or(orStmts []where_clause.Statement) where_clause.Statement {
	return combineStatements(orStmts, "OR")
}

// operator = "AND" or "OR"
func combineStatements(stmts []where_clause.Statement, operator string) where_clause.Statement {
	stmts = FilterOutEmptyStatements(stmts)
	var newWhereStatement where_clause.Statement
	if len(stmts) > 1 {
		newWhereStatement = stmts[0]
		for _, stmt := range stmts[1:] {
			newWhereStatement = where_clause.NewInfixOp(newWhereStatement, operator, stmt)
		}
		return newWhereStatement
	}
	if len(stmts) == 1 {
		return stmts[0]
	}
	return nil
}

func CombineWheres(ctx context.Context, where1, where2 SimpleQuery) SimpleQuery {
	combinedWhereClause := where_clause.NewInfixOp(where1.WhereClause, "AND", where2.WhereClause)
	combined := SimpleQuery{
		WhereClause: combinedWhereClause,
		CanParse:    where1.CanParse && where2.CanParse,
	}
	if len(where1.FieldName) > 0 && len(where2.FieldName) > 0 && where1.FieldName != where2.FieldName {
		logger.WarnWithCtx(ctx).Msgf("combining 2 where clauses with different field names: %s, %s, where queries: %v %v", where1.FieldName, where2.FieldName, where1, where2)
	}
	if len(where1.FieldName) > 0 {
		combined.FieldName = where1.FieldName
	} else {
		combined.FieldName = where2.FieldName
	}
	return combined
}

func FilterOutEmptyStatements(stmts []where_clause.Statement) []where_clause.Statement {
	var nonEmptyStmts []where_clause.Statement
	for _, stmt := range stmts {
		if stmt != nil {
			nonEmptyStmts = append(nonEmptyStmts, stmt)
		}
	}
	return nonEmptyStmts
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
