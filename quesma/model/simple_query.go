package model

import (
	"context"
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/queryparser/where_clause"
)

type SimpleQuery struct {
	WhereClause where_clause.Statement
	OrderBy     []SelectColumn
	CanParse    bool
	FieldName   string
	// NeedCountWithLimit > 0 means we need count(*) LIMIT NeedCountWithLimit
	// NeedCountWithLimit 0 (WeNeedUnlimitedCount) means we need count(*) (unlimited)
	// NeedCountWithLimit -1 (WeDontNeedCount) means we don't need a count(*) query
	NeedCountWithLimit int
}

const (
	WeNeedUnlimitedCount = -1
	WeDontNeedCount      = 0
)

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

// LimitForCount returns (limit, true) if we need count(*) with limit,
// (not-important, false) if we don't need count/limit
func (s *SimpleQuery) LimitForCount() (limit int, doWeNeedLimit bool) {
	return s.NeedCountWithLimit, s.NeedCountWithLimit != WeDontNeedCount && s.NeedCountWithLimit != WeNeedUnlimitedCount
}

func And(andStmts []where_clause.Statement) where_clause.Statement {
	return combineStatements(andStmts, "AND")
}

func Or(orStmts []where_clause.Statement) where_clause.Statement {
	return combineStatements(orStmts, "OR")
}

// operator = "AND" or "OR"
func combineStatements(stmtsToCombine []where_clause.Statement, operator string) where_clause.Statement {
	stmts := FilterOutEmptyStatements(stmtsToCombine)
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
	var combinedWhereClause where_clause.Statement
	if where1.WhereClause != nil && where2.WhereClause != nil {
		combinedWhereClause = where_clause.NewInfixOp(where1.WhereClause, "AND", where2.WhereClause)
	} else if where1.WhereClause != nil {
		combinedWhereClause = where1.WhereClause
	} else if where2.WhereClause != nil {
		combinedWhereClause = where2.WhereClause
	}
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
