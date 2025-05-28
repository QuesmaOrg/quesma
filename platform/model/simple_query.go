// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package model

type SimpleQuery struct {
	WhereClause    Expr
	OrderBy        []OrderByExpr
	SortFieldNames []string // SortFieldNames is used to preserve fields listed in the `sort` part of the query.
	// This can be different from the OrderBy clause, as it may contain Elasticsearch-internal fields like `_doc`.
	// In that case, it is not reflected in the OrderBy clause, but is still used for assembling the response.
	CanParse bool
	// NeedCountWithLimit > 0 means we need count(*) LIMIT NeedCountWithLimit
	// NeedCountWithLimit 0 (WeNeedUnlimitedCount) means we need count(*) (unlimited)
	// NeedCountWithLimit -1 (WeDontNeedCount) means we don't need a count(*) query
	NeedCountWithLimit int
}

const (
	WeNeedUnlimitedCount = -1
	WeDontNeedCount      = 0
)

func (s *SimpleQuery) WhereClauseAsString() string {
	if s.WhereClause == nil {
		return ""
	}
	return AsString(s.WhereClause)
}

func NewSimpleQuery(whereClause Expr, canParse bool) SimpleQuery {
	return SimpleQuery{WhereClause: whereClause, CanParse: canParse}
}

func NewSimpleQueryInvalid() SimpleQuery {
	return SimpleQuery{CanParse: false}
}

// LimitForCount returns (limit, true) if we need count(*) with limit,
// (not-important, false) if we don't need count/limit
func (s *SimpleQuery) LimitForCount() (limit int, doWeNeedLimit bool) {
	return s.NeedCountWithLimit, s.NeedCountWithLimit != WeDontNeedCount && s.NeedCountWithLimit != WeNeedUnlimitedCount
}

func And(andStmts []Expr) Expr {
	return combineStatements(andStmts, "AND")
}

func Or(orStmts []Expr) Expr {
	return combineStatements(orStmts, "OR")
}

// operator = "AND" or "OR"
func combineStatements(stmtsToCombine []Expr, operator string) Expr {
	stmts := FilterOutEmptyStatements(stmtsToCombine)
	var newWhereStatement Expr
	if len(stmts) > 1 {
		newWhereStatement = stmts[0]
		for _, stmt := range stmts[1:] {
			newWhereStatement = NewInfixExpr(newWhereStatement, operator, stmt)
		}
		return newWhereStatement
	}
	if len(stmts) == 1 {
		return stmts[0]
	}
	return nil
}

func FilterOutEmptyStatements(stmts []Expr) []Expr {
	var nonEmptyStmts []Expr
	for _, stmt := range stmts {
		if stmt != nil {
			nonEmptyStmts = append(nonEmptyStmts, stmt)
		}
	}
	return nonEmptyStmts
}
