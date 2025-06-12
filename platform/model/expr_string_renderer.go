// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package model

import (
	"fmt"
	"github.com/QuesmaOrg/quesma/platform/logger"
	"github.com/QuesmaOrg/quesma/platform/types"
	"github.com/QuesmaOrg/quesma/platform/util"
	"regexp"
	"sort"
	"strings"
)

var identifierRegexp = regexp.MustCompile(`^([a-zA-Z_][a-zA-Z0-9_]*|".*")$`)

type renderer struct{}

// AsString renders the given expression to string which can be used to build SQL query
func AsString(expr Expr) string {
	if expr == nil {
		return ""
	}
	return expr.Accept(&renderer{}).(string)
}

func (v *renderer) VisitColumnRef(e ColumnRef) interface{} {
	// TODO this should be done as the last step in the pipeline, not here
	name := strings.TrimSuffix(e.ColumnName, types.MultifieldKeywordSuffix)
	name = strings.TrimSuffix(name, "::keyword") // TODO is this needed?
	name = strings.TrimSuffix(name, types.MultifieldMapKeysSuffix)
	name = strings.TrimSuffix(name, types.MultifieldMapValuesSuffix)
	if len(e.TableAlias) > 0 {
		return fmt.Sprintf("%s.%s", util.BackquoteIdentifier(e.TableAlias), util.BackquoteIdentifier(name))
	} else {
		return util.BackquoteIdentifier(name)
	}
}

func (v *renderer) VisitPrefixExpr(e PrefixExpr) interface{} {
	args := make([]string, len(e.Args))
	for i, arg := range e.Args {
		if arg != nil {
			args[i] = arg.Accept(v).(string)
		}
	}

	argsAsString := strings.Join(args, ", ")
	return fmt.Sprintf("%v (%v)", e.Op, argsAsString)
}

func (v *renderer) VisitNestedProperty(e NestedProperty) interface{} {
	return fmt.Sprintf("%v.%v", e.ObjectExpr.Accept(v), e.PropertyName.Accept(v))
}

func (v *renderer) VisitArrayAccess(e ArrayAccess) interface{} {
	return fmt.Sprintf("%v[%v]", e.ColumnRef.Accept(v), e.Index.Accept(v))
}

func (v *renderer) VisitFunction(e FunctionExpr) interface{} {
	args := make([]string, 0)
	for _, arg := range e.Args {
		args = append(args, arg.Accept(v).(string))
	}
	return e.Name + "(" + strings.Join(args, ",") + ")"
}

func (v *renderer) VisitLiteral(l LiteralExpr) interface{} {
	switch val := l.Value.(type) {
	case string:
		switch l.EscapeType {
		case NormalNotEscaped:
			return escapeStringNormal(val)
		case NotEscapedLikePrefix:
			return util.SingleQuote(escapeStringLike(escapeStringNormal(val)) + "%")
		case NotEscapedLikeFull:
			withoutPercents := escapeStringLike(escapeStringNormal(val))
			if util.IsSingleQuoted(val) {
				withoutPercents = strings.Trim(withoutPercents, "'")
			}
			return util.SingleQuote(withoutPercents)
		case FullyEscaped:
			if util.IsSingleQuoted(val) {
				return val
			}
			return util.SingleQuote(val)
		default:
			logger.WarnWithThrottling("unknown_literal", "VisitLiteral %s", val)
			return escapeStringNormal(val) // like normal
		}
	default:
		return fmt.Sprintf("%v", val)
	}
}

func (v *renderer) VisitTuple(t TupleExpr) interface{} {
	exprs := make([]string, 0, len(t.Exprs))
	for _, expr := range t.Exprs {
		exprs = append(exprs, expr.Accept(v).(string))
	}
	switch len(exprs) {
	case 0:
		logger.WarnWithThrottling("visitTuple", "tupleExpr with no expressions") // hacky way to log this
		return "tuple()"
	case 1:
		return exprs[0]
	default:
		return fmt.Sprintf("tuple(%s)", strings.Join(exprs, ", ")) // can omit "tuple", but I think SQL's more readable with it
	}
}

func (v *renderer) VisitInfix(e InfixExpr) interface{} {
	var lhs, rhs interface{} // TODO FOR NOW LITTLE PARANOID BUT HELPS ME NOT SEE MANY PANICS WHEN TESTING
	if e.Left != nil {
		lhs = e.Left.Accept(v)
	} else {
		lhs = "< LHS NIL >"
	}
	if e.Right != nil {
		rhs = e.Right.Accept(v)
	} else {
		rhs = "< RHS NIL >"
	}

	// This might look like a strange heuristics to but is aligned with the way we are currently generating the statement
	// I think in the future every infix op should be in braces.
	if (strings.HasPrefix(e.Op, "_") && e.Op != MatchOperator) || e.Op == "AND" || e.Op == "OR" { // LIKE is without (), so I propose MatchOperator as well
		return fmt.Sprintf("(%v %v %v)", lhs, e.Op, rhs)
	} else if strings.Contains(e.Op, "LIKE") || strings.Contains(e.Op, "MATCH") || e.Op == MatchOperator || e.Op == "IS" || e.Op == "IN" || e.Op == "NOT IN" || e.Op == "REGEXP" || strings.Contains(e.Op, "UNION") {
		return fmt.Sprintf("%v %v %v", lhs, e.Op, rhs)
	} else {
		return fmt.Sprintf("%v%v%v", lhs, e.Op, rhs)
	}
}

func (v *renderer) VisitGroupByExpr(e GroupByExpr) interface{} {
	return fmt.Sprintf("%s ", e.Expr.Accept(v).(string))
}

func (v *renderer) VisitOrderByExpr(e OrderByExpr) interface{} {
	allExprs := e.Expr.Accept(v).(string)
	if e.Direction == DescOrder {
		return fmt.Sprintf("%s %s", allExprs, "DESC")
	}
	if e.Direction == AscOrder {
		return fmt.Sprintf("%s %s", allExprs, "ASC")
	}
	return allExprs
}

func (v *renderer) VisitDistinctExpr(e DistinctExpr) interface{} {
	return fmt.Sprintf("DISTINCT %s", e.Expr.Accept(v).(string))
}

func (v *renderer) VisitTableRef(e TableRef) interface{} {
	var result []string

	if e.DatabaseName != "" {
		result = append(result, util.BackquoteIdentifier(e.DatabaseName))
	}
	// append table name
	result = append(result, util.BackquoteIdentifier(e.Name))

	return strings.Join(result, ".")
}

func (v *renderer) VisitAliasedExpr(e AliasedExpr) interface{} {
	if e.Alias == "" {
		return fmt.Sprintf("%s ", e.Expr.Accept(v).(string))
	}
	if util.ContainsKeyword(e.Alias) {
		return fmt.Sprintf("%s AS %s", e.Expr.Accept(v).(string), e.Alias)
	}
	return fmt.Sprintf("%s AS %s", e.Expr.Accept(v).(string), util.BackquoteIdentifier(e.Alias))
}

func (v *renderer) VisitSelectCommand(c SelectCommand) interface{} {
	// THIS SHOULD PRODUCE QUERY IN  BRACES
	var sb strings.Builder

	if len(c.NamedCTEs) > 0 {
		sb.WriteString("WITH ")
	}

	var namedCTEsAsString []string
	for _, cte := range c.NamedCTEs {
		namedCTEsAsString = append(namedCTEsAsString, cte.Accept(v).(string))
	}
	sb.WriteString(strings.Join(namedCTEsAsString, ", "))

	sb.WriteString("SELECT ")
	if c.IsDistinct {
		sb.WriteString("DISTINCT ")
	}

	columns := make([]string, 0)

	for _, col := range c.Columns {
		columns = append(columns, AsString(col))
	}

	sb.WriteString(strings.Join(columns, ", "))

	sb.WriteString(" FROM ")
	/* HACK ALERT BEGIN */
	// There are some aggregations that look like they are nested queries, but they aren't properly built as such
	// Instead these are printed out in a smart way, handled by the logic below
	// Example of such query is
	//=== RUN   Test2AggregationParserExternalTestcases/date_histogram(2)
	//SELECT count()
	//FROM (
	//  SELECT 1
	//  FROM "logs-generic-default"
	//  WHERE ("timestamp">=parseDateTime64BestEffort('2024-02-02T13:47:16.029Z') AND
	//    "timestamp"<=parseDateTime64BestEffort('2024-02-09T13:47:16.029Z'))
	//  LIMIT 12)
	if c.SampleLimit > 0 {
		sb.WriteString("(SELECT ")
		usedColumns := make(map[string]bool)
		for _, col := range append(c.Columns, c.GroupBy...) {
			for _, usedCol := range GetUsedColumns(col) {
				usedColumns[AsString(usedCol)] = true
			}
		}
		if len(usedColumns) == 0 {
			sb.WriteString("1") // if no columns are used, it is simple count, 1 is enough
		} else {
			usedKeys := make([]string, 0, len(usedColumns))
			for key := range usedColumns {
				usedKeys = append(usedKeys, key)
			}
			sort.Strings(usedKeys)
			sb.WriteString(strings.Join(usedKeys, ", "))
		}
		sb.WriteString(" FROM ")
	}
	/* HACK ALERT END */
	if c.FromClause != nil {
		// Non-nested FROM clauses don't have to be wrapped in parentheses
		if _, isTableRef := c.FromClause.(TableRef); isTableRef {
			sb.WriteString(AsString(c.FromClause))
		} else if _, isLiteral := c.FromClause.(LiteralExpr); isLiteral {
			sb.WriteString(AsString(c.FromClause))
		} else if _, isJoinExpr := c.FromClause.(JoinExpr); isJoinExpr {
			sb.WriteString(AsString(c.FromClause))
		} else {
			// Nested sub-query
			sb.WriteString(fmt.Sprintf("(%s) tmp_tab", AsString(c.FromClause)))
		}
	}
	if c.WhereClause != nil {
		sb.WriteString(" WHERE ")
		sb.WriteString(AsString(c.WhereClause))
	}
	if c.SampleLimit > 0 {
		sb.WriteString(fmt.Sprintf(" LIMIT %d) tmp_tab ", c.SampleLimit))
	}

	groupBy := make([]string, 0, len(c.GroupBy))
	for _, col := range c.GroupBy {
		groupBy = append(groupBy, AsString(col))
	}
	if len(groupBy) > 0 {
		var usedKeys []string
		if c.SampleLimit > 0 {
			// for doris group by query
			usedColumns := make(map[string]bool)
			for _, col := range append(c.Columns, c.GroupBy...) {
				for _, usedCol := range GetUsedColumns(col) {
					usedColumns[AsString(usedCol)] = true
				}
			}
			usedKeys = make([]string, 0, len(usedColumns))
			for key := range usedColumns {
				usedKeys = append(usedKeys, key)
			}
		}
		if usedKeys != nil {
			sort.Strings(usedKeys)
			sb.WriteString(" GROUP BY ")
			sb.WriteString(strings.Join(usedKeys, ", "))
		} else {
			sb.WriteString(" GROUP BY ")
			fullGroupBy := groupBy
			sb.WriteString(strings.Join(fullGroupBy, ", "))
		}
	}

	orderBy := make([]string, 0, len(c.OrderBy))
	for _, col := range c.OrderBy {
		orderBy = append(orderBy, AsString(col))
	}
	if len(orderBy) > 0 {
		sb.WriteString(" ORDER BY ")
		sb.WriteString(strings.Join(orderBy, ", "))
	}

	if c.Limit != noLimit {
		if len(c.LimitBy) <= 1 {
			sb.WriteString(fmt.Sprintf(" LIMIT %d", c.Limit))
		} else {
			limitBys := make([]string, 0, len(c.LimitBy)-1)
			for _, col := range c.LimitBy[:len(c.LimitBy)-1] {
				limitBys = append(limitBys, AsString(col))
			}
			sb.WriteString(fmt.Sprintf(" LIMIT %d BY %s", c.Limit, strings.Join(limitBys, ", ")))
		}
	}

	return sb.String()
}

func (v *renderer) VisitWindowFunction(f WindowFunction) interface{} {
	args := make([]string, 0)
	for _, arg := range f.Args {
		args = append(args, AsString(arg))
	}

	var sb strings.Builder
	var stmtWithoutOrderBy string
	if f.Name == "" {
		stmtWithoutOrderBy = fmt.Sprintf("%s OVER (", strings.Join(args, ", "))
	} else {
		stmtWithoutOrderBy = fmt.Sprintf("%s(%s) OVER (", f.Name, strings.Join(args, ", "))
	}
	sb.WriteString(stmtWithoutOrderBy)

	if len(f.PartitionBy) > 0 {
		sb.WriteString("PARTITION BY ")

		partitionBy := make([]string, 0)
		for _, col := range f.PartitionBy {
			partitionBy = append(partitionBy, AsString(col))
		}
		sb.WriteString(strings.Join(partitionBy, ", "))
	}

	if len(f.OrderBy) > 0 {
		if len(f.PartitionBy) > 0 {
			sb.WriteString(" ")
		}
		sb.WriteString("ORDER BY ")
		var orderByStr []string
		for _, orderBy := range f.OrderBy {
			orderByStr = append(orderByStr, AsString(orderBy))
		}
		sb.WriteString(strings.Join(orderByStr, ", "))
	}
	sb.WriteString(")")
	return sb.String()
}

func (v *renderer) VisitParenExpr(p ParenExpr) interface{} {
	var exprs []string
	for _, expr := range p.Exprs {
		exprs = append(exprs, expr.Accept(v).(string))
	}
	return fmt.Sprintf("(%s)", strings.Join(exprs, " "))
}

func (v *renderer) VisitLambdaExpr(l LambdaExpr) interface{} {
	return fmt.Sprintf("(%s) -> %s", strings.Join(l.Args, ", "), AsString(l.Body))
}

func (v *renderer) VisitJoinExpr(j JoinExpr) interface{} {

	var sb strings.Builder

	var join *JoinExpr

	join = &j

	sb.WriteString(join.Lhs.Accept(v).(string))

	for join != nil {

		var nextJoin *JoinExpr

		sb.WriteString(" ")
		sb.WriteString(join.JoinType)
		sb.WriteString(" JOIN ")

		if rhsJoin, ok := join.Rhs.(JoinExpr); ok {
			sb.WriteString(rhsJoin.Lhs.Accept(v).(string))
			nextJoin = &rhsJoin
		} else {
			sb.WriteString(join.Rhs.Accept(v).(string))
		}

		sb.WriteString(" ON ")
		sb.WriteString("(")
		sb.WriteString(join.On.Accept(v).(string))
		sb.WriteString(")")

		join = nextJoin
	}

	return sb.String()
}

func (v *renderer) VisitCTE(c CTE) interface{} {
	return fmt.Sprintf("%s AS (%s) ", c.Name, AsString(c.SelectCommand))
}

// escapeStringNormal escapes the given string so that it can be used in a SQL Clickhouse query.
// It escapes ' and \ characters: ' -> \', \ -> \\.
func escapeStringNormal(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`) // \ should be escaped with no exceptions
	if len(s) >= 2 && s[0] == '\'' && s[len(s)-1] == '\'' {
		// don't escape the first and last '
		return util.SingleQuote(strings.ReplaceAll(s[1:len(s)-1], `'`, `\'`))
	}
	return strings.ReplaceAll(s, `'`, `\'`)
}

// escapeStringLike escapes the given string so that it can be used in a SQL 'LIKE' query.
// (% and _ are special characters there and need to be escaped)
func escapeStringLike(s string) string {
	s = strings.ReplaceAll(s, `%`, `\%`)
	return strings.ReplaceAll(s, `_`, `\_`)
}
