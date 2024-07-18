// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package model

import (
	"fmt"
	"github.com/k0kubun/pp"
	"strconv"
	"strings"
)

type newRenderer struct{}

// AsString renders the given expression to string which can be used to build SQL query
func AsStringNew(expr Expr) string {
	if expr == nil {
		return ""
	}
	return expr.Accept(&newRenderer{}).(string)
}

func (v *newRenderer) VisitColumnRef(e ColumnRef) interface{} {
	name := strings.TrimSuffix(e.ColumnName, ".keyword")
	name = strings.TrimSuffix(name, "::keyword") // Not sure if this is the best place to do this
	return strconv.Quote(name)
}

func (v *newRenderer) VisitPrefixExpr(e PrefixExpr) interface{} {
	args := make([]string, len(e.Args))
	for i, arg := range e.Args {
		if arg != nil {
			args[i] = arg.Accept(v).(string)
		}
	}

	argsAsString := strings.Join(args, ", ")
	return fmt.Sprintf("%v (%v)", e.Op, argsAsString)
}

func (v *newRenderer) VisitNestedProperty(e NestedProperty) interface{} {
	return fmt.Sprintf("%v.%v", e.ColumnRef.Accept(v), e.PropertyName.Accept(v))
}

func (v *newRenderer) VisitArrayAccess(e ArrayAccess) interface{} {
	return fmt.Sprintf("%v[%v]", e.ColumnRef.Accept(v), e.Index.Accept(v))
}

func (v *newRenderer) VisitFunction(e FunctionExpr) interface{} {
	args := make([]string, 0)
	for _, arg := range e.Args {
		args = append(args, arg.Accept(v).(string))
	}
	return e.Name + "(" + strings.Join(args, ",") + ")"
}

func (v *newRenderer) VisitLiteral(l LiteralExpr) interface{} {

	if l.Value == "*" {
		return "*"
	}

	switch l.Value.(type) {
	case string:
		return fmt.Sprintf("%s", l.Value)
	case float64:
		return fmt.Sprintf("%f", l.Value)
	default:
		return fmt.Sprintf("%v", l.Value)
	}
}

func (v *newRenderer) VisitString(e StringExpr) interface{} {
	return e.Value
}

func (v *newRenderer) VisitMultiFunction(f MultiFunctionExpr) interface{} {
	args := make([]string, 0)
	for _, arg := range f.Args {
		r := "(" + arg.Accept(v).(string) + ")"
		args = append(args, r)
	}
	return f.Name + strings.Join(args, "")
}

func (v *newRenderer) VisitInfix(e InfixExpr) interface{} {
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
	if e.Op == "AND" || e.Op == "OR" {
		return fmt.Sprintf("(%v %v %v)", lhs, e.Op, rhs)
	} else if strings.Contains(e.Op, "LIKE") || e.Op == "IS" || e.Op == "IN" || e.Op == "REGEXP" {
		return fmt.Sprintf("%v %v %v", lhs, e.Op, rhs)
	} else {
		return fmt.Sprintf("%v%v%v", lhs, e.Op, rhs)
	}
}

func (v *newRenderer) VisitOrderByExpr(e OrderByExpr) interface{} {
	var exprsAsStr []string
	for _, expr := range e.Exprs {
		exprsAsStr = append(exprsAsStr, expr.Accept(v).(string))
	}
	allExprs := strings.Join(exprsAsStr, ", ")
	if e.Direction == DescOrder {
		return fmt.Sprintf("%s %s", allExprs, "DESC")
	}
	if e.Direction == AscOrder {
		return fmt.Sprintf("%s %s", allExprs, "ASC")
	}
	return allExprs
}

func (v *newRenderer) VisitDistinctExpr(e DistinctExpr) interface{} {
	return fmt.Sprintf("DISTINCT %s", e.Expr.Accept(v).(string))
}

func (v *newRenderer) VisitTableRef(e TableRef) interface{} {
	return e.Name
}

func (v *newRenderer) VisitAliasedExpr(e AliasedExpr) interface{} {
	return fmt.Sprintf("%s AS %s", e.Expr.Accept(v).(string), strconv.Quote(e.Alias))
}

func (v *newRenderer) VisitExprArray(exprs []Expr) interface{} {
	asString := make([]string, 0, len(exprs))
	for _, expr := range exprs {
		asString = append(asString, expr.Accept(v).(string))
	}
	return strings.Join(asString, ", ")
}

func (v *newRenderer) neworderprinter(orderBy OrderByExpr) string {
	var sb strings.Builder
	for _, expr := range orderBy.Exprs {
		if aliased, ok := expr.(AliasedExpr); ok {
			sb.WriteString(aliased.Alias)
		} else {
			sb.WriteString(expr.Accept(v).(string))
		}
	}
	if orderBy.Direction == DescOrder {
		sb.WriteString(" DESC")
	}
	return sb.String()
}

func (v *newRenderer) VisitOrderExprArray(exprs []OrderByExpr) interface{} {
	asString := make([]string, 0, len(exprs))
	for _, expr := range exprs {
		asString = append(asString, v.neworderprinter(expr))
	}
	return strings.Join(asString, ", ")
}

func (v *newRenderer) VisitSelectCommand(c SelectCommand) interface{} {
	// THIS SHOULD PRODUCE QUERY IN  BRACES
	var sb strings.Builder

	pp.Println("newColumns:", c.newColumns)
	pp.Println("newGroupBy:", c.newGroupBy)

	weHaveCTE := len(c.newFullGroupBy) > 0

	if weHaveCTE {
		sb.WriteString(fmt.Sprintf("WITH cte AS (SELECT %s FROM %s GROUP BY %s) ",
			v.VisitExprArray(c.newColumns), AsString(c.FromClause), v.VisitExprArray(c.newFullGroupBy)))
	}

	sb.WriteString("SELECT ")
	if c.IsDistinct {
		sb.WriteString("DISTINCT ")
	}
	colsOnlyAliases := make([]string, 0, len(c.newColumns))
	for _, col := range c.newColumns {
		// TODO probably change to some visitor, or prettify in other way. POC style for now.
		if aliased, ok := col.(AliasedExpr); ok {
			colsOnlyAliases = append(colsOnlyAliases, strconv.Quote(aliased.Alias))
		} else if colRef, ok := col.(ColumnRef); ok {
			colsOnlyAliases = append(colsOnlyAliases, strconv.Quote(colRef.ColumnName))
		}
	}
	sb.WriteString(strings.Join(colsOnlyAliases, ", "))
	denseRanks := make([]string, 0)
	denseRankSizes := make([]int, 0)
	for i := range c.newFullGroupBy {
		if c.newGroupBySize[i] != 0 {
			partitionBy := ""
			if i > 0 {
				partitionBy = "PARTITION BY " + v.VisitExprArray(c.newGroupBy[:i]).(string) + " "
			}
			denseRanks = append(denseRanks, fmt.Sprintf("DENSE_RANK() OVER (%sORDER BY %s) AS dense_rank_%d", partitionBy, v.VisitOrderExprArray(c.newOrderBy[i]), i))
			denseRankSizes = append(denseRankSizes, c.newGroupBySize[i])
		}
	}
	sb.WriteString(", " + strings.Join(denseRanks, ", "))

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
		innerColumn := make([]string, 0)
		for _, col := range c.Columns {
			if _, ok := col.(ColumnRef); ok {
				innerColumn = append(innerColumn, AsString(col))
			}
			if aliased, ok := col.(AliasedExpr); ok {
				if v, ok := aliased.Expr.(ColumnRef); ok {
					innerColumn = append(innerColumn, AsString(v))
				}
			}
		}
		if len(innerColumn) == 0 {
			innerColumn = append(innerColumn, "1")
		}
		sb.WriteString(strings.Join(innerColumn, ", "))
		sb.WriteString(" FROM ")
	}
	/* HACK ALERT END */

	if weHaveCTE {
		sb.WriteString("cte")
	} else {
		sb.WriteString(AsString(c.FromClause))
	}

	if c.WhereClause != nil {
		sb.WriteString(" WHERE ")
		sb.WriteString(AsString(c.WhereClause))
	}
	if c.SampleLimit > 0 {
		sb.WriteString(fmt.Sprintf(" LIMIT %d)", c.SampleLimit))
	}

	qualify := make([]string, 0, len(denseRanks))
	orderBy := make([]string, 0, len(denseRanks))
	for i, size := range denseRankSizes {
		qualify = append(qualify, fmt.Sprintf("dense_rank_%d <= %d", i, size))
		orderBy = append(orderBy, fmt.Sprintf("dense_rank_%d", i))
	}
	sb.WriteString(fmt.Sprintf(" QUALIFY %s ORDER BY %s", strings.Join(qualify, " AND "), strings.Join(orderBy, ", ")))

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

func (v *newRenderer) VisitWindowFunction(f WindowFunction) interface{} {
	args := make([]string, 0)
	for _, arg := range f.Args {
		args = append(args, AsString(arg))
	}
	partitionBy := make([]string, 0)
	for _, col := range f.PartitionBy {
		partitionBy = append(partitionBy, AsString(col))
	}

	var sb strings.Builder
	stmtWithoutOrderBy := fmt.Sprintf("%s OVER (PARTITION BY %s", f.Name, strings.Join(partitionBy, ", "))
	sb.WriteString(stmtWithoutOrderBy)

	if len(f.OrderBy.Exprs) != 0 {
		sb.WriteString(" ORDER BY ")
		sb.WriteString(AsString(f.OrderBy))
	}
	sb.WriteString(")")
	return sb.String()
}

func (v *newRenderer) VisitParenExpr(p ParenExpr) interface{} {
	var exprs []string
	for _, expr := range p.Exprs {
		exprs = append(exprs, expr.Accept(v).(string))
	}
	return fmt.Sprintf("(%s)", strings.Join(exprs, " "))
}

func (v *newRenderer) VisitLambdaExpr(l LambdaExpr) interface{} {
	return fmt.Sprintf("(%s) -> %s", strings.Join(l.Args, ", "), AsString(l.Body))
}
