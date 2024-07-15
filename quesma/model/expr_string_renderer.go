// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package model

import (
	"fmt"
	"quesma/logger"
	"strconv"
	"strings"
)

type renderer struct{}

// AsString renders the given expression to string which can be used to build SQL query
func AsString(expr Expr) string {
	if expr == nil {
		return ""
	}
	return expr.Accept(&renderer{}).(string)
}

func (v *renderer) VisitColumnRef(e ColumnRef) interface{} {
	name := strings.TrimSuffix(e.ColumnName, ".keyword")
	name = strings.TrimSuffix(name, "::keyword") // Not sure if this is the best place to do this
	return strconv.Quote(name)
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
	return fmt.Sprintf("%v.%v", e.ColumnRef.Accept(v), e.PropertyName.Accept(v))
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

func (v *renderer) VisitString(e StringExpr) interface{} {
	return e.Value
}

func (v *renderer) VisitMultiFunction(f MultiFunctionExpr) interface{} {
	args := make([]string, 0)
	for _, arg := range f.Args {
		r := "(" + arg.Accept(v).(string) + ")"
		args = append(args, r)
	}
	return f.Name + strings.Join(args, "")
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
	if e.Op == "AND" || e.Op == "OR" {
		return fmt.Sprintf("(%v %v %v)", lhs, e.Op, rhs)
	} else if strings.Contains(e.Op, "LIKE") || e.Op == "IS" || e.Op == "IN" || e.Op == "REGEXP" {
		return fmt.Sprintf("%v %v %v", lhs, e.Op, rhs)
	} else {
		return fmt.Sprintf("%v%v%v", lhs, e.Op, rhs)
	}
}

func (v *renderer) VisitOrderByExpr(e OrderByExpr) interface{} {
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

func (v *renderer) VisitDistinctExpr(e DistinctExpr) interface{} {
	return fmt.Sprintf("DISTINCT %s", e.Expr.Accept(v).(string))
}

func (v *renderer) VisitTableRef(e TableRef) interface{} {
	return e.Name
}

func (v *renderer) VisitAliasedExpr(e AliasedExpr) interface{} {
	return fmt.Sprintf("%s AS %s", e.Expr.Accept(v).(string), strconv.Quote(e.Alias))
}

func (v *renderer) VisitSelectCommand(c SelectCommand) interface{} {
	// THIS SHOULD PRODUCE QUERY IN  BRACES
	var sb strings.Builder

	const cteNamePrefix = "cte"
	cteName := func(cteIdx int) string {
		return fmt.Sprintf("%s_%d", cteNamePrefix, cteIdx+1)
	}
	cteFieldAlias := func(cteIdx, fieldIdx int) string {
		return fmt.Sprintf("%s_%d_%d", cteNamePrefix, cteIdx+1, fieldIdx+1)
	}
	cteCountAlias := func(ctxIdx int) string {
		return fmt.Sprintf("%s_%d_cnt", cteNamePrefix, ctxIdx+1)
	}
	if len(c.CTEs) > 0 {
		CTEsStrings := make([]string, 0, len(c.CTEs))
		for i, cte := range c.CTEs {
			for j, col := range cte.Columns {
				if _, alreadyAliased := cte.Columns[j].(AliasedExpr); !alreadyAliased {
					cte.Columns[j] = AliasedExpr{Expr: col, Alias: cteFieldAlias(i, j)}
				} else {
					logger.Warn().Msgf("Subquery column already aliased: %s, %+v", AsString(col), col)
				}
			}
			str := fmt.Sprintf("%s AS (%s)", cteName(i), AsString(cte))
			CTEsStrings = append(CTEsStrings, str)
		}
		sb.WriteString(fmt.Sprintf("WITH %s ", strings.Join(CTEsStrings, ", ")))
	}

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
	if c.FromClause != nil { // here we have to handle nested
		if nestedCmd, isNested := c.FromClause.(SelectCommand); isNested {
			sb.WriteString(fmt.Sprintf("(%s)", AsString(nestedCmd)))
		} else if nestedCmdPtr, isNested := c.FromClause.(*SelectCommand); isNested {
			sb.WriteString(fmt.Sprintf("(%s)", AsString(nestedCmdPtr)))
		} else {
			sb.WriteString(AsString(c.FromClause))
		}

		if len(c.CTEs) > 0 {
			for cteIdx, cte := range c.CTEs {
				sb.WriteString(" INNER JOIN ")
				sb.WriteString(strconv.Quote(cteName(cteIdx)))
				sb.WriteString(" ON ")
				for colIdx := range len(cte.Columns) - 1 { // at least so far, last one is always count() or some other metric aggr, on which we don't need to GROUP BY
					sb.WriteString(fmt.Sprintf("%s = %s", AsString(c.Columns[colIdx]), strconv.Quote(cteFieldAlias(cteIdx, colIdx))))
					if colIdx < len(cte.Columns)-2 {
						sb.WriteString(" AND ")
					}
				}
			}
		}
	}
	if c.WhereClause != nil {
		sb.WriteString(" WHERE ")
		sb.WriteString(AsString(c.WhereClause))
	}
	if c.SampleLimit > 0 {
		sb.WriteString(fmt.Sprintf(" LIMIT %d)", c.SampleLimit))
	}

	groupBy := make([]string, 0, len(c.GroupBy))
	for _, col := range c.GroupBy {
		groupBy = append(groupBy, AsString(col))
	}
	if len(groupBy) > 0 {
		sb.WriteString(" GROUP BY ")
		fullGroupBy := groupBy
		for i := range c.CTEs {
			fullGroupBy = append(fullGroupBy, cteCountAlias(i))
		}
		sb.WriteString(strings.Join(fullGroupBy, ", "))
	}

	orderBy := make([]string, 0, len(c.OrderBy))
	orderByReplaced, orderByToReplace := 0, len(c.CTEs)
	for _, col := range c.OrderBy {
		if col.Exchange && orderByReplaced < orderByToReplace {
			orderBy = append(orderBy, fmt.Sprintf("%s DESC", cteCountAlias(orderByReplaced)))
			orderByReplaced++
		} else {
			orderBy = append(orderBy, AsString(col))
		}
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
	partitionBy := make([]string, 0)
	for _, col := range f.PartitionBy {
		partitionBy = append(partitionBy, AsString(col))
	}

	var sb strings.Builder
	stmtWithoutOrderBy := fmt.Sprintf("%s(%s) OVER (PARTITION BY %s", f.Name, strings.Join(args, ", "), strings.Join(partitionBy, ", "))
	sb.WriteString(stmtWithoutOrderBy)

	if len(f.OrderBy.Exprs) != 0 {
		sb.WriteString(" ORDER BY ")
		sb.WriteString(AsString(f.OrderBy))
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
