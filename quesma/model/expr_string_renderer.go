package model

import (
	"fmt"
	"strconv"
	"strings"
)

type renderer struct{}

// AsString renders the given expression to string which can be used to build SQL query
func AsString(expr Expr) string {
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
	} else if strings.Contains(e.Op, "LIKE") || e.Op == "IS" || e.Op == "IN" {
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
	if c.SampleLimit > 0 && !c.DisableHack {
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
			sb.WriteString("(")
			sb.WriteString(AsString(nestedCmd))
			sb.WriteString(")")
		} else if nestedCmd, ok := c.FromClause.(*SelectCommand); ok {
			sb.WriteString("(")
			sb.WriteString(AsString(nestedCmd))
			sb.WriteString(")")
		} else {
			sb.WriteString(AsString(c.FromClause))
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
		sb.WriteString(strings.Join(groupBy, ", "))
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
		sb.WriteString(fmt.Sprintf(" LIMIT %d", c.Limit))
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
