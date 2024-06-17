package model

import (
	"fmt"
	"strings"
)

type ExprPlaceholderRewriter struct {
	count  int
	values map[string]interface{}
}

func NewExprPlaceholderRewrite() *ExprPlaceholderRewriter {
	return &ExprPlaceholderRewriter{
		values: make(map[string]interface{}),
	}
}

func (v *ExprPlaceholderRewriter) Rewrite(expr SelectCommand) (SelectCommand, map[string]interface{}) {
	newExpr := expr.Accept(v).(SelectCommand)
	return newExpr, v.values
}

func (v *ExprPlaceholderRewriter) visitChildren(args []Expr) []Expr {
	var newArgs []Expr
	for _, arg := range args {
		if arg != nil {
			newArgs = append(newArgs, arg.Accept(v).(Expr))
		}
	}
	return newArgs
}

func (v *ExprPlaceholderRewriter) VisitLiteral(e LiteralExpr) interface{} {

	// this is workaround for NOT NULL,
	// maybe "NOT NULL" should be passed as sth else
	if e.Value == "NOT NULL" {
		return NewStringExpr("NOT NULL")
	}

	v.count++

	var value string // clickhouse doesn't support any type in placeholders, it must be a string
	var typeName string
	switch val := e.Value.(type) {
	case int, int64, int32:
		typeName = "Int64"
		value = fmt.Sprintf("%v", val)
	case string:
		typeName = "String"
		value = strings.Trim(val, "'")
	case bool:
		typeName = "Boolean"
		value = fmt.Sprintf("%v", val)
	default:
		typeName = "String" // TODO: add more types here
		val = fmt.Sprintf("%v", e.Value)
	}

	parameterName := fmt.Sprintf("P_%d", v.count)
	placeholder := fmt.Sprintf("{%s:%s}", parameterName, typeName)
	v.values[parameterName] = value

	return NewStringExpr(placeholder)
}

// the rest is just visiting the tree

func (v *ExprPlaceholderRewriter) VisitInfix(e InfixExpr) interface{} {
	lhs := e.Left.Accept(v)
	rhs := e.Right.Accept(v)

	return NewInfixExpr(lhs.(Expr), e.Op, rhs.(Expr))
}

func (v *ExprPlaceholderRewriter) VisitPrefixExpr(e PrefixExpr) interface{} {
	return NewPrefixExpr(e.Op, v.visitChildren(e.Args))
}

func (v *ExprPlaceholderRewriter) VisitFunction(e FunctionExpr) interface{} {
	return NewFunction(e.Name, v.visitChildren(e.Args)...)
}

func (v *ExprPlaceholderRewriter) VisitColumnRef(e ColumnRef) interface{} {
	return e
}

func (v *ExprPlaceholderRewriter) VisitNestedProperty(e NestedProperty) interface{} {
	columnRef := e.ColumnRef.Accept(v).(ColumnRef)
	property := e.PropertyName.Accept(v).(LiteralExpr)
	return NewNestedProperty(columnRef, property)
}

func (v *ExprPlaceholderRewriter) VisitArrayAccess(e ArrayAccess) interface{} {
	columnRef := e.ColumnRef.Accept(v).(ColumnRef)
	index := e.Index.Accept(v).(Expr)
	return NewArrayAccess(columnRef, index)
}

func (v *ExprPlaceholderRewriter) VisitMultiFunction(e MultiFunctionExpr) interface{} {
	return MultiFunctionExpr{Name: e.Name, Args: v.visitChildren(e.Args)}
}

func (v *ExprPlaceholderRewriter) VisitString(e StringExpr) interface{} { return e }

func (v *ExprPlaceholderRewriter) VisitTableRef(e TableRef) interface{} {
	return e
}

func (v *ExprPlaceholderRewriter) VisitOrderByExpr(e OrderByExpr) interface{} {
	return OrderByExpr{Exprs: v.visitChildren(e.Exprs), Direction: e.Direction}
}

func (v *ExprPlaceholderRewriter) VisitDistinctExpr(e DistinctExpr) interface{} {
	return DistinctExpr{Expr: e.Accept(v).(Expr)}
}

func (v *ExprPlaceholderRewriter) VisitAliasedExpr(e AliasedExpr) interface{} {
	return NewAliasedExpr(e.Expr.Accept(v).(Expr), e.Alias)
}

func (v *ExprPlaceholderRewriter) VisitWindowFunction(f WindowFunction) interface{} {
	return WindowFunction{
		Name:        f.Name,
		Args:        v.visitChildren(f.Args),
		PartitionBy: v.visitChildren(f.PartitionBy),
		OrderBy:     f.OrderBy.Accept(v).(OrderByExpr),
	}
}

func (v *ExprPlaceholderRewriter) VisitSelectCommand(query SelectCommand) interface{} {

	if query.WhereClause != nil {
		query.WhereClause = query.WhereClause.Accept(v).(Expr)
	}

	for i, group := range query.GroupBy {
		query.GroupBy[i] = group.Accept(v).(Expr)
	}

	for i, column := range query.Columns {
		query.Columns[i] = column.Accept(v).(Expr)
	}

	for i, order := range query.OrderBy {
		query.OrderBy[i] = order.Accept(v).(OrderByExpr)
	}

	return query
}
