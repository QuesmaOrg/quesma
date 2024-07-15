// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package model

// BaseExprOverrides is a struct that holds function pointers to override the default behavior of the BaseExprVisitor
// We keep them in separate struct to avoid name clashes with the methods
type BaseExprOverrides struct {
	VisitFunction       func(b *BaseExprVisitor, e FunctionExpr) interface{}
	VisitMultiFunction  func(b *BaseExprVisitor, e MultiFunctionExpr) interface{}
	VisitLiteral        func(b *BaseExprVisitor, l LiteralExpr) interface{}
	VisitString         func(b *BaseExprVisitor, e StringExpr) interface{}
	VisitInfix          func(b *BaseExprVisitor, e InfixExpr) interface{}
	VisitColumnRef      func(b *BaseExprVisitor, e ColumnRef) interface{}
	VisitPrefixExpr     func(b *BaseExprVisitor, e PrefixExpr) interface{}
	VisitNestedProperty func(b *BaseExprVisitor, e NestedProperty) interface{}
	VisitArrayAccess    func(b *BaseExprVisitor, e ArrayAccess) interface{}
	VisitOrderByExpr    func(b *BaseExprVisitor, e OrderByExpr) interface{}
	VisitDistinctExpr   func(b *BaseExprVisitor, e DistinctExpr) interface{}
	VisitTableRef       func(b *BaseExprVisitor, e TableRef) interface{}
	VisitAliasedExpr    func(b *BaseExprVisitor, e AliasedExpr) interface{}
	VisitSelectCommand  func(b *BaseExprVisitor, e SelectCommand) interface{}
	VisitWindowFunction func(b *BaseExprVisitor, f WindowFunction) interface{}
	VisitParenExpr      func(b *BaseExprVisitor, e ParenExpr) interface{}
	VisitLambdaExpr     func(b *BaseExprVisitor, e LambdaExpr) interface{}
}

type BaseExprVisitor struct {
	Overrides BaseExprOverrides
}

func NewBaseVisitor() *BaseExprVisitor {
	return &BaseExprVisitor{Overrides: BaseExprOverrides{}}
}

func (v *BaseExprVisitor) visitChildren(args []Expr) []Expr {
	var newArgs []Expr
	for _, arg := range args {
		if arg != nil {
			newArgs = append(newArgs, arg.Accept(v).(Expr))
		}
	}
	return newArgs
}

func (v *BaseExprVisitor) VisitLiteral(e LiteralExpr) interface{} {
	if v.Overrides.VisitLiteral != nil {
		return v.Overrides.VisitLiteral(v, e)
	}

	return NewLiteral(e.Value)
}
func (v *BaseExprVisitor) VisitInfix(e InfixExpr) interface{} {
	if v.Overrides.VisitInfix != nil {
		return v.Overrides.VisitInfix(v, e)
	}
	lhs := e.Left.Accept(v)
	rhs := e.Right.Accept(v)
	return NewInfixExpr(lhs.(Expr), e.Op, rhs.(Expr))
}

func (v *BaseExprVisitor) VisitPrefixExpr(e PrefixExpr) interface{} {
	if v.Overrides.VisitPrefixExpr != nil {
		return v.Overrides.VisitPrefixExpr(v, e)
	}
	return NewPrefixExpr(e.Op, v.visitChildren(e.Args))
}

func (v *BaseExprVisitor) VisitFunction(e FunctionExpr) interface{} {
	if v.Overrides.VisitFunction != nil {
		return v.Overrides.VisitFunction(v, e)
	}
	return NewFunction(e.Name, v.visitChildren(e.Args)...)
}

func (v *BaseExprVisitor) VisitColumnRef(e ColumnRef) interface{} {
	if v.Overrides.VisitColumnRef != nil {
		return v.Overrides.VisitColumnRef(v, e)
	}
	return NewColumnRef(e.ColumnName)
}

func (v *BaseExprVisitor) VisitNestedProperty(e NestedProperty) interface{} {
	if v.Overrides.VisitNestedProperty != nil {
		return v.Overrides.VisitNestedProperty(v, e)
	}
	ColumnRef := e.ColumnRef.Accept(v).(ColumnRef)
	Property := e.PropertyName.Accept(v).(LiteralExpr)
	return NewNestedProperty(ColumnRef, Property)
}

func (v *BaseExprVisitor) VisitArrayAccess(e ArrayAccess) interface{} {
	if v.Overrides.VisitArrayAccess != nil {
		return v.Overrides.VisitArrayAccess(v, e)
	}
	columnRef := e.ColumnRef.Accept(v).(ColumnRef)
	index := e.Index.Accept(v).(Expr)
	return NewArrayAccess(columnRef, index)
}

func (v *BaseExprVisitor) VisitMultiFunction(e MultiFunctionExpr) interface{} {
	if v.Overrides.VisitMultiFunction != nil {
		return v.Overrides.VisitMultiFunction(v, e)
	}
	return MultiFunctionExpr{Name: e.Name, Args: v.visitChildren(e.Args)}
}

func (v *BaseExprVisitor) VisitString(e StringExpr) interface{} {
	if v.Overrides.VisitString != nil {
		return v.Overrides.VisitString(v, e)
	}
	return e
}

func (v *BaseExprVisitor) VisitTableRef(e TableRef) interface{} {
	if v.Overrides.VisitTableRef != nil {
		return v.Overrides.VisitTableRef(v, e)
	}
	return e
}

func (v *BaseExprVisitor) VisitOrderByExpr(e OrderByExpr) interface{} {
	if v.Overrides.VisitOrderByExpr != nil {
		return v.Overrides.VisitOrderByExpr(v, e)
	}
	return OrderByExpr{Exprs: v.visitChildren(e.Exprs), Direction: e.Direction}
}

func (v *BaseExprVisitor) VisitDistinctExpr(e DistinctExpr) interface{} {
	if v.Overrides.VisitDistinctExpr != nil {
		return v.Overrides.VisitDistinctExpr(v, e)
	}
	return DistinctExpr{Expr: e.Expr.Accept(v).(Expr)}
}

func (v *BaseExprVisitor) VisitAliasedExpr(e AliasedExpr) interface{} {
	if v.Overrides.VisitAliasedExpr != nil {
		return v.Overrides.VisitAliasedExpr(v, e)
	}
	return NewAliasedExpr(e.Expr.Accept(v).(Expr), e.Alias)
}

func (v *BaseExprVisitor) VisitWindowFunction(f WindowFunction) interface{} {
	if v.Overrides.VisitWindowFunction != nil {
		return v.Overrides.VisitWindowFunction(v, f)
	}
	return WindowFunction{
		Name:        f.Name,
		Args:        v.visitChildren(f.Args),
		PartitionBy: v.visitChildren(f.PartitionBy),
		OrderBy:     f.OrderBy.Accept(v).(OrderByExpr),
	}
}

func (v *BaseExprVisitor) VisitSelectCommand(query SelectCommand) interface{} {
	if v.Overrides.VisitSelectCommand != nil {
		return v.Overrides.VisitSelectCommand(v, query)
	}
	var columns, groupBy []Expr
	var orderBy []OrderByExpr
	from := query.FromClause
	where := query.WhereClause

	for _, expr := range query.Columns {
		columns = append(columns, expr.Accept(v).(Expr))
	}
	for _, expr := range query.GroupBy {
		groupBy = append(groupBy, expr.Accept(v).(Expr))
	}
	for _, expr := range query.OrderBy {
		orderBy = append(orderBy, expr.Accept(v).(OrderByExpr))
	}
	if query.FromClause != nil {
		from = query.FromClause.Accept(v).(Expr)
	}
	if query.WhereClause != nil {
		where = query.WhereClause.Accept(v).(Expr)
	}
	return *NewSelectCommand(columns, groupBy, orderBy, from, where, query.Limit, query.SampleLimit, query.IsDistinct)
}

func (v *BaseExprVisitor) VisitParenExpr(p ParenExpr) interface{} {
	if v.Overrides.VisitParenExpr != nil {
		return v.Overrides.VisitParenExpr(v, p)
	}
	var exprs []Expr
	for _, expr := range p.Exprs {
		exprs = append(exprs, expr.Accept(v).(Expr))
	}
	return NewParenExpr(exprs...)
}

func (v *BaseExprVisitor) VisitLambdaExpr(e LambdaExpr) interface{} {
	if v.Overrides.VisitLambdaExpr != nil {
		return v.Overrides.VisitLambdaExpr(v, e)
	}
	return NewLambdaExpr(e.Args, e.Body.Accept(v).(Expr))
}
