// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package model

type BaseExprVisitor struct {
	OverrideVisitFunction       func(b *BaseExprVisitor, e FunctionExpr) interface{}
	OverrideVisitLiteral        func(b *BaseExprVisitor, l LiteralExpr) interface{}
	OverrideVisitInfix          func(b *BaseExprVisitor, e InfixExpr) interface{}
	OverrideVisitColumnRef      func(b *BaseExprVisitor, e ColumnRef) interface{}
	OverrideVisitPrefixExpr     func(b *BaseExprVisitor, e PrefixExpr) interface{}
	OverrideVisitNestedProperty func(b *BaseExprVisitor, e NestedProperty) interface{}
	OverrideVisitArrayAccess    func(b *BaseExprVisitor, e ArrayAccess) interface{}
	OverrideVisitOrderByExpr    func(b *BaseExprVisitor, e OrderByExpr) interface{}
	OverrideVisitDistinctExpr   func(b *BaseExprVisitor, e DistinctExpr) interface{}
	OverrideVisitTableRef       func(b *BaseExprVisitor, e TableRef) interface{}
	OverrideVisitAliasedExpr    func(b *BaseExprVisitor, e AliasedExpr) interface{}
	OverrideVisitSelectCommand  func(b *BaseExprVisitor, e SelectCommand) interface{}
	OverrideVisitWindowFunction func(b *BaseExprVisitor, f WindowFunction) interface{}
	OverrideVisitParenExpr      func(b *BaseExprVisitor, e ParenExpr) interface{}
	OverrideVisitLambdaExpr     func(b *BaseExprVisitor, e LambdaExpr) interface{}
	OverrideVisitJoinExpr       func(b *BaseExprVisitor, e JoinExpr) interface{}
	OverrideVisitCTE            func(b *BaseExprVisitor, e CTE) interface{}
}

func NewBaseVisitor() *BaseExprVisitor {
	return &BaseExprVisitor{}
}

func (v *BaseExprVisitor) VisitChildren(args []Expr) []Expr {
	var newArgs []Expr
	for _, arg := range args {
		if arg != nil {
			newArgs = append(newArgs, arg.Accept(v).(Expr))
		}
	}
	return newArgs
}

func (v *BaseExprVisitor) VisitLiteral(e LiteralExpr) interface{} {
	if v.OverrideVisitLiteral != nil {
		return v.OverrideVisitLiteral(v, e)
	}

	return LiteralExpr{e.Value, e.LiteralAlreadyEscaped}
}
func (v *BaseExprVisitor) VisitInfix(e InfixExpr) interface{} {
	if v.OverrideVisitInfix != nil {
		return v.OverrideVisitInfix(v, e)
	}
	lhs := e.Left.Accept(v)
	rhs := e.Right.Accept(v)
	return NewInfixExpr(lhs.(Expr), e.Op, rhs.(Expr))
}

func (v *BaseExprVisitor) VisitPrefixExpr(e PrefixExpr) interface{} {
	if v.OverrideVisitPrefixExpr != nil {
		return v.OverrideVisitPrefixExpr(v, e)
	}
	return NewPrefixExpr(e.Op, v.VisitChildren(e.Args))
}

func (v *BaseExprVisitor) VisitFunction(e FunctionExpr) interface{} {
	if v.OverrideVisitFunction != nil {
		return v.OverrideVisitFunction(v, e)
	}
	return NewFunction(e.Name, v.VisitChildren(e.Args)...)
}

func (v *BaseExprVisitor) VisitColumnRef(e ColumnRef) interface{} {
	if v.OverrideVisitColumnRef != nil {
		return v.OverrideVisitColumnRef(v, e)
	}
	return NewColumnRef(e.ColumnName)
}

func (v *BaseExprVisitor) VisitNestedProperty(e NestedProperty) interface{} {
	if v.OverrideVisitNestedProperty != nil {
		return v.OverrideVisitNestedProperty(v, e)
	}
	expr := e.ObjectExpr.Accept(v).(Expr)
	Property := e.PropertyName.Accept(v).(LiteralExpr)
	return NewNestedProperty(expr, Property)
}

func (v *BaseExprVisitor) VisitArrayAccess(e ArrayAccess) interface{} {
	if v.OverrideVisitArrayAccess != nil {
		return v.OverrideVisitArrayAccess(v, e)
	}
	columnRef := e.ColumnRef.Accept(v).(ColumnRef)
	index := e.Index.Accept(v).(Expr)
	return NewArrayAccess(columnRef, index)
}

func (v *BaseExprVisitor) VisitTableRef(e TableRef) interface{} {
	if v.OverrideVisitTableRef != nil {
		return v.OverrideVisitTableRef(v, e)
	}
	return e
}

func (v *BaseExprVisitor) VisitOrderByExpr(e OrderByExpr) interface{} {
	if v.OverrideVisitOrderByExpr != nil {
		return v.OverrideVisitOrderByExpr(v, e)
	}
	return OrderByExpr{Expr: e.Expr.Accept(v).(Expr), Direction: e.Direction}
}

func (v *BaseExprVisitor) VisitDistinctExpr(e DistinctExpr) interface{} {
	if v.OverrideVisitDistinctExpr != nil {
		return v.OverrideVisitDistinctExpr(v, e)
	}
	return DistinctExpr{Expr: e.Expr.Accept(v).(Expr)}
}

func (v *BaseExprVisitor) VisitAliasedExpr(e AliasedExpr) interface{} {
	if v.OverrideVisitAliasedExpr != nil {
		return v.OverrideVisitAliasedExpr(v, e)
	}
	return NewAliasedExpr(e.Expr.Accept(v).(Expr), e.Alias)
}

func (v *BaseExprVisitor) VisitWindowFunction(f WindowFunction) interface{} {
	if v.OverrideVisitWindowFunction != nil {
		return v.OverrideVisitWindowFunction(v, f)
	}
	var orderBy []OrderByExpr
	for _, expr := range f.OrderBy {
		orderBy = append(orderBy, expr.Accept(v).(OrderByExpr))
	}
	return WindowFunction{
		Name:        f.Name,
		Args:        v.VisitChildren(f.Args),
		PartitionBy: v.VisitChildren(f.PartitionBy),
		OrderBy:     orderBy,
	}
}

func (v *BaseExprVisitor) VisitSelectCommand(query SelectCommand) interface{} {
	if v.OverrideVisitSelectCommand != nil {
		return v.OverrideVisitSelectCommand(v, query)
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

	var namedCTEs []*CTE
	if query.NamedCTEs != nil {
		for _, cte := range query.NamedCTEs {
			namedCTEs = append(namedCTEs, cte.Accept(v).(*CTE))
		}
	}

	var limitBy []Expr
	if query.LimitBy != nil {
		for _, expr := range query.LimitBy {
			limitBy = append(limitBy, expr.Accept(v).(Expr))
		}
	}

	return NewSelectCommand(columns, groupBy, orderBy, from, where, limitBy, query.Limit, query.SampleLimit, query.IsDistinct, namedCTEs)
}

func (v *BaseExprVisitor) VisitParenExpr(p ParenExpr) interface{} {
	if v.OverrideVisitParenExpr != nil {
		return v.OverrideVisitParenExpr(v, p)
	}
	var exprs []Expr
	for _, expr := range p.Exprs {
		exprs = append(exprs, expr.Accept(v).(Expr))
	}
	return NewParenExpr(exprs...)
}

func (v *BaseExprVisitor) VisitLambdaExpr(e LambdaExpr) interface{} {
	if v.OverrideVisitLambdaExpr != nil {
		return v.OverrideVisitLambdaExpr(v, e)
	}
	return NewLambdaExpr(e.Args, e.Body.Accept(v).(Expr))
}

func (v *BaseExprVisitor) VisitJoinExpr(j JoinExpr) interface{} {
	if v.OverrideVisitJoinExpr != nil {
		return v.OverrideVisitJoinExpr(v, j)
	}
	return NewJoinExpr(j.Lhs.Accept(v).(Expr), j.Rhs.Accept(v).(Expr), j.JoinType, j.On.Accept(v).(Expr))
}

func (v *BaseExprVisitor) VisitCTE(e CTE) interface{} {
	if v.OverrideVisitCTE != nil {
		return v.OverrideVisitCTE(v, e)
	}
	return NewCTE(e.Name, e.SelectCommand.Accept(v).(*SelectCommand))
}
