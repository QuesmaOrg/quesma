package model

import (
	"mitmproxy/quesma/logger"
)

type aliasResolver struct{}

// ApplyAliases takes all the column references in a given expression and changes column names to aliases specified in the configuration
func ApplyAliases(expr Expr) Expr {
	return expr.Accept(&aliasResolver{}).(Expr)
}

func (a *aliasResolver) VisitPrefixExpr(e PrefixExpr) interface{} {
	var exprs []Expr
	for _, expr := range e.Args {
		exprs = append(exprs, expr.Accept(a).(Expr))
	}
	return NewPrefixExpr(e.Op, exprs)
}

func (a *aliasResolver) VisitNestedProperty(e NestedProperty) interface{} {
	return NewNestedProperty(e.ColumnRef.Accept(a).(ColumnRef), e.PropertyName)
}

func (a *aliasResolver) VisitArrayAccess(e ArrayAccess) interface{} {
	return NewArrayAccess(e.ColumnRef.Accept(a).(ColumnRef), e.Index.Accept(a).(Expr))
}

func (a *aliasResolver) VisitColumnRef(e ColumnRef) interface{} {
	//return NewColumnRef(e.ColumnName + "_PRZEMYSLAW")
	logger.Info().Msgf("PRZEMYSLAW_WOULD_SWAP: %s", e.ColumnName)
	return NewColumnRef(e.ColumnName)
}

func (a *aliasResolver) VisitFunction(e FunctionExpr) interface{} {
	var exprs []Expr
	for _, expr := range e.Args {
		exprs = append(exprs, expr.Accept(a).(Expr))
	}
	return NewFunction(e.Name, exprs...)
}

func (a *aliasResolver) VisitLiteral(l LiteralExpr) interface{} {
	return NewLiteral(l.Value)
}

func (a *aliasResolver) VisitMultiFunction(f MultiFunctionExpr) interface{} {
	var exprs []Expr
	for _, expr := range f.Args {
		exprs = append(exprs, expr.Accept(a).(Expr))
	}
	return MultiFunctionExpr{Name: f.Name, Args: exprs}
}

func (a *aliasResolver) VisitInfix(e InfixExpr) interface{} {
	return NewInfixExpr(e.Left.Accept(a).(Expr), e.Op, e.Right.Accept(a).(Expr))
}

func (a *aliasResolver) VisitString(e StringExpr) interface{} {
	return NewStringExpr(e.Value)
}

func (a *aliasResolver) VisitOrderByExpr(e OrderByExpr) interface{} {
	var exprs []Expr
	for _, expr := range e.Exprs {
		exprs = append(exprs, expr.Accept(a).(Expr))
	}
	return NewOrderByExpr(exprs, e.Direction)
}

func (a *aliasResolver) VisitDistinctExpr(e DistinctExpr) interface{} {
	return NewDistinctExpr(e.Expr.Accept(a).(Expr))
}

func (a *aliasResolver) VisitTableRef(e TableRef) interface{} {
	return NewTableRef(e.Name)
}

func (a *aliasResolver) VisitAliasedExpr(e AliasedExpr) interface{} {
	return NewAliasedExpr(e.Expr.Accept(a).(Expr), e.Alias)
}
func (a *aliasResolver) VisitSelectCommand(s SelectCommand) interface{} {
	var columns, groupBy []Expr
	var orderBy []OrderByExpr
	for _, expr := range s.Columns {
		columns = append(columns, expr.Accept(a).(Expr))
	}
	for _, expr := range s.GroupBy {
		groupBy = append(groupBy, expr.Accept(a).(Expr))
	}
	for _, expr := range s.OrderBy {
		orderBy = append(orderBy, expr.Accept(a).(OrderByExpr))
	}
	return *NewSelectCommand(columns, groupBy, orderBy, s.FromClause.Accept(a).(Expr), s.WhereClause.Accept(a).(Expr), s.Limit, s.SampleLimit, s.IsDistinct)

}

func (a *aliasResolver) VisitWindowFunction(f WindowFunction) interface{} {
	var args, partitionBy []Expr
	for _, expr := range f.Args {
		args = append(args, expr.Accept(a).(Expr))
	}
	for _, expr := range f.PartitionBy {
		partitionBy = append(partitionBy, expr.Accept(a).(Expr))
	}
	return NewWindowFunction(f.Name, args, partitionBy, f.OrderBy.Accept(a).(OrderByExpr))
}
