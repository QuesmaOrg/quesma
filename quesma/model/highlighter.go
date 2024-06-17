package model

import (
	"mitmproxy/quesma/logger"
	"strings"
)

type highlighter struct {
	// TokensToHighlight represents a set of tokens that should be highlighted in the query.
	TokensToHighlight map[string]struct{}
}

func NewHighlighter() *highlighter {
	return &highlighter{
		TokensToHighlight: make(map[string]struct{}),
	}
}

func (v *highlighter) VisitColumnRef(e ColumnRef) interface{} {
	return e
}

func (v *highlighter) VisitPrefixExpr(e PrefixExpr) interface{} {
	var exprs []Expr
	for _, expr := range e.Args {
		exprs = append(exprs, expr.Accept(v).(Expr))
	}
	return NewPrefixExpr(e.Op, exprs)
}

func (v *highlighter) VisitNestedProperty(e NestedProperty) interface{} {
	return NewNestedProperty(e.ColumnRef.Accept(v).(ColumnRef), e.PropertyName)
}

func (v *highlighter) VisitArrayAccess(e ArrayAccess) interface{} {
	return NewArrayAccess(e.ColumnRef.Accept(v).(ColumnRef), e.Index.Accept(v).(Expr))
}

func (v *highlighter) VisitFunction(e FunctionExpr) interface{} {
	var exprs []Expr
	for _, expr := range e.Args {
		exprs = append(exprs, expr.Accept(v).(Expr))
	}
	return NewFunction(e.Name, exprs...)
}

func (v *highlighter) VisitLiteral(l LiteralExpr) interface{} {
	return l
}

func (v *highlighter) VisitString(e StringExpr) interface{} {
	return e
}

func (v *highlighter) VisitMultiFunction(f MultiFunctionExpr) interface{} {
	var exprs []Expr
	for _, expr := range f.Args {
		exprs = append(exprs, expr.Accept(v).(Expr))
	}
	return MultiFunctionExpr{Name: f.Name, Args: exprs}
}

func (v *highlighter) VisitInfix(e InfixExpr) interface{} {
	switch e.Op {
	case "iLIKE", "LIKE": //, "IN", "=": TODO I need to like triple check if it doesn't produce any false positives
		if literal, isLiteral := e.Right.(LiteralExpr); isLiteral {
			switch literalAsString := literal.Value.(type) {
			case string:
				literalAsString = strings.TrimPrefix(literalAsString, "'%")
				literalAsString = strings.TrimSuffix(literalAsString, "%'")
				v.TokensToHighlight[strings.ToLower(literalAsString)] = struct{}{}
			default:
				logger.Info().Msgf("Value is of an unexpected type: %T\n", literalAsString)
			}
		}
	}
	return NewInfixExpr(e.Left.Accept(v).(Expr), e.Op, e.Right.Accept(v).(Expr))
}

func (v *highlighter) VisitOrderByExpr(e OrderByExpr) interface{} {
	var exprs []Expr
	for _, expr := range e.Exprs {
		exprs = append(exprs, expr.Accept(v).(Expr))
	}
	return NewOrderByExpr(exprs, e.Direction)
}

func (v *highlighter) VisitDistinctExpr(e DistinctExpr) interface{} {
	return NewDistinctExpr(e.Expr.Accept(v).(Expr))
}

func (v *highlighter) VisitTableRef(e TableRef) interface{} {
	return e
}

func (v *highlighter) VisitAliasedExpr(e AliasedExpr) interface{} {
	return NewAliasedExpr(e.Expr.Accept(v).(Expr), e.Alias)
}

func (v *highlighter) VisitSelectCommand(c SelectCommand) interface{} {
	var columns, groupBy []Expr
	var orderBy []OrderByExpr
	for _, expr := range c.Columns {
		columns = append(columns, expr.Accept(v).(Expr))
	}
	for _, expr := range c.GroupBy {
		groupBy = append(groupBy, expr.Accept(v).(Expr))
	}
	for _, expr := range c.OrderBy {
		orderBy = append(orderBy, expr.Accept(v).(OrderByExpr))
	}
	return *NewSelectCommand(columns, groupBy, orderBy, c.FromClause.Accept(v).(Expr), c.WhereClause.Accept(v).(Expr), c.Limit, c.SampleLimit, c.IsDistinct)
}

func (v *highlighter) VisitWindowFunction(f WindowFunction) interface{} {
	var args, partitionBy []Expr
	for _, expr := range f.Args {
		args = append(args, expr.Accept(v).(Expr))
	}
	for _, expr := range f.PartitionBy {
		partitionBy = append(partitionBy, expr.Accept(v).(Expr))
	}
	return NewWindowFunction(f.Name, args, partitionBy, f.OrderBy.Accept(v).(OrderByExpr))
}
