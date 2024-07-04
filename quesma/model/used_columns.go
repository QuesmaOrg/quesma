// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package model

// TODO OKAY THIS NEEDS TO BE FIXED FOR THE NEW WHERE STATEMENT
type usedColumns struct{}

func GetUsedColumns(expr Expr) []ColumnRef {
	return expr.Accept(&usedColumns{}).([]ColumnRef)
}

func (v *usedColumns) VisitPrefixExpr(e PrefixExpr) interface{} {
	res := make([]ColumnRef, 0)
	for _, arg := range e.Args {
		v1 := arg.Accept(v)
		if v2, ok := v1.([]ColumnRef); ok {
			res = append(res, v2...)
		}
	}
	return res
}

func (v *usedColumns) VisitNestedProperty(e NestedProperty) interface{} {
	return nil
}

func (v *usedColumns) VisitArrayAccess(e ArrayAccess) interface{} {
	return nil
}

func (v *usedColumns) VisitColumnRef(e ColumnRef) interface{} {
	return []ColumnRef{e}
}

func (v *usedColumns) VisitFunction(e FunctionExpr) interface{} {
	res := make([]ColumnRef, 0)
	for _, arg := range e.Args {
		v1 := arg.Accept(v)
		if v2, ok := v1.([]ColumnRef); ok {
			res = append(res, v2...)
		}
	}
	return res
}

func (v *usedColumns) VisitLiteral(l LiteralExpr) interface{} {
	return make([]ColumnRef, 0)
}

func (v *usedColumns) VisitMultiFunction(f MultiFunctionExpr) interface{} {
	res := make([]ColumnRef, 0)
	for _, arg := range f.Args {
		v1 := arg.Accept(v)
		if v2, ok := v1.([]ColumnRef); ok {
			res = append(res, v2...)
		}
	}
	return res
}

func (v *usedColumns) VisitInfix(e InfixExpr) interface{} {
	res := make([]ColumnRef, 0)
	v1 := e.Left.Accept(v)
	if v2, ok := v1.([]ColumnRef); ok {
		res = append(res, v2...)
	}
	v3 := e.Right.Accept(v)
	if v4, ok := v3.([]ColumnRef); ok {
		res = append(res, v4...)
	}
	return res
}

func (v *usedColumns) VisitString(e StringExpr) interface{} {
	return make([]ColumnRef, 0)
}

func (v *usedColumns) VisitOrderByExpr(e OrderByExpr) interface{} {
	res := make([]ColumnRef, 0)
	for _, expr := range e.Exprs {
		cur := expr.Accept(v)
		res = append(res, cur.([]ColumnRef)...)
	}
	return res
}

func (v *usedColumns) VisitDistinctExpr(e DistinctExpr) interface{} {
	return e.Expr.Accept(v)
}

func (v *usedColumns) VisitTableRef(e TableRef) interface{} {
	return make([]ColumnRef, 0)
}

func (v *usedColumns) VisitAliasedExpr(e AliasedExpr) interface{} {
	res := e.Expr.Accept(v)
	return res
}
func (v *usedColumns) VisitSelectCommand(c SelectCommand) interface{} {
	return nil
}

func (v *usedColumns) VisitWindowFunction(f WindowFunction) interface{} {
	res := make([]ColumnRef, 0)
	for _, expr := range f.Args {
		cur := expr.Accept(v)
		res = append(res, cur.([]ColumnRef)...)
	}
	res = append(res, f.OrderBy.Accept(v).([]ColumnRef)...)
	for _, expr := range f.PartitionBy {
		cur := expr.Accept(v)
		res = append(res, cur.([]ColumnRef)...)
	}
	return res
}

func (v *usedColumns) VisitParenExpr(p ParenExpr) interface{} {
	var exprs []Expr
	for _, expr := range p.Exprs {
		exprs = append(exprs, expr.Accept(v).(Expr))
	}
	return NewParenExpr(exprs...)
}
