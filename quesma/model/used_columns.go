package model

import "context"

// TODO OKAY THIS NEEDS TO BE FIXED FOR THE NEW WHERE STATEMENT
type usedColumns struct{}

func GetUsedColumns(expr Expr) []ColumnRef {
	ctx := context.TODO()
	return expr.Accept(ctx, &usedColumns{}).([]ColumnRef)
}

func (v *usedColumns) VisitPrefixExpr(ctx context.Context, e PrefixExpr) interface{} {
	res := make([]ColumnRef, 0)
	for _, arg := range e.Args {
		v1 := arg.Accept(ctx, v)
		if v2, ok := v1.([]ColumnRef); ok {
			res = append(res, v2...)
		}
	}
	return res
}

func (v *usedColumns) VisitNestedProperty(ctx context.Context, e NestedProperty) interface{} {
	return nil
}

func (v *usedColumns) VisitArrayAccess(ctx context.Context, e ArrayAccess) interface{} {
	return nil
}

func (v *usedColumns) VisitColumnRef(ctx context.Context, e ColumnRef) interface{} {
	return []ColumnRef{e}
}

func (v *usedColumns) VisitFunction(ctx context.Context, e FunctionExpr) interface{} {
	res := make([]ColumnRef, 0)
	for _, arg := range e.Args {
		v1 := arg.Accept(ctx, v)
		if v2, ok := v1.([]ColumnRef); ok {
			res = append(res, v2...)
		}
	}
	return res
}

func (v *usedColumns) VisitLiteral(_ context.Context, _ LiteralExpr) interface{} {
	return make([]ColumnRef, 0)
}

func (v *usedColumns) VisitMultiFunction(ctx context.Context, f MultiFunctionExpr) interface{} {
	res := make([]ColumnRef, 0)
	for _, arg := range f.Args {
		v1 := arg.Accept(ctx, v)
		if v2, ok := v1.([]ColumnRef); ok {
			res = append(res, v2...)
		}
	}
	return res
}

func (v *usedColumns) VisitInfix(ctx context.Context, e InfixExpr) interface{} {
	res := make([]ColumnRef, 0)
	v1 := e.Left.Accept(ctx, v)
	if v2, ok := v1.([]ColumnRef); ok {
		res = append(res, v2...)
	}
	v3 := e.Right.Accept(ctx, v)
	if v4, ok := v3.([]ColumnRef); ok {
		res = append(res, v4...)
	}
	return res
}

func (v *usedColumns) VisitString(_ context.Context, _ StringExpr) interface{} {
	return make([]ColumnRef, 0)
}

func (v *usedColumns) VisitOrderByExpr(ctx context.Context, e OrderByExpr) interface{} {
	res := make([]ColumnRef, 0)
	for _, expr := range e.Exprs {
		cur := expr.Accept(ctx, v)
		res = append(res, cur.([]ColumnRef)...)
	}
	return res
}

func (v *usedColumns) VisitDistinctExpr(ctx context.Context, e DistinctExpr) interface{} {
	return e.Expr.Accept(ctx, v)
}

func (v *usedColumns) VisitTableRef(_ context.Context, _ TableRef) interface{} {
	return make([]ColumnRef, 0)
}

func (v *usedColumns) VisitAliasedExpr(ctx context.Context, e AliasedExpr) interface{} {
	res := e.Expr.Accept(ctx, v)
	return res
}
func (v *usedColumns) VisitSelectCommand(_ context.Context, _ SelectCommand) interface{} {
	return nil
}

func (v *usedColumns) VisitWindowFunction(ctx context.Context, f WindowFunction) interface{} {
	res := make([]ColumnRef, 0)
	for _, expr := range f.Args {
		cur := expr.Accept(ctx, v)
		res = append(res, cur.([]ColumnRef)...)
	}
	res = append(res, f.OrderBy.Accept(ctx, v).([]ColumnRef)...)
	for _, expr := range f.PartitionBy {
		cur := expr.Accept(ctx, v)
		res = append(res, cur.([]ColumnRef)...)
	}
	return res
}
