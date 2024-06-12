package model

// TODO OKAY THIS NEEDS TO BE FIXED FOR THE NEW WHERE STATEMENT
type usedColumns struct{}

func GetUsedColumns(expr Expr) []TableColumnExpr {
	return expr.Accept(&usedColumns{}).([]TableColumnExpr)
}

// TODO ugh this needs to be changed ...
func (v *usedColumns) VisitColumnRef(e ColumnRef) interface{} {
	res := make([]TableColumnExpr, 0)

	return res
}

func (v *usedColumns) VisitPrefixExpr(e PrefixExpr) interface{} {
	res := make([]TableColumnExpr, 0)
	for _, arg := range e.Args {
		v1 := arg.Accept(v)
		if v2, ok := v1.([]TableColumnExpr); ok {
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

func (v *usedColumns) VisitTableColumnExpr(e TableColumnExpr) interface{} {
	return []TableColumnExpr{e}
}

func (v *usedColumns) VisitFunction(e FunctionExpr) interface{} {
	res := make([]TableColumnExpr, 0)
	for _, arg := range e.Args {
		v1 := arg.Accept(v)
		if v2, ok := v1.([]TableColumnExpr); ok {
			res = append(res, v2...)
		}
	}
	return res
}

func (v *usedColumns) VisitLiteral(l LiteralExpr) interface{} {
	return make([]TableColumnExpr, 0)
}

func (v *usedColumns) VisitComposite(e CompositeExpr) interface{} {
	res := make([]TableColumnExpr, 0)
	for _, arg := range e.Expressions {
		v1 := arg.Accept(v)
		if v2, ok := v1.([]TableColumnExpr); ok {
			res = append(res, v2...)
		}
	}
	return res
}

func (v *usedColumns) VisitSQL(s SQL) interface{} {
	return make([]TableColumnExpr, 0)
}

func (v *usedColumns) VisitMultiFunction(f MultiFunctionExpr) interface{} {
	res := make([]TableColumnExpr, 0)
	for _, arg := range f.Args {
		v1 := arg.Accept(v)
		if v2, ok := v1.([]TableColumnExpr); ok {
			res = append(res, v2...)
		}
	}
	return res
}

func (v *usedColumns) VisitInfix(e InfixExpr) interface{} {
	res := make([]TableColumnExpr, 0)
	v1 := e.Left.Accept(v)
	if v2, ok := v1.([]TableColumnExpr); ok {
		res = append(res, v2...)
	}
	v3 := e.Right.Accept(v)
	if v4, ok := v3.([]TableColumnExpr); ok {
		res = append(res, v4...)
	}
	return res
}

func (v *usedColumns) VisitString(e StringExpr) interface{} {
	return make([]TableColumnExpr, 0)
}

func (v *usedColumns) VisitOrderByExpr(e OrderByExpr) interface{} {
	return make([]TableColumnExpr, 0) //TODO
}
