package aexp

type used_columns struct{}

func (v *used_columns) VisitTableColumn(e TableColumnExp) interface{} {
	return []TableColumnExp{e}
}

func (v *used_columns) VisitFunction(e FunctionExp) interface{} {
	res := make([]TableColumnExp, 0)
	for _, arg := range e.Args {
		v1 := arg.Accept(v)
		if v2, ok := v1.([]TableColumnExp); ok {
			res = append(res, v2...)
		}
	}
	return res
}

func (v *used_columns) VisitLiteral(l LiteralExp) interface{} {
	return make([]TableColumnExp, 0)
}

func (v *used_columns) VisitComposite(e CompositeExp) interface{} {
	res := make([]TableColumnExp, 0)
	for _, arg := range e.Expressions {
		v1 := arg.Accept(v)
		if v2, ok := v1.([]TableColumnExp); ok {
			res = append(res, v2...)
		}
	}
	return res
}

func (v *used_columns) VisitSQL(s SQL) interface{} {
	return make([]TableColumnExp, 0)
}

func (v *used_columns) VisitMultiFunction(f MultiFunctionExp) interface{} {
	res := make([]TableColumnExp, 0)
	for _, arg := range f.Args {
		v1 := arg.Accept(v)
		if v2, ok := v1.([]TableColumnExp); ok {
			res = append(res, v2...)
		}
	}
	return res
}

func (v *used_columns) VisitInfix(e InfixExp) interface{} {
	res := make([]TableColumnExp, 0)
	v1 := e.Left.Accept(v)
	if v2, ok := v1.([]TableColumnExp); ok {
		res = append(res, v2...)
	}
	v3 := e.Right.Accept(v)
	if v4, ok := v3.([]TableColumnExp); ok {
		res = append(res, v4...)
	}
	return res
}

func (v *used_columns) VisitString(e StringExp) interface{} {
	return make([]TableColumnExp, 0)
}
