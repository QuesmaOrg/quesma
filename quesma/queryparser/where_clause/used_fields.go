package where_clause

import "strings"

// UsedFieldsVisitor is a visitor that fetches all fields (columns) used in a given where clause
type UsedFieldsVisitor struct {
	Columns []*ColumnRef
}

//	Sheer beauty:
//		colFetch := &where_clause.UsedFieldsVisitor{}
//		parsedQuery.Sql.WhereStatement.Accept(colFetch)
//		cc := colFetch.GetColumnsUsed()

func (v *UsedFieldsVisitor) GetColumnsUsed() []*ColumnRef {
	return v.Columns
}

func (v *UsedFieldsVisitor) PrintColumnsUsed() string {
	var columns []string
	for _, col := range v.Columns {
		columns = append(columns, col.ColumnName)
	}
	return strings.Join(columns, ", ")
}

func (v *UsedFieldsVisitor) VisitLiteral(e *Literal) interface{} {
	return nil
}

func (v *UsedFieldsVisitor) VisitInfixOp(e *InfixOp) interface{} {
	e.Left.Accept(v)
	e.Right.Accept(v)
	return nil
}

func (v *UsedFieldsVisitor) VisitPrefixOp(e *PrefixOp) interface{} {
	for _, arg := range e.Args {
		arg.Accept(v)
	}
	return nil
}

func (v *UsedFieldsVisitor) VisitFunction(e *Function) interface{} {
	for _, arg := range e.Args {
		arg.Accept(v)
	}
	return nil
}

func (v *UsedFieldsVisitor) VisitColumnRef(e *ColumnRef) interface{} {
	v.Columns = append(v.Columns, e)
	return nil
}

func (v *UsedFieldsVisitor) VisitNestedProperty(e *NestedProperty) interface{} {
	v.Columns = append(v.Columns, &e.ColumnRef)
	return nil
}

func (v *UsedFieldsVisitor) VisitArrayAccess(e *ArrayAccess) interface{} {
	v.Columns = append(v.Columns, &e.ColumnRef)
	return nil
}
