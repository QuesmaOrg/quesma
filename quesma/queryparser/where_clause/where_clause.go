package where_clause

import "fmt"

// Statement is main structure for WHERE clause
type Statement interface {
	Accept(v StatementVisitor) interface{}
}

// ColumnRef is a reference to a column in a table, we can enrich it with more information (e.g. type used) as we go
type ColumnRef struct {
	ColumnName string
}

func NewColumnRef(name string) *ColumnRef {
	return &ColumnRef{ColumnName: name}
}

func (e *ColumnRef) Accept(v StatementVisitor) interface{} {
	return v.VisitColumnRef(e)
}

type Literal struct {
	Name string
}

// NestedProperty for nested objects, e.g. `columnName.propertyName`
type NestedProperty struct {
	ColumnRef    ColumnRef
	PropertyName Literal
}

func NewNestedProperty(columnRef ColumnRef, propertyName Literal) *NestedProperty {
	return &NestedProperty{ColumnRef: columnRef, PropertyName: propertyName}
}

func (e *NestedProperty) Accept(v StatementVisitor) interface{} { return v.VisitNestedProperty(e) }

// ArrayAccess for array accessing, e.g. `columnName[0]`
type ArrayAccess struct {
	ColumnRef ColumnRef
	Index     Statement
}

func NewArrayAccess(columnRef ColumnRef, index Statement) *ArrayAccess {
	return &ArrayAccess{ColumnRef: columnRef, Index: index}
}

func (e *ArrayAccess) Accept(v StatementVisitor) interface{} { return v.VisitArrayAccess(e) }

func NewLiteral(name string) *Literal {
	return &Literal{Name: name}
}

func (e *Literal) String() string {
	return fmt.Sprintf("(Literal %v)", e.Name)
}

func (e *Literal) Accept(v StatementVisitor) interface{} {
	return v.VisitLiteral(e)
}

type InfixOp struct {
	Left  Statement
	Op    string
	Right Statement
}

func NewInfixOp(left Statement, op string, right Statement) *InfixOp {
	return &InfixOp{
		Left:  left,
		Op:    op,
		Right: right,
	}
}

func (e *InfixOp) String() string {
	return fmt.Sprintf("(infix '%v' %v %v)", e.Op, e.Left, e.Right)
}

func (e *InfixOp) Accept(v StatementVisitor) interface{} {
	return v.VisitInfixOp(e)
}

type PrefixOp struct {
	Op   string
	Args []Statement
}

func NewPrefixOp(op string, args []Statement) *PrefixOp {
	return &PrefixOp{
		Op:   op,
		Args: args,
	}
}

func (e *PrefixOp) String() string {
	return fmt.Sprintf("(prefix '%v' %v)", e.Op, e.Args)
}

func (e *PrefixOp) Accept(v StatementVisitor) interface{} {
	return v.VisitPrefixOp(e)
}

type Function struct {
	Name Literal
	Args []Statement
}

func NewFunction(name string, args ...Statement) *Function {
	return &Function{
		Name: Literal{Name: name},
		Args: args,
	}
}

func (e *Function) String() string {
	return fmt.Sprintf("(function %v %v)", e.Name, e.Args)
}

func (e *Function) Accept(v StatementVisitor) interface{} {
	return v.VisitFunction(e)
}

type StatementVisitor interface {
	VisitLiteral(e *Literal) interface{}
	VisitInfixOp(e *InfixOp) interface{}
	VisitPrefixOp(e *PrefixOp) interface{}
	VisitFunction(e *Function) interface{}
	VisitColumnRef(e *ColumnRef) interface{}
	VisitNestedProperty(e *NestedProperty) interface{}
	VisitArrayAccess(e *ArrayAccess) interface{}
}
