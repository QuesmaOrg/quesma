package model

import (
	"strings"
)

// Expr is a generic representation of an expression which is a part of the SQL query.
type Expr interface {
	Accept(v ExprVisitor) interface{}
}

// ColumnRef is a reference to a column in a table, we can enrich it with more information (e.g. type used) as we go
type ColumnRef struct {
	ColumnName string
}

func NewColumnRef(name string) ColumnRef {
	return ColumnRef{ColumnName: name}
}

func (e ColumnRef) Accept(v ExprVisitor) interface{} {
	return v.VisitColumnRef(e)
}

type PrefixExpr struct {
	Op   string
	Args []Expr
}

func NewPrefixExpr(op string, args []Expr) PrefixExpr {
	return PrefixExpr{
		Op:   op,
		Args: args,
	}
}

func (e PrefixExpr) Accept(v ExprVisitor) interface{} {
	return v.VisitPrefixExpr(e)
}

// TableColumnExpr is a little questionable at this point
type TableColumnExpr struct {
	TableAlias string
	ColumnRef  ColumnRef
}

func (e TableColumnExpr) Accept(v ExprVisitor) interface{} {
	return v.VisitTableColumnExpr(e)
}

// NestedProperty for nested objects, e.g. `columnName.propertyName`
type NestedProperty struct {
	ColumnRef    ColumnRef
	PropertyName LiteralExpr
}

func NewNestedProperty(columnRef ColumnRef, propertyName LiteralExpr) NestedProperty {
	return NestedProperty{ColumnRef: columnRef, PropertyName: propertyName}
}

func (e NestedProperty) Accept(v ExprVisitor) interface{} { return v.VisitNestedProperty(e) }

// ArrayAccess for array accessing, e.g. `columnName[0]`
type ArrayAccess struct {
	ColumnRef ColumnRef
	Index     Expr
}

func NewArrayAccess(columnRef ColumnRef, index Expr) ArrayAccess {
	return ArrayAccess{ColumnRef: columnRef, Index: index}
}

func (e ArrayAccess) Accept(v ExprVisitor) interface{} { return v.VisitArrayAccess(e) }

type FunctionExpr struct {
	Name string
	Args []Expr
}

func (e FunctionExpr) Accept(v ExprVisitor) interface{} {
	return v.VisitFunction(e)
}

// It represents functions with multitple arguments list
// like `quantile(level)(expr)
type MultiFunctionExpr struct {
	Name string
	Args []Expr
}

func (e MultiFunctionExpr) Accept(v ExprVisitor) interface{} {
	return v.VisitMultiFunction(e)
}

type LiteralExpr struct {
	Value any
}

func (e LiteralExpr) Accept(v ExprVisitor) interface{} {
	return v.VisitLiteral(e)
}

// StringExpr is just like LiteralExpr with string Value, but when rendering we don't quote it.
// Used e.g. for representing ASC/DESC, or tablename
type StringExpr struct {
	Value string
}

func (e StringExpr) Accept(v ExprVisitor) interface{} {
	return v.VisitString(e)
}

// deprecated
type CompositeExpr struct { // Space separated expressions, we should figure out something better
	Expressions []Expr
}

func (e CompositeExpr) Accept(v ExprVisitor) interface{} {
	return v.VisitComposite(e)
}

type InfixExpr struct {
	Left  Expr
	Op    string
	Right Expr
}

func (e InfixExpr) Accept(v ExprVisitor) interface{} {
	return v.VisitInfix(e)
}

// ASIS expressions, this is workaroung for not supported expressions
// It can be named as TODO.
type SQL struct {
	Query string
}

func (s SQL) Accept(v ExprVisitor) interface{} {
	return v.VisitSQL(s)
}

func NewFunction(name string, args ...Expr) FunctionExpr {
	return FunctionExpr{Name: name, Args: args}
}

func NewCountFunc(args ...Expr) FunctionExpr {
	return NewFunction("count", args...)
}

var NewWildcardExpr = LiteralExpr{Value: "*"}

// it will render as IS
type symbol string

func Symbol(s string) LiteralExpr {
	return NewLiteral(symbol(s))
}

func NewTableColumnExpr(columnName string) TableColumnExpr {
	columnName = strings.TrimSuffix(columnName, ".keyword")
	return TableColumnExpr{ColumnRef: NewColumnRef(columnName)}
}

func NewStringExpr(value string) StringExpr {
	return StringExpr{Value: value}
}

func NewLiteral(value any) LiteralExpr {
	return LiteralExpr{Value: value}
}

func NewComposite(Exprressions ...Expr) *CompositeExpr {
	return &CompositeExpr{Expressions: Exprressions}
}

func NewInfixExpr(lhs Expr, operator string, rhs Expr) InfixExpr {
	return InfixExpr{lhs, operator, rhs}
}

type ExprVisitor interface {
	VisitTableColumnExpr(e TableColumnExpr) interface{}
	VisitFunction(e FunctionExpr) interface{}
	VisitMultiFunction(e MultiFunctionExpr) interface{}
	VisitLiteral(l LiteralExpr) interface{}
	VisitString(e StringExpr) interface{}
	VisitComposite(e CompositeExpr) interface{}
	VisitInfix(e InfixExpr) interface{}
	VisitSQL(s SQL) interface{}
	VisitColumnRef(e ColumnRef) interface{}
	VisitPrefixExpr(e PrefixExpr) interface{}
	VisitNestedProperty(e NestedProperty) interface{}
	VisitArrayAccess(e ArrayAccess) interface{}
}
