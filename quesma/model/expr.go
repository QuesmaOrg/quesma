package model

import (
	"strings"
)

type Expr interface {
	Accept(v ExprVisitor) interface{}
}

type TableColumnExpr struct {
	TableAlias string
	ColumnName string
}

func (e TableColumnExpr) Accept(v ExprVisitor) interface{} {
	return v.VisitTableColumnExpr(e)
}

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
	return TableColumnExpr{ColumnName: columnName}
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
}
