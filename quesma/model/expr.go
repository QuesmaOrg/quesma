package model

import "context"

// Expr is a generic representation of an expression which is a part of the SQL query.
type Expr interface {
	Accept(ctx context.Context, v ExprVisitor) interface{}
}

// ColumnRef is a reference to a column in a table, we can enrich it with more information (e.g. type used) as we go
type ColumnRef struct {
	ColumnName string
}

func NewColumnRef(name string) ColumnRef {
	return ColumnRef{ColumnName: name}
}

func (e ColumnRef) Accept(ctx context.Context, v ExprVisitor) interface{} {
	return v.VisitColumnRef(ctx, e)
}

// PrefixExpr represents unary operators, e.g. NOT, - etc.
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

func (e PrefixExpr) Accept(ctx context.Context, v ExprVisitor) interface{} {
	return v.VisitPrefixExpr(ctx, e)
}

// NestedProperty represents a call to nested property e.g. `columnName.propertyName`
type NestedProperty struct {
	ColumnRef    ColumnRef
	PropertyName LiteralExpr
}

func NewNestedProperty(columnRef ColumnRef, propertyName LiteralExpr) NestedProperty {
	return NestedProperty{ColumnRef: columnRef, PropertyName: propertyName}
}

func (e NestedProperty) Accept(ctx context.Context, v ExprVisitor) interface{} {
	return v.VisitNestedProperty(ctx, e)
}

// ArrayAccess represents accessing array by index, e.g. `columnName[0]`
type ArrayAccess struct {
	ColumnRef ColumnRef
	Index     Expr
}

func NewArrayAccess(columnRef ColumnRef, index Expr) ArrayAccess {
	return ArrayAccess{ColumnRef: columnRef, Index: index}
}

func (e ArrayAccess) Accept(ctx context.Context, v ExprVisitor) interface{} {
	return v.VisitArrayAccess(ctx, e)
}

type FunctionExpr struct {
	Name string
	Args []Expr
}

func (e FunctionExpr) Accept(ctx context.Context, v ExprVisitor) interface{} {
	return v.VisitFunction(ctx, e)
}

// MultiFunctionExpr represents call of a function with multiple arguments lists, e.g. `quantile(level)(expr)`
type MultiFunctionExpr struct {
	Name string
	Args []Expr
}

func (e MultiFunctionExpr) Accept(ctx context.Context, v ExprVisitor) interface{} {
	return v.VisitMultiFunction(ctx, e)
}

type LiteralExpr struct {
	Value any
}

func (e LiteralExpr) Accept(ctx context.Context, v ExprVisitor) interface{} {
	return v.VisitLiteral(ctx, e)
}

// Deprecated
type StringExpr struct {
	// StringExpr is just like LiteralExpr with string Value, but when rendering we don't quote it.
	Value string
}

func (e StringExpr) Accept(ctx context.Context, v ExprVisitor) interface{} {
	return v.VisitString(ctx, e)
}

type InfixExpr struct {
	Left  Expr
	Op    string
	Right Expr
}

func (e InfixExpr) Accept(ctx context.Context, v ExprVisitor) interface{} {
	return v.VisitInfix(ctx, e)
}

func NewFunction(name string, args ...Expr) FunctionExpr {
	return FunctionExpr{Name: name, Args: args}
}

func NewCountFunc(args ...Expr) FunctionExpr {
	return NewFunction("count", args...)
}

var NewWildcardExpr = LiteralExpr{Value: "*"}

func NewStringExpr(value string) StringExpr {
	return StringExpr{Value: value}
}

func NewLiteral(value any) LiteralExpr {
	return LiteralExpr{Value: value}
}

// DistinctExpr is a representation of DISTINCT keyword in SQL, e.g. `SELECT DISTINCT` ... or `SELECT COUNT(DISTINCT ...)`
type DistinctExpr struct {
	Expr Expr
}

func NewDistinctExpr(expr Expr) DistinctExpr {
	return DistinctExpr{Expr: expr}
}

func (s DistinctExpr) Accept(ctx context.Context, v ExprVisitor) interface{} {
	return v.VisitDistinctExpr(ctx, s)
}

// TableRef is an explicit reference to a table in a query
type TableRef struct {
	Name string
	// to be considered - alias (e.g. FROM tableName AS t)
	// to be considered - database prefix (e.g. FROM databaseName.tableName)
}

func NewTableRef(name string) TableRef {
	return TableRef{Name: name}
}

func (t TableRef) Accept(ctx context.Context, v ExprVisitor) interface{} {
	return v.VisitTableRef(ctx, t)
}

type OrderByDirection int8

const (
	DefaultOrder OrderByDirection = iota // DEFAULT means leaving ordering unspecified and deferring to whatever DBMS default is
	AscOrder
	DescOrder
)

type OrderByExpr struct {
	Exprs     []Expr
	Direction OrderByDirection
}

func (o OrderByExpr) Accept(ctx context.Context, v ExprVisitor) interface{} {
	return v.VisitOrderByExpr(ctx, o)
}

func NewOrderByExpr(exprs []Expr, direction OrderByDirection) OrderByExpr {
	return OrderByExpr{Exprs: exprs, Direction: direction}
}
func NewOrderByExprWithoutOrder(exprs ...Expr) OrderByExpr {
	return OrderByExpr{Exprs: exprs, Direction: DefaultOrder}
}

func NewInfixExpr(lhs Expr, operator string, rhs Expr) InfixExpr {
	return InfixExpr{lhs, operator, rhs}
}

// AliasedExpr is an expression with an alias, e.g. `columnName AS alias` or `COUNT(x) AS sum_of_xs`
type AliasedExpr struct {
	Expr  Expr
	Alias string
}

func NewAliasedExpr(expr Expr, alias string) AliasedExpr {
	return AliasedExpr{Expr: expr, Alias: alias}
}

func (a AliasedExpr) Accept(ctx context.Context, v ExprVisitor) interface{} {
	return v.VisitAliasedExpr(ctx, a)
}

// WindowFunction representation e.g. `SUM(x) OVER (PARTITION BY y ORDER BY z)`
type WindowFunction struct {
	Name        string
	Args        []Expr
	PartitionBy []Expr
	OrderBy     OrderByExpr
}

func NewWindowFunction(name string, args, partitionBy []Expr, orderBy OrderByExpr) WindowFunction {
	return WindowFunction{Name: name, Args: args, PartitionBy: partitionBy, OrderBy: orderBy}
}

func (f WindowFunction) Accept(ctx context.Context, v ExprVisitor) interface{} {
	return v.VisitWindowFunction(ctx, f)
}

type ExprVisitor interface {
	VisitFunction(ctx context.Context, e FunctionExpr) interface{}
	VisitMultiFunction(ctx context.Context, e MultiFunctionExpr) interface{}
	VisitLiteral(ctx context.Context, l LiteralExpr) interface{}
	VisitString(ctx context.Context, e StringExpr) interface{}
	VisitInfix(ctx context.Context, e InfixExpr) interface{}
	VisitColumnRef(ctx context.Context, e ColumnRef) interface{}
	VisitPrefixExpr(ctx context.Context, e PrefixExpr) interface{}
	VisitNestedProperty(ctx context.Context, e NestedProperty) interface{}
	VisitArrayAccess(ctx context.Context, e ArrayAccess) interface{}
	VisitOrderByExpr(ctx context.Context, e OrderByExpr) interface{}
	VisitDistinctExpr(ctx context.Context, e DistinctExpr) interface{}
	VisitTableRef(ctx context.Context, e TableRef) interface{}
	VisitAliasedExpr(ctx context.Context, e AliasedExpr) interface{}
	VisitSelectCommand(ctx context.Context, e SelectCommand) interface{}
	VisitWindowFunction(ctx context.Context, f WindowFunction) interface{}
}
