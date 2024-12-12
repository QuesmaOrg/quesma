// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package model

import (
	"fmt"
	"strconv"
)

// Expr is a generic representation of an expression which is a part of the SQL query.
type Expr interface {
	Accept(v ExprVisitor) interface{}
}

var (
	InvalidExpr = Expr(nil)
	TrueExpr    = NewLiteral(true)
)

// ColumnRef is a reference to a column in a table, we can enrich it with more information (e.g. type used) as we go
type ColumnRef struct {
	TableAlias string // used for alias in joins, most of the times empty string.
	ColumnName string
}

func NewColumnRef(name string) ColumnRef {
	return ColumnRef{ColumnName: name}
}

func (e ColumnRef) Accept(v ExprVisitor) interface{} {
	return v.VisitColumnRef(e)
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

func (e PrefixExpr) Accept(v ExprVisitor) interface{} {
	return v.VisitPrefixExpr(e)
}

// NestedProperty represents a call to nested property e.g. `columnName.propertyName`
type NestedProperty struct {
	ColumnRef    ColumnRef
	PropertyName LiteralExpr
}

func NewNestedProperty(columnRef ColumnRef, propertyName LiteralExpr) NestedProperty {
	return NestedProperty{ColumnRef: columnRef, PropertyName: propertyName}
}

func (e NestedProperty) Accept(v ExprVisitor) interface{} { return v.VisitNestedProperty(e) }

// ArrayAccess represents accessing array by index, e.g. `columnName[0]`
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

type LiteralExpr struct {
	Value any
}

func (e LiteralExpr) Accept(v ExprVisitor) interface{} {
	return v.VisitLiteral(e)
}

type InfixExpr struct {
	Left  Expr
	Op    string
	Right Expr
}

func (e InfixExpr) Accept(v ExprVisitor) interface{} {
	return v.VisitInfix(e)
}

func NewFunction(name string, args ...Expr) FunctionExpr {
	return FunctionExpr{Name: name, Args: args}
}

func NewCountFunc(args ...Expr) FunctionExpr {
	if len(args) == 0 {
		args = []Expr{NewWildcardExpr}
	}
	return NewFunction("count", args...)
}

var NewWildcardExpr = LiteralExpr{Value: "*"}

func NewLiteral(value any) LiteralExpr {
	return LiteralExpr{Value: value}
}

func NewLiteralSingleQuoted(value string) LiteralExpr {
	return LiteralExpr{Value: fmt.Sprintf("'%s'", value)}
}

// DistinctExpr is a representation of DISTINCT keyword in SQL, e.g. `SELECT DISTINCT` ... or `SELECT COUNT(DISTINCT ...)`
type DistinctExpr struct {
	Expr Expr
}

func NewDistinctExpr(expr Expr) DistinctExpr {
	return DistinctExpr{Expr: expr}
}

func (s DistinctExpr) Accept(v ExprVisitor) interface{} {
	return v.VisitDistinctExpr(s)
}

// TableRef is an explicit reference to a table in a query
type TableRef struct {
	Name string
	// to be considered - alias (e.g. FROM tableName AS t)

	// DatabaseName is optional and represents what in database realm is called 'schema', e.g. 'FROM databaseName.tableName'
	// ClickHouse calls this 'database' so we stick to that; FWIW - Hydrolix calls this a 'project'.
	DatabaseName string
}

func NewTableRef(name string) TableRef {
	return TableRef{Name: name}
}

func NewTableRefWithDatabaseName(name, databaseName string) TableRef {
	return TableRef{Name: name, DatabaseName: databaseName}
}

func (t TableRef) Accept(v ExprVisitor) interface{} {
	return v.VisitTableRef(t)
}

type OrderByDirection int8

const (
	DefaultOrder OrderByDirection = iota // DEFAULT means leaving ordering unspecified and deferring to whatever DBMS default is
	AscOrder
	DescOrder
)

type OrderByExpr struct {
	Expr      Expr
	Direction OrderByDirection
}

func (o OrderByExpr) Accept(v ExprVisitor) interface{} {
	return v.VisitOrderByExpr(o)
}

func NewOrderByExpr(expr Expr, direction OrderByDirection) OrderByExpr {
	return OrderByExpr{Expr: expr, Direction: direction}
}
func NewOrderByExprWithoutOrder(expr Expr) OrderByExpr {
	return OrderByExpr{Expr: expr, Direction: DefaultOrder}
}

// IsCountDesc returns true <=> this OrderByExpr is count() DESC
func (o OrderByExpr) IsCountDesc() bool {
	if o.Direction != DescOrder {
		return false
	}
	function, ok := o.Expr.(FunctionExpr)
	return ok && function.Name == "count"
}

func NewInfixExpr(lhs Expr, operator string, rhs Expr) InfixExpr {
	return InfixExpr{Left: lhs, Op: operator, Right: rhs}
}

// AliasedExpr is an expression with an alias, e.g. `columnName AS alias` or `COUNT(x) AS sum_of_xs`
type AliasedExpr struct {
	Expr  Expr
	Alias string
}

func NewAliasedExpr(expr Expr, alias string) AliasedExpr {
	return AliasedExpr{Expr: expr, Alias: alias}
}

func (a AliasedExpr) Accept(v ExprVisitor) interface{} { return v.VisitAliasedExpr(a) }

func (a AliasedExpr) AliasRef() LiteralExpr {
	return LiteralExpr{Value: strconv.Quote(a.Alias)}
}

// WindowFunction representation e.g. `SUM(x) OVER (PARTITION BY y ORDER BY z)`
type WindowFunction struct {
	Name        string
	Args        []Expr
	PartitionBy []Expr
	OrderBy     []OrderByExpr
}

func NewWindowFunction(name string, args, partitionBy []Expr, orderBy []OrderByExpr) WindowFunction {
	return WindowFunction{Name: name, Args: args, PartitionBy: partitionBy, OrderBy: orderBy}
}

func (f WindowFunction) Accept(v ExprVisitor) interface{} { return v.VisitWindowFunction(f) }

// ParenExpr enables grouping of expressions with parentheses
// e.g. `SELECT (x + y) * z`
// This is important for precedence of operators
type ParenExpr struct {
	Exprs []Expr
}

func NewParenExpr(exprs ...Expr) ParenExpr {
	return ParenExpr{Exprs: exprs}
}

func (p ParenExpr) Accept(v ExprVisitor) interface{} {
	return v.VisitParenExpr(p)
}

// LambdaExpr represents a lambda expression,
// e.g. `x -> x LIKE '%foo'%`
// Some Clickhouse functions take lambda expressions as an argument.
type LambdaExpr struct {
	Args []string
	Body Expr
}

func NewLambdaExpr(args []string, body Expr) LambdaExpr {
	return LambdaExpr{Args: args, Body: body}
}

func (l LambdaExpr) Accept(v ExprVisitor) interface{} {
	return v.VisitLambdaExpr(l)
}

// JoinExpr represents a JOIN expression, e.g. `table1 INNER JOIN table2 ON (table1.id = table2.id)`
type JoinExpr struct {
	Lhs      Expr
	JoinType string
	Rhs      Expr
	On       Expr
}

func NewJoinExpr(lhs, rhs Expr, joinType string, on Expr) JoinExpr {
	return JoinExpr{Lhs: lhs, JoinType: joinType, Rhs: rhs, On: on}
}

func (e JoinExpr) Accept(v ExprVisitor) interface{} {
	return v.VisitJoinExpr(e)
}

type CTE struct {
	Name          string
	SelectCommand *SelectCommand
}

func NewCTE(name string, selectCommand *SelectCommand) *CTE {
	return &CTE{Name: name, SelectCommand: selectCommand}
}

func (e CTE) Accept(v ExprVisitor) interface{} {
	return v.VisitCTE(e)
}

type ExprVisitor interface {
	VisitFunction(e FunctionExpr) interface{}
	VisitLiteral(l LiteralExpr) interface{}
	VisitInfix(e InfixExpr) interface{}
	VisitColumnRef(e ColumnRef) interface{}
	VisitPrefixExpr(e PrefixExpr) interface{}
	VisitNestedProperty(e NestedProperty) interface{}
	VisitArrayAccess(e ArrayAccess) interface{}
	VisitOrderByExpr(e OrderByExpr) interface{}
	VisitDistinctExpr(e DistinctExpr) interface{}
	VisitTableRef(e TableRef) interface{}
	VisitAliasedExpr(e AliasedExpr) interface{}
	VisitSelectCommand(e SelectCommand) interface{}
	VisitWindowFunction(f WindowFunction) interface{}
	VisitParenExpr(e ParenExpr) interface{}
	VisitLambdaExpr(e LambdaExpr) interface{}
	VisitJoinExpr(e JoinExpr) interface{}
	VisitCTE(e CTE) interface{}
}
