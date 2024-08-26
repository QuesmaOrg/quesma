// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package model

type NoOpVisitor struct {
	ExprVisitor
}

func (NoOpVisitor) VisitFunction(e FunctionExpr) interface{}         { return e }
func (NoOpVisitor) VisitLiteral(l LiteralExpr) interface{}           { return l }
func (NoOpVisitor) VisitInfix(e InfixExpr) interface{}               { return e }
func (NoOpVisitor) VisitColumnRef(e ColumnRef) interface{}           { return e }
func (NoOpVisitor) VisitPrefixExpr(e PrefixExpr) interface{}         { return e }
func (NoOpVisitor) VisitNestedProperty(e NestedProperty) interface{} { return e }
func (NoOpVisitor) VisitArrayAccess(e ArrayAccess) interface{}       { return e }
func (NoOpVisitor) VisitOrderByExpr(e OrderByExpr) interface{}       { return e }
func (NoOpVisitor) VisitDistinctExpr(e DistinctExpr) interface{}     { return e }
func (NoOpVisitor) VisitTableRef(e TableRef) interface{}             { return e }
func (NoOpVisitor) VisitAliasedExpr(e AliasedExpr) interface{}       { return e }
func (NoOpVisitor) VisitSelectCommand(e SelectCommand) interface{}   { return e }
func (NoOpVisitor) VisitWindowFunction(f WindowFunction) interface{} { return f }
func (NoOpVisitor) VisitParenExpr(e ParenExpr) interface{}           { return e }
func (NoOpVisitor) VisitLambdaExpr(e LambdaExpr) interface{}         { return e }
func (NoOpVisitor) VisitJoinExpr(e JoinExpr) interface{}             { return e }
