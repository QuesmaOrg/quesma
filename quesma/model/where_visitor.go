// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package model

import (
	"fmt"
	"math"
	"quesma/util"
	"strings"
)

// FindTimestampBounds returns y if there is "x>=y" or "x>y" in the WHERE clause, but only as a single top-level expression.
// (I mean by that a>=0 is fine, a>=0 AND expr2 [AND ...]] is also fine (AND between all), but a>=0 OR e2 is not fine.
// a>=0 AND (expr2 OR expr3) is also fine, as on top level it's only an AND.
// We achieve that by only descending for AND operators.
// If there are multiple such expressions, we return the smallest one.
func FindTimestampBounds(field ColumnRef, whereClause Expr) (lowerBoundInMs int64, lowerBoundFound bool,
	upperBoundInMs int64, upperBoundFound bool) {
	
	lowerBoundInMs = math.MaxInt64
	upperBoundInMs = math.MaxInt64
	visitor := NewBaseVisitor()
	visitor.OverrideVisitInfix = func(visitor *BaseExprVisitor, e InfixExpr) interface{} {
		if strings.ToUpper(e.Op) == "AND" {
			e.Left.Accept(visitor)
			e.Right.Accept(visitor)
			return nil
		}

		columnRef, ok := e.Left.(ColumnRef)
		goodField := ok && columnRef == field
		goodOp := e.Op == ">=" || e.Op == ">" || e.Op == "<" || e.Op == "<="
		fun, ok := e.Right.(FunctionExpr)
		goodFun := ok && len(fun.Args) == 1
		if !goodField || !goodOp || !goodFun {
			return nil
		}

		var value int64
		var found bool
		if fun.Name == "fromUnixTimestamp64Milli" {
			if rhs, ok := fun.Args[0].(LiteralExpr); ok {
				value, found = util.ExtractInt64Maybe(rhs.Value)
			}
		} else if fun.Name == "fromUnixTimestamp" {
			if rhs, ok := fun.Args[0].(LiteralExpr); ok {
				value, found = util.ExtractInt64Maybe(rhs.Value)
				value *= 1000
			}
		}

		if found && (e.Op == ">=" || e.Op == ">") {
			lowerBoundInMs = min(lowerBoundInMs, value)
			lowerBoundFound = true
		}
		if found && (e.Op == "<" || e.Op == "<=") {
			upperBoundInMs = min(upperBoundInMs, value)
			upperBoundFound = true
		}

		return nil
	}

	whereClause.Accept(visitor)
	fmt.Println("lowerBoundInMs: ", lowerBoundInMs, "upperBoundInMs: ", upperBoundInMs)
	return
}
