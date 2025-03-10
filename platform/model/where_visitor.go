// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package model

import (
	"time"
)

// FindTimestampLowerBound returns y if there is "x>=y" or "x>y" in the WHERE clause, but only as a single top-level expression.
// (I mean by that a>=0 is fine, a>=0 AND expr2 [AND ...]] is also fine (AND between all), but a>=0 OR e2 is not fine.
// a>=0 AND (expr2 OR expr3) is also fine, as on top level it's only an AND.
// We achieve that by only descending for AND operators.
// If there are multiple such expressions, we return the smallest one.
//
// TODO: add upper bound here too, when bucket_nr=1 in auto_date_histogram (only use case of this function), it's not needed.
func FindTimestampLowerBound(field ColumnRef, whereClause Expr) (lowerBoundTs time.Time, found bool) {
	visitor := NewBaseVisitor()
	visitor.OverrideVisitInfix = func(visitor *BaseExprVisitor, e InfixExpr) interface{} {
		if columnRef, ok := e.Left.(ColumnRef); ok && columnRef == field && e.Op == ">=" || e.Op == ">" {
			if fun, ok := e.Right.(FunctionExpr); ok && fun.Name == FromUnixTimestampMs && len(fun.Args) == 1 {
				if rhs, ok := fun.Args[0].(LiteralExpr); ok {
					var tim time.Time
					switch v := rhs.Value.(type) {
					case time.Time:
						tim = v
					case TimeLiteral:
						tim = v.Value
					default:
						return nil
					}

					if !found || tim.Before(lowerBoundTs) {
						lowerBoundTs = tim
						found = true
					}
				}
			}
		} else if e.Op == "AND" {
			e.Left.Accept(visitor)
			e.Right.Accept(visitor)
		}
		return nil
	}

	whereClause.Accept(visitor)
	return
}
