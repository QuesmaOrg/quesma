// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package model

// FindLowerBounds returns x if there is "x>=y" or "x>y" in the WHERE clause, but only as a single top-level expression.
// (I mean by that a>=0 is fine, a>=0 AND expr2 [AND ...]] is also fine (AND between all), but a>=0 OR e2 is not fine.
// a>=0 AND (expr2 OR expr3) is also fine, as on top level it's only an AND.
// We achieve that by only descending for AND operators.
//
// TODO: add upper bound here too, when bucket_nr=1 in auto_date_histogram (only use case of this function), it's not needed.
func FindLowerBounds(expr Expr) []InfixExpr {
	if expr == nil {
		return []InfixExpr{}
	}

	lowerBounds := make([]InfixExpr, 0)
	visitor := NewBaseVisitor()
	visitor.OverrideVisitInfix = func(visitor *BaseExprVisitor, e InfixExpr) interface{} {
		if e.Op == ">=" || e.Op == ">" {
			lowerBounds = append(lowerBounds, e)
		} else if e.Op == "AND" {
			e.Left.Accept(visitor)
			e.Right.Accept(visitor)
		}
		return nil
	}

	expr.Accept(visitor)
	return lowerBounds
}
