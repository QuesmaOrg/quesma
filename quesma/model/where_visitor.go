// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package model

// FindLowerBounds returns x if there is "x>=y" or "x>y" in the WHERE clause, but only as a single top-level expression.
// (I mean by that a>=0 is fine, a>=0 AND e2 [AND ...]] is also fine, but a>=0 OR e2 is not fine.)
// We achieve that by only descending for AND operators.
func FindLowerBounds(expr Expr) ([]InfixExpr, bool) {
	if expr == nil {
		return []InfixExpr{}, false
	}

	candidates := make([]InfixExpr, 0)
	visitor := NewBaseVisitor()
	visitor.OverrideVisitInfix = func(visitor *BaseExprVisitor, e InfixExpr) interface{} {
		if e.Op == ">=" || e.Op == ">" {
			candidates = append(candidates, e)
		} else if e.Op == "AND" {
			e.Left.Accept(visitor)
			e.Right.Accept(visitor)
		}
		return nil
	}

	expr.Accept(visitor)
	if len(candidates) >= 1 {
		return candidates, true
	}
	return []InfixExpr{}, false
}