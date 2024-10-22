package model

// TODO: it's not 100% full/proper implementation, but works in the client case
func FindTimestampLowerBound(expr Expr) (InfixExpr, bool) {
	candidates := make([]InfixExpr, 0)
	visitor := NewBaseVisitor()
	visitor.OverrideVisitInfix = func(visitor *BaseExprVisitor, e InfixExpr) interface{} {
		if e.Op == ">=" {
			candidates = append(candidates, e)
		} else if e.Op == "AND" {
			e.Left.Accept(visitor)
			e.Right.Accept(visitor)
		}
		return nil
	}

	expr.Accept(visitor)
	if len(candidates) == 1 {
		return candidates[0], true
	}
	return InfixExpr{}, false
}
