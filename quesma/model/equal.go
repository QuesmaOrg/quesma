// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package model

// Check if two expressions are equal, ignores aliases
// Partly implemented, can return false even if it should be equal
func PartlyImplementedIsEqual(a, b Expr) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}

	// Remove aliases
	if alias, ok := a.(AliasedExpr); ok {
		return PartlyImplementedIsEqual(alias.Expr, b)
	}
	if alias, ok := b.(AliasedExpr); ok {
		return PartlyImplementedIsEqual(a, alias.Expr)
	}

	switch aTyped := a.(type) {
	case ColumnRef:
		if bTyped, ok := b.(ColumnRef); ok {
			return aTyped.ColumnName == bTyped.ColumnName
		}
	case LiteralExpr:
		if bTyped, ok := b.(LiteralExpr); ok {
			return aTyped.Value == bTyped.Value
		}
	case InfixExpr:
		if bTyped, ok := b.(InfixExpr); ok {
			return aTyped.Op == bTyped.Op && PartlyImplementedIsEqual(aTyped.Left, bTyped.Left) &&
				PartlyImplementedIsEqual(aTyped.Right, bTyped.Right)
		}
	case FunctionExpr:
		if bTyped, ok := b.(FunctionExpr); ok {
			if aTyped.Name != bTyped.Name || len(aTyped.Args) != len(bTyped.Args) {
				return false
			}
			for i := range aTyped.Args {
				if !PartlyImplementedIsEqual(aTyped.Args[i], bTyped.Args[i]) {
					return false
				}
			}
			return true
		}
	case ParenExpr:
		if bTyped, ok := b.(ParenExpr); ok {
			if len(aTyped.Exprs) != len(bTyped.Exprs) {
				return false
			}
			for i := range aTyped.Exprs {
				if !PartlyImplementedIsEqual(aTyped.Exprs[i], bTyped.Exprs[i]) {
					return false
				}
			}
			return true
		}
	}
	return false
}
