// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package model

// ExtractColRef returns the ColumnRef from an expression, or false if the expression is not a ColumnRef.
func ExtractColRef(e Expr) (c ColumnRef, ok bool) {
	if c, ok = e.(ColumnRef); ok {
		return c, true
	}
	return ColumnRef{}, false
}

// ToLiteralValue returns the value of a LiteralExpr, or false if the expression is not a LiteralExpr.
func ToLiteralValue(e Expr) (v any, ok bool) {
	if l, ok_ := e.(LiteralExpr); ok_ {
		return l.Value, true
	}
	return nil, false
}
