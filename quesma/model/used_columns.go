// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package model

func GetUsedColumns(expr Expr) []ColumnRef {

	var usedColumns []ColumnRef

	visitor := NewBaseVisitor()

	visitor.OverrideVisitColumnRef = func(visitor *BaseExprVisitor, e ColumnRef) interface{} {
		usedColumns = append(usedColumns, e)
		return e
	}

	expr.Accept(visitor)

	return usedColumns
}
