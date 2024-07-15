// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package model

// TODO OKAY THIS NEEDS TO BE FIXED FOR THE NEW WHERE STATEMENT
type usedColumns struct{}

func GetUsedColumns(expr Expr) []ColumnRef {

	var usedColumns []ColumnRef

	visitor := NewBaseVisitor()

	visitor.Overrides.VisitColumnRef = func(visitor *BaseExprVisitor, e ColumnRef) interface{} {
		usedColumns = append(usedColumns, e)
		return e
	}

	expr.Accept(visitor)

	return usedColumns
}
