// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package quesma

import (
	"quesma/clickhouse"
	"quesma/logger"
	"quesma/model"
	"strings"
)

type mapTypeResolver struct {
	table *clickhouse.Table
}

func (v *mapTypeResolver) dbColumnType(fieldName string) string {

	//
	// This is a HACK to get the column database type from the schema
	//
	//
	fieldName = strings.TrimSuffix(fieldName, ".keyword")

	tableColumnName := strings.ReplaceAll(fieldName, ".", "::")
	col, ok := v.table.Cols[tableColumnName]
	if ok {
		return col.Type.String()
	}

	return ""
}

func NewMapTypeVisitor(resolver mapTypeResolver) model.ExprVisitor {

	visitor := model.NewBaseVisitor()

	visitor.OverrideVisitInfix = func(b *model.BaseExprVisitor, e model.InfixExpr) interface{} {

		column, ok := e.Left.(model.ColumnRef)
		if ok {
			dbType := resolver.dbColumnType(column.ColumnName)

			if strings.HasPrefix(dbType, "Unknown(Map") {

				op := strings.ToUpper(e.Op)

				switch {

				case (op == "ILIKE" || op == "LIKE") && dbType == "Unknown(Map(String, String))":

					variableName := "x"
					lambda := model.NewLambdaExpr([]string{variableName}, model.NewInfixExpr(model.NewLiteral(variableName), op, e.Right.Accept(b).(model.Expr)))
					existsInKey := model.NewFunction("arrayExists", lambda, model.NewFunction("mapKeys", e.Left))
					existsInValue := model.NewFunction("arrayExists", lambda, model.NewFunction("mapValues", e.Left))
					return model.NewInfixExpr(existsInKey, "OR", existsInValue)

				case e.Op == "=":
					return model.NewFunction("has", e.Left, e.Right.Accept(b).(model.Expr))

				default:
					logger.Warn().Msgf("Unhandled array infix operation  %s, column %v (%v)", e.Op, column.ColumnName, dbType)
				}
			}
		}

		left := e.Left.Accept(b).(model.Expr)
		right := e.Right.Accept(b).(model.Expr)

		return model.NewInfixExpr(left, e.Op, right)

	}

	return visitor
}
