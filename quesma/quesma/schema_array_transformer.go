// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package quesma

import (
	"fmt"
	"quesma/clickhouse"
	"quesma/logger"
	"quesma/model"
	"strings"
)

//
//
// Do not use `arrayJoin` here. It's considered harmful.
//
//
//

type arrayTypeResolver struct {
	table *clickhouse.Table
}

func (v *arrayTypeResolver) dbColumnType(fieldName string) string {

	//
	// This is a HACK to get the column database type from the schema
	//
	//
	fieldName = strings.TrimSuffix(fieldName, ".keyword")

	tableColumnName := strings.ReplaceAll(fieldName, ".", ".")
	col, ok := v.table.Cols[tableColumnName]
	if ok {
		return col.Type.String()
	}

	return ""
}

func NewArrayTypeVisitor(resolver arrayTypeResolver) model.ExprVisitor {

	visitor := model.NewBaseVisitor()

	visitor.OverrideVisitInfix = func(b *model.BaseExprVisitor, e model.InfixExpr) interface{} {

		column, ok := e.Left.(model.ColumnRef)
		if ok {
			dbType := resolver.dbColumnType(column.ColumnName)

			if strings.HasPrefix(dbType, "Array") {

				op := strings.ToUpper(e.Op)

				switch {

				case (op == "ILIKE" || op == "LIKE") && dbType == "Array(String)":

					variableName := "x"
					lambda := model.NewLambdaExpr([]string{variableName}, model.NewInfixExpr(model.NewLiteral(variableName), op, e.Right.Accept(b).(model.Expr)))
					return model.NewFunction("arrayExists", lambda, e.Left)

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

	visitor.OverrideVisitFunction = func(b *model.BaseExprVisitor, e model.FunctionExpr) interface{} {
		if len(e.Args) == 1 {
			arg := e.Args[0]
			column, ok := arg.(model.ColumnRef)
			if ok {
				dbType := resolver.dbColumnType(column.ColumnName)
				if strings.HasPrefix(dbType, "Array") {
					switch {

					case e.Name == "sumOrNull" && dbType == "Array(Int64)":
						fnName := model.LiteralExpr{Value: fmt.Sprintf("'%s'", e.Name)}
						wrapped := model.NewFunction("arrayReduce", fnName, column)
						wrapped = model.NewFunction(e.Name, wrapped)
						return wrapped

					default:
						logger.Warn().Msgf("Unhandled array function %s, column %v (%v)", e.Name, column.ColumnName, dbType)

					}
				}
			}
		}

		args := b.VisitChildren(e.Args)
		return model.NewFunction(e.Name, args...)
	}
	return visitor
}
