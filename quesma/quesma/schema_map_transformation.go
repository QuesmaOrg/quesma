// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package quesma

import (
	"quesma/clickhouse"
	"quesma/logger"
	"quesma/model"
	"quesma/quesma/types"
	"strings"
)

type mapTypeResolver struct {
	table *clickhouse.Table
}

type searchScope int

const (
	scopeWholeMap searchScope = iota
	scopeKeys
	scopeValues
)

func (v *mapTypeResolver) isMap(fieldName string) (exists bool, scope searchScope, columnName string) {

	//
	// This is a HACK to get the column database type from the schema
	//
	//
	suffixes := []string{types.MultifieldKeywordSuffix, types.MultifieldTextSuffix, types.MultifieldMapKeysSuffix, types.MultifieldMapValuesSuffix}
	var resultSuffix string
	for _, suffix := range suffixes {
		if strings.HasSuffix(fieldName, suffix) {
			fieldName = strings.TrimSuffix(fieldName, suffix)
			resultSuffix = suffix
		}
	}

	switch resultSuffix {
	case types.MultifieldMapKeysSuffix:
		scope = scopeKeys
	case types.MultifieldMapValuesSuffix:
		scope = scopeValues
	default:
		scope = scopeWholeMap
	}

	tableColumnName := strings.ReplaceAll(fieldName, ".", ".")
	col, ok := v.table.Cols[tableColumnName]

	if ok {
		if strings.HasPrefix(col.Type.String(), "Map") {
			return true, scope, tableColumnName
		}
	}

	return false, scope, tableColumnName
}

func existsInMap(left model.Expr, op string, mapToArrayFunction string, right model.Expr) model.Expr {
	variableName := "x"
	lambda := model.NewLambdaExpr([]string{variableName}, model.NewInfixExpr(model.NewLiteral(variableName), op, right))
	return model.NewFunction("arrayExists", lambda, model.NewFunction(mapToArrayFunction, left))
}

func NewMapTypeVisitor(resolver mapTypeResolver) model.ExprVisitor {

	visitor := model.NewBaseVisitor()

	visitor.OverrideVisitColumnRef = func(b *model.BaseExprVisitor, e model.ColumnRef) interface{} {
		isMap, _, realName := resolver.isMap(e.ColumnName)
		if !isMap {
			return e
		}

		return model.NewColumnRef(realName)
	}

	visitor.OverrideVisitInfix = func(b *model.BaseExprVisitor, e model.InfixExpr) interface{} {
		column, ok := e.Left.(model.ColumnRef)
		if ok {
			isMap, scope, _ := resolver.isMap(column.ColumnName)
			if !isMap {
				return model.NewInfixExpr(e.Left.Accept(b).(model.Expr), e.Op, e.Right.Accept(b).(model.Expr))
			}

			left := e.Left.Accept(b).(model.Expr)
			op := strings.ToUpper(e.Op)

			switch {

			case (op == "ILIKE" || op == "LIKE") && scope == scopeWholeMap:
				right := e.Right.Accept(b).(model.Expr)
				existsInKey := existsInMap(left, op, "mapKeys", right)
				existsInValue := existsInMap(left, op, "mapValues", right)
				return model.NewInfixExpr(existsInKey, "OR", existsInValue)

			case op == "=" && (scope == scopeWholeMap || scope == scopeKeys):
				return model.NewFunction("mapContains", left, e.Right.Accept(b).(model.Expr))

			case (op == "ILIKE" || op == "LIKE") && scope == scopeKeys:
				return existsInMap(left, op, "mapKeys", e.Right.Accept(b).(model.Expr))

			case (op == "ILIKE" || op == "LIKE") && scope == scopeValues:
				return existsInMap(left, op, "mapValues", e.Right.Accept(b).(model.Expr))

			case op == "=" && scope == scopeValues:
				toArray := model.NewFunction("keyValues", left)
				return model.NewFunction("has", toArray, e.Right.Accept(b).(model.Expr))

			default:
				logger.Warn().Msgf("Unhandled map infix operation  %s, column: %v, scope: %v, expr: %v", e.Op, column.ColumnName, scope, e)
			}
		}

		left := e.Left.Accept(b).(model.Expr)
		right := e.Right.Accept(b).(model.Expr)

		return model.NewInfixExpr(left, e.Op, right)
	}

	return visitor
}
