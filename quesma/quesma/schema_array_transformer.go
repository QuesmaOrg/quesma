// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package quesma

import (
	"quesma/logger"
	"quesma/model"
	"quesma/schema"
	"strings"
)

//
//
// Do not use `arrayJoin` here. It's considered harmful.
//
//
//

type arrayTypeResolver struct {
	indexSchema schema.Schema
}

func (v *arrayTypeResolver) dbColumnType(columName string) string {

	//
	// This is a HACK to get the column database type from the schema
	//
	//
	// here we should resolve field by column name not field name
	columName = strings.TrimSuffix(columName, ".keyword")

	field, ok := v.indexSchema.ResolveFieldByInternalName(columName)

	if !ok {
		return ""
	}

	return field.InternalPropertyType
}

func NewArrayTypeVisitor(resolver arrayTypeResolver) model.ExprVisitor {

	visitor := model.NewBaseVisitor()

	visitor.OverrideVisitInfix = func(b *model.BaseExprVisitor, e model.InfixExpr) interface{} {

		column, ok := e.Left.(model.ColumnRef)
		if ok {
			dbType := resolver.dbColumnType(column.ColumnName)
			if strings.HasPrefix(dbType, "Array") {
				op := strings.ToUpper(e.Op)
				op = strings.TrimSpace(op)
				switch {
				case (op == "ILIKE" || op == "LIKE") && dbType == "Array(String)":

					variableName := "x"
					lambda := model.NewLambdaExpr([]string{variableName}, model.NewInfixExpr(model.NewLiteral(variableName), op, e.Right.Accept(b).(model.Expr)))
					return model.NewFunction("arrayExists", lambda, e.Left)

				case op == "=":
					return model.NewFunction("has", e.Left, e.Right.Accept(b).(model.Expr))

				default:
					logger.Error().Msgf("Unhandled array infix operation '%s', column '%v' ('%v')", e.Op, column.ColumnName, dbType)
				}
			}
		}

		left := e.Left.Accept(b).(model.Expr)
		right := e.Right.Accept(b).(model.Expr)

		return model.NewInfixExpr(left, e.Op, right)

	}

	visitor.OverrideVisitFunction = func(b *model.BaseExprVisitor, e model.FunctionExpr) interface{} {

		if len(e.Args) > 0 {
			arg := e.Args[0]
			column, ok := arg.(model.ColumnRef)
			if ok {
				dbType := resolver.dbColumnType(column.ColumnName)
				if strings.HasPrefix(dbType, "Array") {
					if strings.HasPrefix(e.Name, "sum") {
						// here we apply -Array combinator to the sum function
						// https://clickhouse.com/docs/en/sql-reference/aggregate-functions/combinators#-array
						//
						// TODO this can be rewritten to transform all aggregate functions as well
						//
						e.Name = strings.ReplaceAll(e.Name, "sum", "sumArray")
					} else {
						logger.Error().Msgf("Unhandled array function %s, column %v (%v)", e.Name, column.ColumnName, dbType)
					}
				}
			}
		}

		args := b.VisitChildren(e.Args)
		return model.NewFunction(e.Name, args...)
	}
	return visitor
}

func checkIfGroupingByArrayColumn(selectCommand model.SelectCommand, resolver arrayTypeResolver) bool {

	isArrayColumn := func(e model.Expr) bool {
		columnIsArray := false
		findArrayColumn := model.NewBaseVisitor()

		findArrayColumn.OverrideVisitColumnRef = func(b *model.BaseExprVisitor, e model.ColumnRef) interface{} {
			dbType := resolver.dbColumnType(e.ColumnName)
			if strings.HasPrefix(dbType, "Array") {
				columnIsArray = true
			}
			return e
		}

		e.Accept(findArrayColumn)

		return columnIsArray
	}

	visitor := model.NewBaseVisitor()

	var found bool

	visitor.OverrideVisitSelectCommand = func(b *model.BaseExprVisitor, e model.SelectCommand) interface{} {

		for _, expr := range e.GroupBy {

			if isArrayColumn(expr) {
				found = true
			}
		}

		for _, expr := range e.Columns {
			expr.Accept(b)
		}

		if e.FromClause != nil {
			e.FromClause.Accept(b)
		}

		for _, cte := range e.NamedCTEs {
			cte.Accept(b)
		}

		return &e
	}

	visitor.OverrideVisitFunction = func(b *model.BaseExprVisitor, e model.FunctionExpr) interface{} {

		if strings.HasPrefix(e.Name, "sum") || strings.HasPrefix(e.Name, "count") {

			if len(e.Args) > 0 {
				arg := e.Args[0]

				if isArrayColumn(arg) {
					found = true
				}

			}

		}
		return e
	}

	selectCommand.Accept(visitor)

	return found
}

func NewArrayJoinVisitor(resolver arrayTypeResolver) model.ExprVisitor {

	visitor := model.NewBaseVisitor()

	visitor.OverrideVisitColumnRef = func(b *model.BaseExprVisitor, e model.ColumnRef) interface{} {
		dbType := resolver.dbColumnType(e.ColumnName)
		if strings.HasPrefix(dbType, "Array") {
			return model.NewFunction("arrayJoin", e)
		}
		return e
	}

	return visitor
}
