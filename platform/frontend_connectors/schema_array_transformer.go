// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package frontend_connectors

import (
	"github.com/QuesmaOrg/quesma/platform/logger"
	"github.com/QuesmaOrg/quesma/platform/model"
	"github.com/QuesmaOrg/quesma/platform/schema"
	"strings"
)

//
//
// Do not use `arrayJoin` here. It's considered harmful.
//
//
//

type functionWithCombinator struct {
	baseFunctionName string
	isArray          bool
	isIf             bool
	isOrNull         bool
	isState          bool
	isMerge          bool
}

func (f functionWithCombinator) String() string {
	result := f.baseFunctionName
	if f.isArray {
		result = result + "Array"
	}
	if f.isIf {
		result = result + "If"
	}
	if f.isOrNull {
		result = result + "OrNull"
	}
	if f.isState {
		result = result + "State"
	}
	if f.isMerge {
		result = result + "Merge"
	}
	return result
}

func parseFunctionWithCombinator(funcName string) (result functionWithCombinator) {
	stripSuffix := func(s string, suffix string) (string, bool) {
		if strings.HasSuffix(s, suffix) {
			return strings.TrimSuffix(s, suffix), true
		}
		return s, false
	}

	result.baseFunctionName = funcName
	result.baseFunctionName, result.isState = stripSuffix(result.baseFunctionName, "State")
	result.baseFunctionName, result.isMerge = stripSuffix(result.baseFunctionName, "Merge")
	result.baseFunctionName, result.isIf = stripSuffix(result.baseFunctionName, "If")
	result.baseFunctionName, result.isOrNull = stripSuffix(result.baseFunctionName, "OrNull")

	return result
}

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

	var childGotArrayFunc bool
	visitor.OverrideVisitFunction = func(b *model.BaseExprVisitor, e model.FunctionExpr) interface{} {

		if len(e.Args) > 0 {
			arg := e.Args[0]
			column, ok := arg.(model.ColumnRef)
			if ok {
				dbType := resolver.dbColumnType(column.ColumnName)
				if strings.HasPrefix(dbType, "Array") {
					funcParsed := parseFunctionWithCombinator(e.Name)
					funcParsed.isArray = true
					childGotArrayFunc = true
					e.Name = funcParsed.String()
				}
			} else {
				e.Args = b.VisitChildren(e.Args)
			}
		}

		return model.NewFunction(e.Name, e.Args...)
	}

	visitor.OverrideVisitWindowFunction = func(b *model.BaseExprVisitor, e model.WindowFunction) interface{} {
		childGotArrayFunc = false
		args := b.VisitChildren(e.Args)
		if childGotArrayFunc {
			funcParsed := parseFunctionWithCombinator(e.Name)
			funcParsed.isArray = true
			e.Name = funcParsed.String()
		}
		return model.NewWindowFunction(e.Name, args, e.PartitionBy, e.OrderBy)
	}

	visitor.OverrideVisitColumnRef = func(b *model.BaseExprVisitor, e model.ColumnRef) interface{} {
		dbType := resolver.dbColumnType(e.ColumnName)
		if strings.HasPrefix(dbType, "Array") {
			logger.Error().Msgf("Unhandled array column ref %v (%v)", e.ColumnName, dbType)
		}
		return e
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

	selectCommand.Accept(visitor)

	return found
}

func NewArrayJoinVisitor(resolver arrayTypeResolver) model.ExprVisitor {

	visitor := model.NewBaseVisitor()

	visitor.OverrideVisitColumnRef = func(b *model.BaseExprVisitor, e model.ColumnRef) interface{} {
		dbType := resolver.dbColumnType(e.ColumnName)
		if strings.HasPrefix(dbType, "Array") {
			return model.NewFunction("arrayJoin", model.NewFunction("arrayDistinct", e))
		}
		return e
	}

	return visitor
}
