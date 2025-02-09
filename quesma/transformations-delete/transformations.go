package transformations_delete

import (
	"context"
	"fmt"
	"github.com/QuesmaOrg/quesma/quesma/clickhouse"
	"github.com/QuesmaOrg/quesma/quesma/logger"
	"github.com/QuesmaOrg/quesma/quesma/model"
	"github.com/QuesmaOrg/quesma/quesma/schema"
	"github.com/QuesmaOrg/quesma/quesma/util"
	"slices"
	"strings"
)

// Delete some time in the future. It should just use normal schema transformations.
func ApplyNecessaryTransformations(ctx context.Context, table *clickhouse.Table, tableName string, indexSchema schema.Schema, query *model.Query) (*model.Query, error) {
	visitor := model.NewBaseVisitor()
	visitor.OverrideVisitInfix = func(b *model.BaseExprVisitor, e model.InfixExpr) interface{} {
		visitChildren := func() model.InfixExpr {
			return model.NewInfixExpr(e.Left.Accept(b).(model.Expr), e.Op, e.Right.Accept(b).(model.Expr))
		}
		// we look for: timestamp_field OP from/toUnixTimestamp...

		/*
			table := lm.FindTable(tableName)
			fmt.Println("table", table, "name:", query.TableName)
			if table == nil {
				logger.WarnWithCtx(ctx).Msgf("table %s not found", query.TableName)
				return visitChildren()
			}

		*/

		// check if timestamp_field is ok
		col, ok := e.Left.(model.ColumnRef)
		fmt.Println("KKK col", col, ok, "e.Left:", e.Left)
		if !ok {
			return visitChildren()
		}
		field, ok := indexSchema.ResolveField(col.ColumnName)
		fmt.Println("KKK field", field, "name:", col.ColumnName)
		if !ok {
			logger.WarnWithCtx(ctx).Msgf("field %s not found in schema for table %s", col.ColumnName, query.TableName)
			return visitChildren()
		}
		isDatetime := table.Cols[field.InternalPropertyName.AsString()].IsDatetime()
		isDateTime64 := table.Cols[field.InternalPropertyName.AsString()].IsDatetime64()
		if !isDatetime && !isDateTime64 {
			return visitChildren()
		}

		// check if operator is ok
		op := strings.TrimSpace(e.Op)
		if !slices.Contains([]string{"=", "!=", ">", "<", ">=", "<="}, op) {
			return visitChildren()
		}

		// check if right side is a function we want
		tsFunc, ok := e.Right.(model.FunctionExpr)
		if !ok {
			return visitChildren()
		}
		if tsFunc.Name != model.FromUnixTimestampMs && tsFunc.Name != model.ToUnixTimestampMs {
			return visitChildren()
		}
		if len(tsFunc.Args) != 1 {
			logger.WarnWithCtx(ctx).Msgf("invalid number of arguments for %s function", tsFunc.Name)
			return visitChildren()
		}

		arg := tsFunc.Args[0].Accept(b).(model.Expr)
		if isDateTime64 {
			return model.NewInfixExpr(col, e.Op,
				model.NewFunction(model.ClickhouseFromUnixTimestampMsToDatetime64Function, arg),
			)
		} else if isDatetime {
			tsAny, isLiteral := arg.(model.LiteralExpr)
			if !isLiteral {
				logger.WarnWithCtx(ctx).Msgf("invalid argument for %s function: %v. isn't literal, but %T", tsFunc.Name, arg, arg)
				return visitChildren()
			}
			ts, err := util.ExtractInt64(tsAny.Value)
			if err != nil {
				logger.WarnWithCtx(ctx).Msgf("invalid argument for %s function: %v. isn't integer, but %T", tsFunc.Name, arg, arg)
				return visitChildren()
			}

			return model.NewInfixExpr(col, e.Op,
				model.NewFunction(model.ClickhouseFromUnixTimestampMsToDatetimeFunction, model.NewLiteral(ts/1000)),
			)
		}

		return visitChildren() // unreachable
	}
	expr := query.SelectCommand.Accept(visitor)

	if _, ok := expr.(*model.SelectCommand); ok {
		query.SelectCommand = *expr.(*model.SelectCommand)
	}
	return query, nil
}
