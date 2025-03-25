// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package transformations_delete

import (
	"context"
	"fmt"
	"github.com/QuesmaOrg/quesma/platform/clickhouse"
	"github.com/QuesmaOrg/quesma/platform/logger"
	"github.com/QuesmaOrg/quesma/platform/model"
	"github.com/QuesmaOrg/quesma/platform/schema"
	"github.com/QuesmaOrg/quesma/platform/util"
	"github.com/k0kubun/pp"
	"slices"
	"strings"
)

func ApplyNecessaryTransformations(ctx context.Context, query *model.Query, table *clickhouse.Table, indexSchema schema.Schema) (*model.Query, error) {

	type scopeType = int
	const (
		datetime scopeType = iota
		datetime64
		none
	)
	scope := none

	visitor := model.NewBaseVisitor()

	// we look for: (timestamp_field OP fromUnixTimestamp)
	visitor.OverrideVisitInfix = func(b *model.BaseExprVisitor, e model.InfixExpr) interface{} {
		visitChildren := func() model.InfixExpr {
			return model.NewInfixExpr(e.Left.Accept(b).(model.Expr), e.Op, e.Right.Accept(b).(model.Expr))
		}

		fmt.Println("KK start 1", e)

		// check if timestamp_field is ok
		colRef, ok := e.Left.(model.ColumnRef)
		fmt.Println("KK start 2", colRef, ok)
		if !ok {
			return visitChildren()
		}
		field, ok := indexSchema.ResolveField(colRef.ColumnName)
		fmt.Println("KK start 3", field, ok)
		if !ok {
			logger.WarnWithCtx(ctx).Msgf("field %s not found in schema for table %s", colRef.ColumnName, query.TableName)
			return visitChildren()
		}
		col, ok := table.Cols[field.InternalPropertyName.AsString()]
		if !ok {
			logger.WarnWithCtx(ctx).Msgf("field %s not found in table %s", field.InternalPropertyName.AsString(), query.TableName)
			return visitChildren()
		}
		fmt.Println("KK start 3", e, col, ok)
		isDatetime := col.IsDatetime()
		isDateTime64 := col.IsDatetime64()
		fmt.Println("KK start 4", isDatetime, isDateTime64)
		if !isDatetime && !isDateTime64 {
			return visitChildren()
		}

		// check if operator is ok
		op := strings.TrimSpace(e.Op)
		fmt.Println("KK start 5", op)
		if !slices.Contains([]string{"=", "!=", ">", "<", ">=", "<=", "/"}, op) {
			return visitChildren()
		}

		// check if right side is a function we want
		tsFunc, ok := e.Right.(model.FunctionExpr)
		if !ok {
			fmt.Println("koniec")
			return visitChildren()
		}
		if tsFunc.Name != model.FromUnixTimestampMs && tsFunc.Name != model.ToUnixTimestampMs {
			//fmt.Println("wtf, name:", tsFunc.Name)
			return visitChildren()
		}
		if len(tsFunc.Args) != 1 {
			logger.WarnWithCtx(ctx).Msgf("invalid number of arguments for %s function", tsFunc.Name)
			return visitChildren()
		}

		arg := tsFunc.Args[0].Accept(b).(model.Expr)
		pp.Println("KK 74 ARG", tsFunc.Args[0], arg)
		if isDateTime64 {
			clickhouseFunc := model.ClickhouseFromUnixTimestampMsToDatetime64Function
			return model.NewInfixExpr(colRef, e.Op, model.NewFunction(clickhouseFunc, arg))
		} else if isDatetime {
			fmt.Println("KK 79l", arg)
			tsAny, isLiteral := arg.(model.LiteralExpr)
			if !isLiteral {
				logger.WarnWithCtx(ctx).Msgf("invalid argument for %s function: %v. isn't literal, but %T", tsFunc.Name, arg, arg)
				return visitChildren()
			}
			ts, isNumber := util.ExtractNumeric64Maybe(tsAny.Value)
			if !isNumber {
				logger.WarnWithCtx(ctx).Msgf("invalid argument for %s function: %v. isn't integer, but %T", tsFunc.Name, arg, arg)
				return visitChildren()
			}

			clickhouseFunc := model.ClickhouseFromUnixTimestampMsToDatetimeFunction
			return model.NewInfixExpr(colRef, e.Op, model.NewFunction(clickhouseFunc, model.NewLiteral(int64(ts/1000))))
		}

		return visitChildren() // unreachable
	}

	// we look for: toUnixTimestamp(timestamp_field) or fromUnixTimestamp(TimeLiteral)
	visitor.OverrideVisitFunction = func(b *model.BaseExprVisitor, e model.FunctionExpr) interface{} {
		visitChildren := func() model.FunctionExpr {
			return model.NewFunction(e.Name, b.VisitChildren(e.Args)...)
		}

		scopeBefore := scope
		defer func() { scope = scopeBefore }()

		toUnix := func() interface{} {
			if len(e.Args) != 1 {
				logger.WarnWithCtx(ctx).Msgf("invalid number of arguments for %s function", e.Name)
				return visitChildren()
			}
			colRef, ok := e.Args[0].(model.ColumnRef)
			fmt.Printf("KK colref %v ok %v\n", colRef, ok)
			if !ok {
				if f, ok := e.Args[0].(model.FunctionExpr); ok && strings.ToLower(f.Name) == "coalesce" && len(f.Args) > 1 {
					colRef, ok = f.Args[0].(model.ColumnRef)
					if !ok {
						logger.WarnWithCtx(ctx).Msgf("invalid argument for %s function: %v. isn't column reference, but %T", e.Name, f.Args[0], f.Args[0])
						return visitChildren()
					}
				}
			}
			fmt.Println("KK f start 2", e, colRef)
			field, ok := indexSchema.ResolveField(colRef.ColumnName)
			fmt.Println("KK f start 2.5", field, ok)
			if !ok {
				logger.WarnWithCtx(ctx).Msgf("field %s not found in schema for table %s", colRef.ColumnName, query.TableName)
				return visitChildren()
			}
			col, ok := table.Cols[field.InternalPropertyName.AsString()]
			if !ok {
				logger.WarnWithCtx(ctx).Msgf("field %s not found in table %s", field.InternalPropertyName.AsString(), query.TableName)
				return visitChildren()
			}
			isDatetime := col.IsDatetime()
			isDateTime64 := col.IsDatetime64()
			fmt.Println("KK f start 3", e, isDatetime, isDateTime64)
			if !isDatetime && !isDateTime64 {
				return visitChildren()
			}

			var clickhouseFunc string
			if isDateTime64 {
				scope = datetime64
				clickhouseFunc = model.ClickhouseToUnixTimestampMsFromDatetime64Function
			} else if isDatetime {
				scope = datetime
				clickhouseFunc = model.ClickhouseToUnixTimestampMsFromDatetimeFunction
			}

			return model.NewFunction(clickhouseFunc, b.VisitChildren(e.Args)...)
		}

		fromUnix := func() interface{} {
			if len(e.Args) != 1 {
				logger.WarnWithCtx(ctx).Msgf("invalid number of arguments for %s function", e.Name)
				return visitChildren()
			}

			children := b.VisitChildren(e.Args)
			var clickhouseFunc string
			switch scope {
			case datetime:
				clickhouseFunc = model.ClickhouseFromUnixTimestampMsToDatetimeFunction
			default:
				pp.Println("Children", children)
				/*threeDigitsOfPrecisionSuffice := utcTs.UnixNano()%1_000_000 == 0
				if threeDigitsOfPrecisionSuffice {
					return model.NewFunction("fromUnixTimestamp64Milli", model.NewLiteral(utcTs.UnixMilli())), true
				} else {
					return model.NewFunction(
						"toDateTime64",
						model.NewInfixExpr(
							model.NewLiteral(utcTs.UnixNano()),
							"/",
							model.NewLiteral(1_000_000_000),
						),
						model.NewLiteral(9),
					), true
				}*/
				clickhouseFunc = model.ClickhouseFromUnixTimestampMsToDatetime64Function
			}

			return model.NewFunction(clickhouseFunc, b.VisitChildren(e.Args)...)
		}

		switch e.Name {
		case model.ToUnixTimestampMs:
			fmt.Println("KK f START ToUnix", e)
			return toUnix()
		case model.FromUnixTimestampMs:
			fmt.Println("KK f START FromUnix", e)
			return fromUnix()
		default:
			fmt.Println("wtf, name:", e.Name)
			return visitChildren()
		}
	}

	// we look for: DurationLiteral/TimeLiteral
	visitor.OverrideVisitLiteral = func(b *model.BaseExprVisitor, l model.LiteralExpr) interface{} {
		pp.Println("visitor literal", l)
		if timeL, ok := l.Value.(model.TimeLiteral); ok {
			ts := timeL.Value
			fmt.Println("eee", ts, scope)
			switch scope {
			case datetime:
				return model.NewLiteral(ts.Unix())
			default:
				threeDigitsOfPrecisionSuffice := ts.UnixNano()%1_000_000 == 0
				fmt.Println("three?", threeDigitsOfPrecisionSuffice)
				if threeDigitsOfPrecisionSuffice {
					return model.NewLiteral(ts.UnixMilli())
				} else {
					return model.NewFunction(
						"toDateTime64",
						model.NewInfixExpr(
							model.NewLiteral(ts.UnixNano()),
							"/",
							model.NewLiteral(1_000_000_000),
						),
						model.NewLiteral(9),
					)
				}
			}
		} else {
			fmt.Println(l.Value)
		}

		msLiteral, ok := l.Value.(model.DurationLiteral)
		if !ok {
			return l.Clone()
		}

		fmt.Println("LOL", msLiteral)

		field, ok := indexSchema.ResolveField(msLiteral.TimestampField.ColumnName)
		fmt.Println("1 LOL", msLiteral, field, ok)
		if !ok {
			logger.WarnWithCtx(ctx).Msgf("field %v not found in schema for table %s", msLiteral.TimestampField, query.TableName)
			return l.Clone()
		}
		col, ok := table.Cols[field.InternalPropertyName.AsString()]
		fmt.Println("1LOL", msLiteral, col)
		if !ok {
			logger.WarnWithCtx(ctx).Msgf("field %s not found in table %s", field.InternalPropertyName.AsString(), query.TableName)
			return l.Clone()
		}

		fmt.Println("2LOL", msLiteral, col.IsDatetime())

		if col.IsDatetime() {
			return model.NewLiteral(msLiteral.Value.Milliseconds() / 1000)
		}
		return model.NewLiteral(msLiteral.Value.Milliseconds())
	}

	expr := query.SelectCommand.Accept(visitor)

	if _, ok := expr.(*model.SelectCommand); ok {
		query.SelectCommand = *expr.(*model.SelectCommand)
	}
	return query, nil

}
