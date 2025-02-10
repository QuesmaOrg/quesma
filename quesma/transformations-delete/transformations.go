package transformations_delete

import (
	"context"
	"fmt"
	"github.com/QuesmaOrg/quesma/quesma/clickhouse"
	"github.com/QuesmaOrg/quesma/quesma/logger"
	"github.com/QuesmaOrg/quesma/quesma/model"
	"github.com/QuesmaOrg/quesma/quesma/schema"
	"github.com/QuesmaOrg/quesma/quesma/util"
	"github.com/k0kubun/pp"
	"slices"
	"strings"
)

// Delete some time in the future. It should just use normal schema transformations.
func ApplyNecessaryTransformations(ctx context.Context, query *model.Query, table *clickhouse.Table, indexSchema schema.Schema) (*model.Query, error) {
	fmt.Println("KK TRANSF")
	pp.Println(query.SelectCommand)

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

	// we look for: [from|to]UnixTimestamp(... (most likely simply a column))
	visitor.OverrideVisitFunction = func(b *model.BaseExprVisitor, e model.FunctionExpr) interface{} {
		visitChildren := func() model.FunctionExpr {
			return model.NewFunction(e.Name, b.VisitChildren(e.Args)...)
		}

		scopeBefore := scope
		defer func() { scope = scopeBefore }()
		fmt.Println("KK f start 1", e)
		if e.Name != model.ToUnixTimestampMs && e.Name != model.FromUnixTimestampMs { // TODO
			fmt.Println("wtf, name:", e.Name)
			return visitChildren()
		}
		if len(e.Args) != 1 {
			logger.WarnWithCtx(ctx).Msgf("invalid number of arguments for %s function", e.Name)
			return visitChildren()
		}

		// usually it's simply [To/From]UnixTimestamp(timestamp_field), so we could simply e.Args[0].(ColumnRef)
		// but sometimes it's more complex expr, e.g. ToUnixTimestamp(COALESCE(...)), so using GetUsedColumns
		usedColumns := model.GetUsedColumns(e.Args[0])
		if len(usedColumns) == 1 {
			colRef := usedColumns[0]
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
			if isDatetime {
				scope = datetime
			} else if isDateTime64 {
				scope = datetime64
			}
			fmt.Println("KK f start 3", e, isDatetime, isDateTime64)
			if !isDatetime && !isDateTime64 {
				return visitChildren()
			}
		}

		var clickhouseFunc string
		switch {
		case e.Name == model.ToUnixTimestampMs && scope == datetime:
			clickhouseFunc = model.ClickhouseToUnixTimestampMsFromDatetimeFunction
		case e.Name == model.ToUnixTimestampMs && scope != datetime:
			clickhouseFunc = model.ClickhouseToUnixTimestampMsFromDatetime64Function
		case e.Name == model.FromUnixTimestampMs && scope == datetime:
			clickhouseFunc = model.ClickhouseFromUnixTimestampMsToDatetimeFunction
		case e.Name == model.FromUnixTimestampMs && scope != datetime:
			clickhouseFunc = model.ClickhouseFromUnixTimestampMsToDatetime64Function

		default:
		}

		return model.NewFunction(clickhouseFunc, b.VisitChildren(e.Args)...)
	}

	// we look for: MillisecondsLiteral
	visitor.OverrideVisitLiteral = func(b *model.BaseExprVisitor, l model.LiteralExpr) interface{} {
		pp.Println("literal scope", scope, l)
		if scope == datetime {
			if ts, isNumber := util.ExtractNumeric64Maybe(l.Value); isNumber {
				return model.NewLiteral(int64(ts / 1000))
			}
		}

		msLiteral, ok := l.Value.(model.MillisecondsLiteral)
		if !ok {
			return model.NewLiteral(l.Value)
		}

		fmt.Println("LOL", msLiteral)

		field, ok := indexSchema.ResolveField(msLiteral.TimestampField.ColumnName)
		fmt.Println("1 LOL", msLiteral, field, ok)
		if !ok {
			logger.WarnWithCtx(ctx).Msgf("field %v not found in schema for table %s", msLiteral.TimestampField, query.TableName)
			return model.NewLiteral(l.Value)
		}
		col, ok := table.Cols[field.InternalPropertyName.AsString()]
		fmt.Println("1LOL", msLiteral, col)
		if !ok {
			logger.WarnWithCtx(ctx).Msgf("field %s not found in table %s", field.InternalPropertyName.AsString(), query.TableName)
			return model.NewLiteral(l.Value)
		}

		fmt.Println("2LOL", msLiteral, col.IsDatetime())

		if col.IsDatetime() {
			ts, isNumber := util.ExtractNumeric64Maybe(msLiteral.Value)
			if !isNumber {
				logger.WarnWithCtx(ctx).Msgf("invalid argument for a timestamp: %v. isn't integer, but %T", msLiteral.Value, msLiteral.Value)
				return model.NewLiteral(msLiteral.Value)
			}
			return model.NewLiteral(int64(ts / 1000))
		}
		return model.NewLiteral(msLiteral.Value)
	}

	expr := query.SelectCommand.Accept(visitor)

	if _, ok := expr.(*model.SelectCommand); ok {
		query.SelectCommand = *expr.(*model.SelectCommand)
	}

	pp.Println(query.SelectCommand)
	return query, nil

}
