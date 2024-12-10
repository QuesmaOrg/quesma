// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package optimize

import (
	"quesma/logger"
	"quesma/model"
)

type shortenTimeRange struct{}

type timeRange struct {
	columnName string
	lowerLimit int64
	upperLimit int64
	direction  model.OrderByDirection
}

func (s shortenTimeRange) validateSelectedColumns(columns []model.Expr) bool {
	// The main purpose is to disallow window functions for which this optimization might be hard to reason about
	// (and could be invalid). The allowed Expr types are whitelisted here rather than blacklisted (window functions)
	// to be less error-prone in the future.
	for i, column := range columns {
		if _, ok := column.(model.ColumnRef); ok {
			continue
		}
		if _, ok := column.(model.LiteralExpr); ok {
			continue
		}
		if functionExpr, ok := column.(model.FunctionExpr); ok && s.validateSelectedColumns(functionExpr.Args) {
			continue
		}

		logger.Info().Msgf("Query not eligible for time range optimization: column at index %d is an unsupported %T", i, column)
		return false
	}
	return true
}

func (s shortenTimeRange) findOrderByColumn(selectCommand *model.SelectCommand) (string, model.OrderByDirection, bool) {
	if len(selectCommand.OrderBy) != 1 {
		logger.Info().Msg("Query not eligible for time range optimization: ORDER BY longer than 1")
		return "", model.DefaultOrder, false
	}

	if orderByColumn, ok := selectCommand.OrderBy[0].Expr.(model.ColumnRef); ok {
		return orderByColumn.ColumnName, selectCommand.OrderBy[0].Direction, true
	}

	logger.Info().Msg("Query not eligible for time range optimization: ORDER BY not a column reference")
	return "", model.DefaultOrder, false
}

func (s shortenTimeRange) findTimeLimits(selectCommand *model.SelectCommand, orderByColumnName string) (*int64, *int64) {
	var lowerLimit, upperLimit *int64

	visitor := model.NewBaseVisitor()
	visitor.OverrideVisitInfix = func(b *model.BaseExprVisitor, e model.InfixExpr) interface{} {
		if columnName, ok := e.Left.(model.ColumnRef); ok && columnName.ColumnName == orderByColumnName {
			switch e.Op {
			case "<", "<=":
				if functionExpr, ok := e.Right.(model.FunctionExpr); ok && functionExpr.Name == "fromUnixTimestamp64Milli" {
					upperBoundValue := functionExpr.Args[0].(model.LiteralExpr).Value.(int64)
					upperLimit = &upperBoundValue
				}
			case ">", ">=":
				if functionExpr, ok := e.Right.(model.FunctionExpr); ok && functionExpr.Name == "fromUnixTimestamp64Milli" {
					lowerBoundValue := functionExpr.Args[0].(model.LiteralExpr).Value.(int64)
					lowerLimit = &lowerBoundValue
				}
			}
		}
		e.Left.Accept(b)
		e.Right.Accept(b)
		return e
	}
	selectCommand.Accept(visitor)

	return lowerLimit, upperLimit
}

func (s shortenTimeRange) findTimeRange(selectCommand *model.SelectCommand) *timeRange {
	// The optimization is not possible for all queries.
	// Some of those restrictions are not strictly necessary, but added here conservatively to avoid potential issues.
	if selectCommand.Limit == 0 {
		logger.Info().Msg("Query not eligible for time range optimization: LIMIT 0")
		return nil
	}
	if len(selectCommand.LimitBy) != 0 {
		logger.Info().Msg("Query not eligible for time range optimization: LIMIT BY")
		return nil
	}
	if selectCommand.SampleLimit != 0 {
		logger.Info().Msg("Query not eligible for time range optimization: SAMPLE LIMIT")
		return nil
	}
	if selectCommand.IsDistinct {
		logger.Info().Msg("Query not eligible for time range optimization: DISTINCT")
		return nil
	}
	if selectCommand.GroupBy != nil {
		logger.Info().Msg("Query not eligible for time range optimization: GROUP BY")
		return nil
	}
	if len(selectCommand.NamedCTEs) != 0 {
		logger.Info().Msg("Query not eligible for time range optimization: CTEs")
		return nil
	}

	if !s.validateSelectedColumns(selectCommand.Columns) {
		return nil
	}

	orderByColumnName, direction, found := s.findOrderByColumn(selectCommand)
	if !found {
		return nil
	}

	lowerLimit, upperLimit := s.findTimeLimits(selectCommand, orderByColumnName)
	if lowerLimit == nil || upperLimit == nil {
		logger.Info().Msg("Query not eligible for time range optimization: missing time limits (both lower and upper)")
		return nil
	}

	return &timeRange{columnName: orderByColumnName, lowerLimit: *lowerLimit, upperLimit: *upperLimit, direction: direction}
}

func (s shortenTimeRange) transformQuery(query *model.Query) (*model.Query, error) {
	foundTimeRange := s.findTimeRange(&query.SelectCommand)
	if foundTimeRange == nil {
		return query, nil
	}

	if foundTimeRange.direction != model.DescOrder {
		// TODO: support other directions
		return query, nil
	}

	splitPoint := foundTimeRange.upperLimit - 60*60*1000 // TODO: better split point
	if !(splitPoint >= foundTimeRange.lowerLimit && splitPoint <= foundTimeRange.upperLimit) {
		// The time range is too short to be split
		return query, nil
	}

	shortSelectCommand := query.SelectCommand
	shortSelectCommand.WhereClause = model.NewInfixExpr(model.NewInfixExpr(model.NewColumnRef(foundTimeRange.columnName), ">=", model.NewFunction("fromUnixTimestamp64Milli", model.NewLiteral(splitPoint))), "AND", shortSelectCommand.WhereClause)

	longSelectCommand := query.SelectCommand
	longSelectCommand.WhereClause = model.NewInfixExpr(model.NewInfixExpr(model.NewColumnRef(foundTimeRange.columnName), "<", model.NewFunction("fromUnixTimestamp64Milli", model.NewLiteral(splitPoint))), "AND", longSelectCommand.WhereClause)

	unionSelectCommand := model.SelectCommand{
		IsDistinct:  false,
		Columns:     shortSelectCommand.Columns,
		FromClause:  model.NewParenExpr(model.NewInfixExpr(model.NewParenExpr(shortSelectCommand), "UNION ALL", model.NewParenExpr(longSelectCommand))),
		WhereClause: nil,
		GroupBy:     nil,
		OrderBy:     nil,
		LimitBy:     nil,
		Limit:       shortSelectCommand.Limit,
		SampleLimit: 0,
		NamedCTEs:   []*model.CTE{
			// TODO: use CTEs
			//{
			//	Name:          "shortSelectCommand",
			//	SelectCommand: &shortSelectCommand,
			//},
			//{
			//	Name:          "longSelectCommand",
			//	SelectCommand: &longSelectCommand,
			//},
		},
	}

	query.SelectCommand = unionSelectCommand

	return query, nil
}

func (s shortenTimeRange) Transform(queries []*model.Query, properties map[string]string) ([]*model.Query, error) {
	for i, query := range queries {
		transformedQuery, err := s.transformQuery(query)
		if err != nil {
			return nil, err
		}
		queries[i] = transformedQuery
	}
	return queries, nil
}

func (s shortenTimeRange) Name() string {
	return "shorten_time_range"
}

func (s shortenTimeRange) IsEnabledByDefault() bool {
	return true
}
