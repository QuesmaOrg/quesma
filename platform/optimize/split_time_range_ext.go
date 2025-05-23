// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package optimize

import (
	"github.com/QuesmaOrg/quesma/platform/logger"
	"github.com/QuesmaOrg/quesma/platform/model"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Context: We noticed that for some schemas (that don't ORDER BY time), the "Discover" view in Kibana
// over long time ranges can be very slow, even though it only shows 500 results. Changing the time range
// to a shorter one can make the query faster. (See this issue in ClickHouse for a similar example:
// https://github.com/ClickHouse/ClickHouse/issues/69315)
//
// This optimization therefore splits the time range into parts: a short time range, on which we bet that the query
// will be fast (and still return LIMIT many results) and a long time range, which will be used to get the rest of the
// results (in case the short time range didn't return enough results).
type splitTimeRangeExt struct{}

func (s splitTimeRangeExt) validateSelectedColumns(columns []model.Expr) bool {
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
		if aliasedExpr, ok := column.(model.AliasedExpr); ok && s.validateSelectedColumns([]model.Expr{aliasedExpr.Expr}) {
			continue
		}
		if functionExpr, ok := column.(model.FunctionExpr); ok && s.validateSelectedColumns(functionExpr.Args) {
			continue
		}

		logger.Debug().Msgf("Query not eligible for time range optimization: column at index %d is an unsupported %T", i, column)
		return false
	}
	return true
}

func (s splitTimeRangeExt) findOrderByColumn(selectCommand *model.SelectCommand) (string, model.OrderByDirection, bool) {
	if len(selectCommand.OrderBy) != 1 {
		logger.Debug().Msg("Query not eligible for time range optimization: ORDER BY longer than 1")
		return "", model.DefaultOrder, false
	}

	if orderByColumn, ok := selectCommand.OrderBy[0].Expr.(model.ColumnRef); ok {
		return orderByColumn.ColumnName, selectCommand.OrderBy[0].Direction, true
	}

	logger.Debug().Msg("Query not eligible for time range optimization: ORDER BY not a column reference")
	return "", model.DefaultOrder, false
}

func (s splitTimeRangeExt) checkAndFindTimeLimits(selectCommand *model.SelectCommand, orderByColumnName string) (*timeRangeLimit, *timeRangeLimit) {
	var lowerLimit, upperLimit *timeRangeLimit

	visitor := model.NewBaseVisitor()
	visitor.OverrideVisitInfix = func(b *model.BaseExprVisitor, e model.InfixExpr) interface{} {
		if columnName, ok := e.Left.(model.ColumnRef); ok && columnName.ColumnName == orderByColumnName {
			switch e.Op {
			case "<", "<=":
				if functionExpr, ok := e.Right.(model.FunctionExpr); ok &&
					(functionExpr.Name == "fromUnixTimestamp64Milli" || functionExpr.Name == "fromUnixTimestamp") {
					upperBoundValue := functionExpr.Args[0].(model.LiteralExpr).Value.(int64)
					upperLimit = &timeRangeLimit{value: upperBoundValue, funcName: functionExpr.Name}
				}
			case ">", ">=":
				if functionExpr, ok := e.Right.(model.FunctionExpr); ok &&
					(functionExpr.Name == "fromUnixTimestamp64Milli" || functionExpr.Name == "fromUnixTimestamp") {
					lowerBoundValue := functionExpr.Args[0].(model.LiteralExpr).Value.(int64)
					lowerLimit = &timeRangeLimit{value: lowerBoundValue, funcName: functionExpr.Name}
				}
			}
		} else if e.Op == "AND" {
			e.Left.Accept(b)
			e.Right.Accept(b)
		}
		return e
	}
	selectCommand.Accept(visitor)

	return lowerLimit, upperLimit
}

func (s splitTimeRangeExt) findTimeRange(selectCommand *model.SelectCommand) *timeRange {
	// The optimization is not possible for all queries.
	// Some of those restrictions are not strictly necessary, but added here conservatively to avoid potential issues.
	if selectCommand.Limit == 0 {
		logger.Debug().Msg("Query not eligible for time range optimization: LIMIT 0")
		return nil
	}
	if len(selectCommand.LimitBy) != 0 {
		logger.Debug().Msg("Query not eligible for time range optimization: LIMIT BY")
		return nil
	}
	if selectCommand.SampleLimit != 0 {
		logger.Debug().Msg("Query not eligible for time range optimization: SAMPLE LIMIT")
		return nil
	}
	if selectCommand.IsDistinct {
		logger.Debug().Msg("Query not eligible for time range optimization: DISTINCT")
		return nil
	}
	if selectCommand.GroupBy != nil {
		logger.Debug().Msg("Query not eligible for time range optimization: GROUP BY")
		return nil
	}
	if len(selectCommand.NamedCTEs) != 0 {
		logger.Debug().Msg("Query not eligible for time range optimization: CTEs")
		return nil
	}

	if !s.validateSelectedColumns(selectCommand.Columns) {
		return nil
	}

	orderByColumnName, direction, found := s.findOrderByColumn(selectCommand)
	if !found {
		return nil
	}
	if direction != model.DescOrder {
		logger.Debug().Msg("Query not eligible for time range optimization: ORDER BY not DESC")
		return nil
	}

	lowerLimit, upperLimit := s.checkAndFindTimeLimits(selectCommand, orderByColumnName)
	if lowerLimit == nil || upperLimit == nil {
		logger.Debug().Msg("Query not eligible for time range optimization: missing time limits (both lower and upper)")
		return nil
	}

	logger.Debug().Msgf("Query eligible for time range optimization on table '%s'", model.AsString(selectCommand.FromClause))
	return &timeRange{columnName: orderByColumnName, lowerLimit: *lowerLimit, upperLimit: *upperLimit, direction: direction}
}

func (s splitTimeRangeExt) getSplitPoints(foundTimeRange timeRange, properties map[string]string) []timeRangeLimit {
	shorterTimeRangesMinutes := defaultShorterTimeRangesMinutes
	if shorterTimeRangesMinutesStr, ok := properties["ranges"]; ok {
		shorterTimeRangesMinutesStrList := strings.Split(shorterTimeRangesMinutesStr, ",")

		shorterTimeRangesMinutes = []int64{}
		for _, shorterTimeRangeMinutesStr := range shorterTimeRangesMinutesStrList {
			parsedTimeRange, err := strconv.Atoi(shorterTimeRangeMinutesStr)
			if err != nil {
				logger.Error().Msgf("Failed to parse time range: %s", err)
			}
			shorterTimeRangesMinutes = append(shorterTimeRangesMinutes, int64(parsedTimeRange))
		}
	}

	result := []timeRangeLimit{foundTimeRange.lowerLimit}

	for _, shorterTimeRangeMinute := range shorterTimeRangesMinutes {
		var splitPoint timeRangeLimit
		if foundTimeRange.upperLimit.funcName == "fromUnixTimestamp64Milli" {
			splitPoint = timeRangeLimit{value: foundTimeRange.upperLimit.value - shorterTimeRangeMinute*time.Minute.Milliseconds(), funcName: foundTimeRange.upperLimit.funcName}
		} else {
			splitPoint = timeRangeLimit{value: foundTimeRange.upperLimit.value - shorterTimeRangeMinute*int64(time.Minute.Seconds()), funcName: foundTimeRange.upperLimit.funcName}
		}
		if splitPoint.value >= foundTimeRange.lowerLimit.value && splitPoint.value <= foundTimeRange.upperLimit.value {
			result = append(result, splitPoint)
		}
	}

	result = append(result, foundTimeRange.upperLimit)

	sort.Slice(result, func(i, j int) bool {
		return result[i].value >= result[j].value
	})
	return result
}

func (s splitTimeRangeExt) transformQuery(query *model.Query, properties map[string]string) ([]*model.Query, error) {
	var subqueries []model.SelectCommand
	foundTimeRange := s.findTimeRange(&query.SelectCommand)
	if foundTimeRange == nil {
		var queries []*model.Query
		queries = append(queries, query)
		return queries, nil
	}

	splitPoints := s.getSplitPoints(*foundTimeRange, properties)
	if len(splitPoints) <= 2 {
		var queries []*model.Query
		queries = append(queries, query)
		return queries, nil
	}

	for i := 1; i < len(splitPoints); i++ {
		subquery := query.SelectCommand

		start := splitPoints[i].value
		end := splitPoints[0].value // always ends at "now"

		startExpr := model.NewFunction(splitPoints[i].funcName, model.NewLiteral(start))
		endExpr := model.NewFunction(splitPoints[0].funcName, model.NewLiteral(end))

		whereClause := model.NewInfixExpr(
			model.NewInfixExpr(
				model.NewColumnRef(foundTimeRange.columnName), ">=", startExpr,
			),
			"AND",
			model.NewInfixExpr(
				model.NewColumnRef(foundTimeRange.columnName), "<", endExpr,
			),
		)

		subquery.WhereClause = whereClause
		subqueries = append(subqueries, subquery)
	}

	var queries []*model.Query
	for i := 0; i < len(subqueries); i++ {
		queries = append(queries, &model.Query{
			SelectCommand: subqueries[i],
		})
	}
	return queries, nil
}

func (s splitTimeRangeExt) Transform(plan *model.ExecutionPlan, properties map[string]string) (*model.ExecutionPlan, error) {

	queriesSubqueriesMapping := make(map[int][]*model.Query)
	for i, query := range plan.Queries {
		subqueries, err := s.transformQuery(query, properties)
		if err != nil {
			return nil, err
		}
		queriesSubqueriesMapping[i] = subqueries
	}

	var newQueries []*model.Query
	nextQueryId := len(plan.Queries)
	for i := range plan.Queries {
		subqueries := queriesSubqueriesMapping[i]
		plan.Queries[i].SelectCommand = subqueries[0].SelectCommand
		for j := 1; j < len(subqueries); j++ {
			newQuery := plan.Queries[0].Clone()
			newQuery.SelectCommand = subqueries[j].SelectCommand
			newQueries = append(newQueries, newQuery)
			plan.Siblings[i] = append(plan.Siblings[i], nextQueryId)
			nextQueryId++
		}
	}
	plan.Queries = append(plan.Queries, newQueries...)
	for i, subqueryPerQuery := range queriesSubqueriesMapping {
		querySQL := plan.Queries[i].SelectCommand
		logger.Info().Msgf("@@@@@@Original query: %s", querySQL.String())
		for j := 0; j < len(subqueryPerQuery); j++ {
			logger.Info().Msgf("@@@@@@Transformed query: %s", subqueryPerQuery[j].SelectCommand.String())
		}
	}

	plan.Interrupt = func(queryId int, rows []model.QueryResultRow) bool {
		const maxRows = 500
		if _, ok := plan.Siblings[queryId]; ok {
			return len(rows) >= maxRows
		}
		return false
	}
	plan.Merge = func(plan *model.ExecutionPlan, results [][]model.QueryResultRow) (*model.ExecutionPlan, [][]model.QueryResultRow) {
		for k, v := range plan.Siblings {
			for _, sibling := range v {
				// remove sibling query from the plan
				plan.Queries = append(plan.Queries[:sibling], plan.Queries[sibling+1:]...)
				// merge results of sibling query into the original query
				if len(results[k]) == 0 && len(results[sibling]) > 0 {
					results[k] = append(results[k], results[sibling]...)
				}
				if len(results[k]) > 0 && len(results[sibling]) > 0 {
					results[k] = make([]model.QueryResultRow, 0)
					results[k] = append(results[k], results[sibling]...)
				}
				// remove results of sibling query from the results
				results = append(results[:sibling], results[sibling+1:]...)
			}
		}
		plan.Siblings = make(map[int][]int)
		return plan, results
	}
	return plan, nil
}

func (s splitTimeRangeExt) Name() string {
	return "split_time_range_ext"
}

func (s splitTimeRangeExt) IsEnabledByDefault() bool {
	return true
}
