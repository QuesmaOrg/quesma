// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package queryparser

import (
	"errors"
	"fmt"
	"quesma/clickhouse"
	"quesma/model"
	"strconv"
)

type pancakeSqlQueryGenerator struct {
}

func (p *pancakeSqlQueryGenerator) newQuotedLiteral(value string) model.LiteralExpr {
	return model.LiteralExpr{Value: strconv.Quote(value)}
}

func (p *pancakeSqlQueryGenerator) aliasedExprArrayToExpr(aliasedExprs []model.AliasedExpr) []model.Expr {
	exprs := make([]model.Expr, 0, len(aliasedExprs))
	for _, aliasedExpr := range aliasedExprs {
		exprs = append(exprs, aliasedExpr)
	}
	return exprs
}

func (p *pancakeSqlQueryGenerator) aliasedExprArrayToLiteralExpr(aliasedExprs []model.AliasedExpr) []model.Expr {
	exprs := make([]model.Expr, 0, len(aliasedExprs))
	for _, aliasedExpr := range aliasedExprs {
		exprs = append(exprs, p.newQuotedLiteral(aliasedExpr.Alias))
	}
	return exprs
}

func (p *pancakeSqlQueryGenerator) generatePartitionBy(groupByColumns []model.AliasedExpr) []model.Expr {
	partitionBy := make([]model.Expr, 0)
	if len(groupByColumns) == 0 {
		partitionBy = []model.Expr{model.NewLiteral(1)}
	} else {
		for _, col := range groupByColumns {
			partitionBy = append(partitionBy, p.newQuotedLiteral(col.Alias))
		}
	}
	return partitionBy
}

// TODO: Implement all functions
func (p *pancakeSqlQueryGenerator) generateAccumAggrFunctions(origExpr model.Expr, queryType model.QueryType) (accumExpr model.Expr, aggrFuncName string, err error) {
	switch origFunc := origExpr.(type) {
	case model.FunctionExpr:
		switch origFunc.Name {
		case "sumOrNull", "minOrNull", "maxOrNull":
			return origExpr, origFunc.Name, nil
		case "avgOrNull":
			return model.NewFunction("avgState", origFunc.Args...), "avgMerge", nil
		case "count":
			return model.NewFunction("count", origFunc.Args...), "sum", nil
		}
	}
	debugQueryType := "<nil>"
	if queryType != nil {
		debugQueryType = queryType.String()
	}
	return nil, "",
		fmt.Errorf("not implemented, queryType: %s, origExpr: %s", debugQueryType, model.AsString(origExpr))
}

func (p *pancakeSqlQueryGenerator) generateMetricSelects(metric *pancakeModelMetricAggregation, groupByColumns []model.AliasedExpr, hasMoreBucketAggregations bool) (addSelectColumns []model.AliasedExpr, addPartColumns []model.AliasedExpr, err error) {
	for columnId, column := range metric.selectedColumns {
		aliasedName := fmt.Sprintf("%s_col_%d", metric.internalName, columnId)
		finalColumn := column

		if hasMoreBucketAggregations {
			partColumnName := aliasedName + "_part"
			partColumn, aggFunctionName, err := p.generateAccumAggrFunctions(column, metric.queryType)
			if err != nil {
				return nil, nil, err
			}
			aliasedPartColumn := model.AliasedExpr{Expr: partColumn, Alias: partColumnName}
			addPartColumns = append(addPartColumns, aliasedPartColumn)
			finalColumn = model.WindowFunction{Name: aggFunctionName,
				Args:        []model.Expr{p.newQuotedLiteral(partColumnName)},
				PartitionBy: p.generatePartitionBy(groupByColumns),
				OrderBy:     []model.OrderByExpr{},
			}
		}
		aliasedColumn := model.AliasedExpr{Expr: finalColumn, Alias: aliasedName}
		addSelectColumns = append(addSelectColumns, aliasedColumn)
	}
	return
}

func (p *pancakeSqlQueryGenerator) generateBucketSqlParts(bucketAggregation *pancakeModelBucketAggregation, groupByColumns []model.AliasedExpr, hasMoreBucketAggregations bool) (
	addSelectColumns, addPartColumns, addGroupBys, addRankColumns []model.AliasedExpr, addRankWheres []model.Expr, addRankOrderBys []model.OrderByExpr, err error) {

	// TODO: ...
	for columnId, column := range bucketAggregation.selectedColumns {
		aliasedColumn := model.AliasedExpr{Expr: column, Alias: bucketAggregation.InternalNameForKey(columnId)}
		addSelectColumns = append(addSelectColumns, aliasedColumn)
		addGroupBys = append(addGroupBys, aliasedColumn)
	}

	// build count for aggr
	// TODO: Maybe optimize
	var countColumn model.Expr
	if hasMoreBucketAggregations {
		partCountAliasName := bucketAggregation.InternalNameForCount() + "_part"
		partCountColumn := model.NewFunction("count", model.NewLiteral("*"))
		partCountAliasedColumn := model.AliasedExpr{Expr: partCountColumn, Alias: partCountAliasName}
		addPartColumns = append(addPartColumns, partCountAliasedColumn)

		countColumn = model.WindowFunction{Name: "sum",
			Args:        []model.Expr{p.newQuotedLiteral(partCountAliasName)},
			PartitionBy: p.generatePartitionBy(append(groupByColumns, addGroupBys...)), /// TODO
			OrderBy:     []model.OrderByExpr{},
		}
	} else {
		countColumn = model.NewFunction("count", model.NewLiteral("*"))
	}
	countAliasedColumn := model.AliasedExpr{Expr: countColumn, Alias: bucketAggregation.InternalNameForCount()}
	addSelectColumns = append(addSelectColumns, countAliasedColumn)

	columnId := len(bucketAggregation.selectedColumns)
	if bucketAggregation.orderBy != nil && len(bucketAggregation.orderBy) > 0 {
		// TODO: handle all columns
		orderBy := bucketAggregation.orderBy[0].Exprs[0]
		orderByDirection := bucketAggregation.orderBy[0].Direction

		_, isColumnRef := orderBy.(model.ColumnRef)

		if hasMoreBucketAggregations && !isColumnRef {
			partColumnName := bucketAggregation.InternalNameForOrderBy(columnId) + "_part"
			partColumn, aggFunctionName, err := p.generateAccumAggrFunctions(orderBy, nil)
			if err != nil {
				return nil, nil, nil, nil, nil, nil, err
			}
			aliasedColumn := model.AliasedExpr{Expr: partColumn, Alias: partColumnName}
			addPartColumns = append(addPartColumns, aliasedColumn)
			// TODO: fix order by
			orderBy = model.WindowFunction{Name: aggFunctionName,
				Args:        []model.Expr{p.newQuotedLiteral(partColumnName)},
				PartitionBy: p.generatePartitionBy(append(groupByColumns, addGroupBys...)),
				OrderBy:     []model.OrderByExpr{},
			}
		}
		aliasedColumn := model.AliasedExpr{Expr: orderBy, Alias: bucketAggregation.InternalNameForOrderBy(columnId)}
		addSelectColumns = append(addSelectColumns, aliasedColumn)

		// We order by count, but add key to get right dense_rank()
		rankColumOrderBy := []model.OrderByExpr{model.NewOrderByExpr([]model.Expr{p.newQuotedLiteral(bucketAggregation.InternalNameForOrderBy(columnId))}, orderByDirection)}
		for _, addedGroupByAlias := range p.aliasedExprArrayToLiteralExpr(addGroupBys) {
			rankColumOrderBy = append(rankColumOrderBy, model.NewOrderByExpr([]model.Expr{addedGroupByAlias}, model.AscOrder))
		}

		rankColum := model.WindowFunction{Name: "dense_rank",
			Args:        []model.Expr{},
			PartitionBy: p.generatePartitionBy(groupByColumns),
			OrderBy:     rankColumOrderBy,
		}
		aliasedRank := model.AliasedExpr{Expr: rankColum, Alias: bucketAggregation.InternalNameForOrderBy(columnId) + "_rank"}
		addRankColumns = append(addRankColumns, aliasedRank)

		if bucketAggregation.limit != pancakeBucketAggregationNoLimit {
			// if where not null, increase limit by 1
			limit := bucketAggregation.limit
			if bucketAggregation.filterOurEmptyKeyBucket {
				limit += 1
			}
			whereRank := model.NewInfixExpr(p.newQuotedLiteral(aliasedRank.Alias), "<=", model.NewLiteral(limit))
			addRankWheres = append(addRankWheres, whereRank)
		}

		rankOrderBy := model.NewOrderByExpr([]model.Expr{p.newQuotedLiteral(aliasedRank.Alias)}, model.AscOrder)
		addRankOrderBys = append(addRankOrderBys, rankOrderBy)
	}
	return
}

// TODO: deduplicate metric names
func (p *pancakeSqlQueryGenerator) generateSelectCommand(aggregation *pancakeModel, table *clickhouse.Table) (*model.SelectCommand, bool, error) {
	if aggregation == nil {
		return nil, false, errors.New("aggregation is nil in generateQuery")
	}

	selectColumns := make([]model.AliasedExpr, 0)
	partColumns := make([]model.AliasedExpr, 0)
	rankColumns := make([]model.AliasedExpr, 0)
	rankWheres := make([]model.Expr, 0)
	rankOrderBys := make([]model.OrderByExpr, 0)
	groupBys := make([]model.AliasedExpr, 0)
	for layerId, layer := range aggregation.layers {
		hasMoreBucketAggregations := layerId+1 < len(aggregation.layers)

		for _, metric := range layer.currentMetricAggregations {
			addSelectColumns, addPartColumns, err := p.generateMetricSelects(metric, groupBys, hasMoreBucketAggregations)
			if err != nil {
				return nil, false, err
			}
			selectColumns = append(selectColumns, addSelectColumns...)
			partColumns = append(partColumns, addPartColumns...)
		}

		if layer.nextBucketAggregation != nil {
			hasMoreBucketAggregations = hasMoreBucketAggregations && aggregation.layers[layerId+1].nextBucketAggregation != nil
			addSelectColumns, addPartColumns, addGroupBys, addRankColumns, addRankWheres, addRankOrderBys, err :=
				p.generateBucketSqlParts(layer.nextBucketAggregation, groupBys, hasMoreBucketAggregations)
			if err != nil {
				return nil, false, err
			}
			selectColumns = append(selectColumns, addSelectColumns...)
			partColumns = append(partColumns, addPartColumns...)
			groupBys = append(groupBys, addGroupBys...)
			rankColumns = append(rankColumns, addRankColumns...)
			rankWheres = append(rankWheres, addRankWheres...)
			rankOrderBys = append(rankOrderBys, addRankOrderBys...)
		}
	}

	// if we have single layer we can emit simpler query
	if len(aggregation.layers) == 1 || len(aggregation.layers) == 2 && aggregation.layers[1].nextBucketAggregation == nil {
		limit := 0
		orderBy := make([]model.OrderByExpr, 0)
		if aggregation.layers[0].nextBucketAggregation != nil {
			limit = aggregation.layers[0].nextBucketAggregation.limit
			// if where not null, increase limit by 1
			if aggregation.layers[0].nextBucketAggregation.filterOurEmptyKeyBucket {
				if limit != 0 {
					limit += 1
				}
			}

			if len(rankColumns) > 0 {
				orderBy = rankColumns[0].Expr.(model.WindowFunction).OrderBy
			}
		}

		query := model.SelectCommand{
			Columns:     p.aliasedExprArrayToExpr(append(selectColumns, partColumns...)),
			GroupBy:     p.aliasedExprArrayToExpr(groupBys),
			WhereClause: aggregation.whereClause,
			FromClause:  model.NewTableRef(table.FullTableName()),
			OrderBy:     orderBy,
			Limit:       limit,
		}
		return &query, false, nil
	}

	windowCte := model.SelectCommand{
		Columns:     p.aliasedExprArrayToExpr(append(selectColumns, partColumns...)),
		GroupBy:     p.aliasedExprArrayToExpr(groupBys),
		WhereClause: aggregation.whereClause,
		FromClause:  model.NewTableRef(table.FullTableName()),
	}

	rankCte := model.SelectCommand{
		Columns:    append(p.aliasedExprArrayToLiteralExpr(selectColumns), p.aliasedExprArrayToExpr(rankColumns)...),
		FromClause: windowCte,
	}

	finalQuery := model.SelectCommand{
		Columns:     p.aliasedExprArrayToLiteralExpr(selectColumns),
		FromClause:  rankCte,
		WhereClause: model.And(rankWheres),
		OrderBy:     rankOrderBys,
	}

	return &finalQuery, true, nil
}

func (p *pancakeSqlQueryGenerator) generateQuery(aggregation *pancakeModel, table *clickhouse.Table) (*model.Query, error) {
	if aggregation == nil {
		return nil, errors.New("aggregation is nil in generateQuery")
	}

	resultSelectCommand, isFullPancake, err := p.generateSelectCommand(aggregation, table)
	if err != nil {
		return nil, err
	}

	resultQuery := &model.Query{
		SelectCommand: *resultSelectCommand,
		TableName:     table.FullTableName(),
		Type:          PancakeQueryType{pancakeAggregation: aggregation},
		OptimizeHints: model.NewQueryExecutionHints(),
	}

	if isFullPancake {
		resultQuery.OptimizeHints.OptimizationsPerformed = append(resultQuery.OptimizeHints.OptimizationsPerformed, PancakeOptimizerName)
	} else {
		resultQuery.OptimizeHints.OptimizationsPerformed = append(resultQuery.OptimizeHints.OptimizationsPerformed, PancakeOptimizerName+"(half)")
	}

	return resultQuery, nil
}
