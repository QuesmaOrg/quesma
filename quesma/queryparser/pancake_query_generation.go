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

type pancakeQueryGenerator struct {
}

func (p *pancakeQueryGenerator) newQuotedLiteral(value string) model.LiteralExpr {
	return model.LiteralExpr{Value: strconv.Quote(value)}
}

func (p *pancakeQueryGenerator) aliasedExprArrayToExpr(aliasedExprs []model.AliasedExpr) []model.Expr {
	exprs := make([]model.Expr, 0, len(aliasedExprs))
	for _, aliasedExpr := range aliasedExprs {
		exprs = append(exprs, aliasedExpr)
	}
	return exprs
}

func (p *pancakeQueryGenerator) aliasedExprArrayToLiteralExpr(aliasedExprs []model.AliasedExpr) []model.Expr {
	exprs := make([]model.Expr, 0, len(aliasedExprs))
	for _, aliasedExpr := range aliasedExprs {
		exprs = append(exprs, p.newQuotedLiteral(aliasedExpr.Alias))
	}
	return exprs
}

func (p *pancakeQueryGenerator) generatePartitionBy(groupByColumns []model.AliasedExpr) []model.Expr {
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
func (p *pancakeQueryGenerator) generateAccumAggrFunctions(origExpr model.Expr, queryType model.QueryType) (accumExpr model.Expr, aggrFuncName string, err error) {
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

// TODO: deduplicate metric names
func (p *pancakeQueryGenerator) generateSelectCommand(aggregation *pancakeAggregation, table *clickhouse.Table) (*model.SelectCommand, bool, error) {
	if aggregation == nil {
		return nil, false, errors.New("aggregation is nil in generateQuery")
	}

	selectedColumns := make([]model.AliasedExpr, 0)
	selectedPartColumns := make([]model.AliasedExpr, 0)
	selectedRankColumns := make([]model.AliasedExpr, 0)
	whereRanks := make([]model.Expr, 0)
	rankOrderBys := make([]model.OrderByExpr, 0)
	groupByColumns := make([]model.AliasedExpr, 0)
	for layerId, layer := range aggregation.layers {
		for _, metric := range layer.currentMetricAggregations {
			for columnId, column := range metric.selectedColumns {
				aliasedName := fmt.Sprintf("%s_col_%d", metric.aliasName, columnId)

				if layerId < len(aggregation.layers)-1 {
					partColumnName := aliasedName + "_part"
					partColumn, aggFunctionName, err := p.generateAccumAggrFunctions(column, metric.queryType)
					if err != nil {
						return nil, false, err
					}
					aliasedPartColumn := model.AliasedExpr{Expr: partColumn, Alias: partColumnName}
					selectedPartColumns = append(selectedPartColumns, aliasedPartColumn)
					finalColumn := model.WindowFunction{Name: aggFunctionName,
						Args:        []model.Expr{p.newQuotedLiteral(partColumnName)},
						PartitionBy: p.generatePartitionBy(groupByColumns),
						OrderBy:     []model.OrderByExpr{},
					}
					aliasedColumn := model.AliasedExpr{Expr: finalColumn, Alias: aliasedName}
					selectedColumns = append(selectedColumns, aliasedColumn)
				} else {
					aliasedColumn := model.AliasedExpr{Expr: column, Alias: aliasedName}
					selectedColumns = append(selectedColumns, aliasedColumn)
				}
			}
		}

		if layer.nextBucketAggregation != nil {
			bucketAggregation := layer.nextBucketAggregation
			// take care of bucket aggregation at level - 1

			addedGroupByAliases := []model.Expr{}
			previousGroupByColumns := groupByColumns

			// TODO: ...
			for columnId, column := range bucketAggregation.selectedColumns {
				aliasedColumn := model.AliasedExpr{Expr: column, Alias: bucketAggregation.AliasNameForKey(columnId)}
				selectedColumns = append(selectedColumns, aliasedColumn)
				groupByColumns = append(groupByColumns, aliasedColumn)
				addedGroupByAliases = append(addedGroupByAliases, p.newQuotedLiteral(aliasedColumn.Alias))
			}

			hasMoreBucketAggregations := layerId < len(aggregation.layers)-1 && aggregation.layers[layerId+1].nextBucketAggregation != nil

			// build count for aggr
			// TODO: Maybe optimize
			if hasMoreBucketAggregations {
				partCountAliasName := bucketAggregation.AliasNameForCount() + "_part"
				partCountColumn := model.NewFunction("count", model.NewLiteral("*"))
				partCountAliasedColumn := model.AliasedExpr{Expr: partCountColumn, Alias: partCountAliasName}
				selectedPartColumns = append(selectedPartColumns, partCountAliasedColumn)

				countColumn := model.WindowFunction{Name: "sum",
					Args:        []model.Expr{p.newQuotedLiteral(partCountAliasName)},
					PartitionBy: p.generatePartitionBy(groupByColumns), /// TODO
					OrderBy:     []model.OrderByExpr{},
				}
				countAliasedColumn := model.AliasedExpr{Expr: countColumn, Alias: bucketAggregation.AliasNameForCount()}
				selectedColumns = append(selectedColumns, countAliasedColumn)
			} else {
				countColumn := model.NewFunction("count", model.NewLiteral("*"))
				countAliasedColumn := model.AliasedExpr{Expr: countColumn, Alias: bucketAggregation.AliasNameForCount()}
				selectedColumns = append(selectedColumns, countAliasedColumn)
			}

			columnId := len(bucketAggregation.selectedColumns)
			if bucketAggregation.orderBy != nil && len(bucketAggregation.orderBy) > 0 {
				// TODO: handle all columns
				orderBy := bucketAggregation.orderBy[0].Exprs[0]
				orderByDirection := bucketAggregation.orderBy[0].Direction

				_, isColumnRef := orderBy.(model.ColumnRef)

				if hasMoreBucketAggregations && !isColumnRef {
					partColumnName := bucketAggregation.AliasNameForOrderBy(columnId) + "_part"
					partColumn, aggFunctionName, err := p.generateAccumAggrFunctions(orderBy, nil)
					if err != nil {
						return nil, false, err
					}
					aliasedColumn := model.AliasedExpr{Expr: partColumn, Alias: partColumnName}
					selectedPartColumns = append(selectedPartColumns, aliasedColumn)
					// TODO: fix order by
					orderByAgg := model.WindowFunction{Name: aggFunctionName,
						Args:        []model.Expr{p.newQuotedLiteral(partColumnName)},
						PartitionBy: p.generatePartitionBy(groupByColumns),
						OrderBy:     []model.OrderByExpr{},
					}
					aliasedOrderByAgg := model.AliasedExpr{Expr: orderByAgg, Alias: bucketAggregation.AliasNameForOrderBy(columnId)}
					selectedColumns = append(selectedColumns, aliasedOrderByAgg)
				} else {
					aliasedColumn := model.AliasedExpr{Expr: orderBy, Alias: bucketAggregation.AliasNameForOrderBy(columnId)}
					selectedColumns = append(selectedColumns, aliasedColumn)
				}

				// We order by count, but add key to get right dense_rank()
				rankColumOrderBy := []model.OrderByExpr{model.NewOrderByExpr([]model.Expr{p.newQuotedLiteral(bucketAggregation.AliasNameForOrderBy(columnId))}, orderByDirection)}
				for _, addedGroupByAlias := range addedGroupByAliases {
					rankColumOrderBy = append(rankColumOrderBy, model.NewOrderByExpr([]model.Expr{addedGroupByAlias}, model.AscOrder))
				}

				rankColum := model.WindowFunction{Name: "dense_rank",
					Args:        []model.Expr{},
					PartitionBy: p.generatePartitionBy(previousGroupByColumns),
					OrderBy:     rankColumOrderBy,
				}
				aliasedRank := model.AliasedExpr{Expr: rankColum, Alias: bucketAggregation.AliasNameForOrderBy(columnId) + "_rank"}
				selectedRankColumns = append(selectedRankColumns, aliasedRank)

				// if where not null, increase limit by 1
				limit := bucketAggregation.limit
				if bucketAggregation.filterOurEmptyKeyBucket {
					if limit != 0 {
						limit += 1
					}
				}

				whereRank := model.NewInfixExpr(p.newQuotedLiteral(aliasedRank.Alias), "<=", model.NewLiteral(bucketAggregation.limit))
				whereRanks = append(whereRanks, whereRank)

				rankOrderBy := model.NewOrderByExpr([]model.Expr{p.newQuotedLiteral(aliasedRank.Alias)}, model.AscOrder)
				rankOrderBys = append(rankOrderBys, rankOrderBy)
			}
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

			if len(selectedRankColumns) > 0 {
				orderBy = selectedRankColumns[0].Expr.(model.WindowFunction).OrderBy
			}
		}

		query := model.SelectCommand{
			Columns:     p.aliasedExprArrayToExpr(append(selectedColumns, selectedPartColumns...)),
			GroupBy:     p.aliasedExprArrayToExpr(groupByColumns),
			WhereClause: aggregation.whereClause,
			FromClause:  model.NewTableRef(table.FullTableName()),
			OrderBy:     orderBy,
			Limit:       limit,
		}
		return &query, false, nil
	}

	windowCte := model.SelectCommand{
		Columns:     p.aliasedExprArrayToExpr(append(selectedColumns, selectedPartColumns...)),
		GroupBy:     p.aliasedExprArrayToExpr(groupByColumns),
		WhereClause: aggregation.whereClause,
		FromClause:  model.NewTableRef(table.FullTableName()),
	}

	rankCte := model.SelectCommand{
		Columns:    append(p.aliasedExprArrayToLiteralExpr(selectedColumns), p.aliasedExprArrayToExpr(selectedRankColumns)...),
		FromClause: windowCte,
	}

	finalQuery := model.SelectCommand{
		Columns:     p.aliasedExprArrayToLiteralExpr(selectedColumns),
		FromClause:  rankCte,
		WhereClause: model.And(whereRanks),
		OrderBy:     rankOrderBys,
	}

	return &finalQuery, true, nil
}

func (p *pancakeQueryGenerator) generateQuery(aggregation *pancakeAggregation, table *clickhouse.Table) (*model.Query, error) {
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
