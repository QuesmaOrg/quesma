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

func newQuotedLiteral(value string) model.LiteralExpr {
	return model.LiteralExpr{Value: strconv.Quote(value)}
}

func aliasedExprArrayToExpr(aliasedExprs []model.AliasedExpr) []model.Expr {
	exprs := make([]model.Expr, 0, len(aliasedExprs))
	for _, aliasedExpr := range aliasedExprs {
		exprs = append(exprs, aliasedExpr)
	}
	return exprs
}

func aliasedExprArrayToLiteralExpr(aliasedExprs []model.AliasedExpr) []model.Expr {
	exprs := make([]model.Expr, 0, len(aliasedExprs))
	for _, aliasedExpr := range aliasedExprs {
		exprs = append(exprs, newQuotedLiteral(aliasedExpr.Alias))
	}
	return exprs
}

func pancakeGeneratePartitionBy(groupByColumns []model.AliasedExpr) []model.Expr {
	partitionBy := make([]model.Expr, 0)
	if len(groupByColumns) == 0 {
		partitionBy = []model.Expr{model.NewLiteral(1)}
	} else {
		for _, col := range groupByColumns {
			partitionBy = append(partitionBy, newQuotedLiteral(col.Alias))
		}
	}
	return partitionBy
}

// TODO: Implement all functions
func pancakeGenerateAccumAggrFunctions(origExpr model.Expr, queryType model.QueryType) (accumExpr model.Expr, aggrFuncName string, err error) {
	switch origExpr.(type) {
	case model.FunctionExpr:
		origFunc := origExpr.(model.FunctionExpr)
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
func pancakeGenerateSelectCommand(aggregation *pancakeAggregation, table *clickhouse.Table) (*model.SelectCommand, error) {
	if aggregation == nil {
		return nil, errors.New("aggregation is nil in pancakeGenerateQuery")
	}

	selectedColumns := make([]model.AliasedExpr, 0)
	selectedPartColumns := make([]model.AliasedExpr, 0)
	selectedRankColumns := make([]model.AliasedExpr, 0)
	whereRanks := make([]model.Expr, 0)
	rankOrderBys := make([]model.OrderByExpr, 0)
	groupByColumns := make([]model.AliasedExpr, 0)
	namePrefix := ""
	for layerId, layer := range aggregation.layers {
		for _, metrics := range layer.currentMetricAggregations {
			for columnId, column := range metrics.selectedColumns {
				aliasedName := fmt.Sprintf("metric__%s%s_col_%d", namePrefix, metrics.name, columnId)

				// TODO: check for collisions
				if layerId < len(aggregation.layers)-1 {
					partColumnName := aliasedName + "_part"
					partColumn, aggFunctionName, err := pancakeGenerateAccumAggrFunctions(column, metrics.queryType)
					if err != nil {
						return nil, err
					}
					aliasedPartColumn := model.AliasedExpr{partColumn, partColumnName}
					selectedPartColumns = append(selectedPartColumns, aliasedPartColumn)
					finalColumn := model.WindowFunction{Name: aggFunctionName,
						Args:        []model.Expr{newQuotedLiteral(partColumnName)},
						PartitionBy: pancakeGeneratePartitionBy(groupByColumns),
						OrderBy:     []model.OrderByExpr{},
					}
					aliasedColumn := model.AliasedExpr{finalColumn, aliasedName}
					selectedColumns = append(selectedColumns, aliasedColumn)
				} else {
					aliasedColumn := model.AliasedExpr{column, aliasedName}
					selectedColumns = append(selectedColumns, aliasedColumn)
				}
			}
		}

		if layer.nextBucketAggregation != nil {
			bucketAggregation := layer.nextBucketAggregation
			// take care of bucket aggregation at level - 1
			namePrefix = fmt.Sprintf("%s%s__", namePrefix, bucketAggregation.name)

			addedGroupByAliases := []model.Expr{}
			previousGroupByColumns := groupByColumns

			// TODO: ...
			for columnId, column := range bucketAggregation.selectedColumns {
				aliasedName := fmt.Sprintf("aggr__%skey_%d", namePrefix, columnId)
				// TODO: check for collisions
				aliasedColumn := model.AliasedExpr{column, aliasedName}
				selectedColumns = append(selectedColumns, aliasedColumn)
				groupByColumns = append(groupByColumns, aliasedColumn)
				addedGroupByAliases = append(addedGroupByAliases, newQuotedLiteral(aliasedName))
			}
			columnId := len(bucketAggregation.selectedColumns)
			if bucketAggregation.orderBy != nil && len(bucketAggregation.orderBy) > 0 {
				// TODO: different columns
				orderBy := bucketAggregation.orderBy[0].Exprs[0]
				aliasedName := fmt.Sprintf("aggr__%sorder_%d", namePrefix, columnId)
				columnId += 1

				// if it is not last bucket aggregation
				if layerId < len(aggregation.layers)-1 && aggregation.layers[layerId+1].nextBucketAggregation != nil {
					partColumnName := aliasedName + "_part"
					partColumn, aggFunctionName, err := pancakeGenerateAccumAggrFunctions(orderBy, nil)
					if err != nil {
						return nil, err
					}
					aliasedColumn := model.AliasedExpr{partColumn, partColumnName}
					selectedPartColumns = append(selectedPartColumns, aliasedColumn)
					orderByAgg := model.WindowFunction{Name: aggFunctionName,
						Args:        []model.Expr{newQuotedLiteral(partColumnName)},
						PartitionBy: pancakeGeneratePartitionBy(groupByColumns),
						OrderBy:     []model.OrderByExpr{},
					}
					aliasedOrderByAgg := model.AliasedExpr{orderByAgg, aliasedName}
					selectedColumns = append(selectedColumns, aliasedOrderByAgg)
				} else {
					aliasedColumn := model.AliasedExpr{orderBy, aliasedName}
					selectedColumns = append(selectedColumns, aliasedColumn)
				}
				// We order by count, but add key to get right dense_rank()
				rankColumOrderBy := []model.OrderByExpr{model.NewOrderByExpr([]model.Expr{newQuotedLiteral(aliasedName)}, model.DescOrder)}
				for _, addedGroupByAlias := range addedGroupByAliases {
					rankColumOrderBy = append(rankColumOrderBy, model.NewOrderByExpr([]model.Expr{addedGroupByAlias}, model.AscOrder))
				}

				rankColum := model.WindowFunction{Name: "dense_rank",
					Args:        []model.Expr{},
					PartitionBy: pancakeGeneratePartitionBy(previousGroupByColumns),
					OrderBy:     rankColumOrderBy,
				}
				aliasedRank := model.AliasedExpr{rankColum, aliasedName + "_rank"}
				selectedRankColumns = append(selectedRankColumns, aliasedRank)

				whereRank := model.NewInfixExpr(newQuotedLiteral(aliasedRank.Alias), "<=", model.NewLiteral(bucketAggregation.limit))
				whereRanks = append(whereRanks, whereRank)

				rankOrderBy := model.NewOrderByExpr([]model.Expr{newQuotedLiteral(aliasedRank.Alias)}, model.AscOrder)
				rankOrderBys = append(rankOrderBys, rankOrderBy)
			}
		}
	}

	windowCte := model.SelectCommand{
		Columns:     aliasedExprArrayToExpr(append(selectedColumns, selectedPartColumns...)),
		GroupBy:     aliasedExprArrayToExpr(groupByColumns),
		WhereClause: aggregation.whereClause,
		FromClause:  model.NewTableRef(table.FullTableName()),
	}

	rankCte := model.SelectCommand{
		Columns:    append(aliasedExprArrayToLiteralExpr(selectedColumns), aliasedExprArrayToExpr(selectedRankColumns)...),
		FromClause: windowCte,
	}

	finalQuery := model.SelectCommand{
		Columns:     aliasedExprArrayToLiteralExpr(selectedColumns),
		FromClause:  rankCte,
		WhereClause: model.And(whereRanks),
		OrderBy:     rankOrderBys,
	}

	return &finalQuery, nil
}

func pancakeGenerateQuery(aggregation *pancakeAggregation, table *clickhouse.Table) (*model.Query, error) {
	if aggregation == nil {
		return nil, errors.New("aggregation is nil in pancakeGenerateQuery")
	}

	resultSelectCommand, err := pancakeGenerateSelectCommand(aggregation, table)
	if err != nil {
		return nil, err
	}

	resultQuery := &model.Query{
		SelectCommand: *resultSelectCommand,
		TableName:     table.FullTableName(),
		// TODO: Rest is to be filled, some of them incompatible with current query.model
	}

	return resultQuery, nil
}
