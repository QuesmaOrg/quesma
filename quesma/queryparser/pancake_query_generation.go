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

func pancakeGenerateSelectCommand(aggregation *pancakeAggregation, table *clickhouse.Table) (*model.SelectCommand, error) {
	if aggregation == nil {
		return nil, errors.New("aggregation is nil in pancakeGenerateQuery")
	}

	selectedColumns := make([]model.AliasedExpr, 0)
	selectedPartColumns := make([]model.AliasedExpr, 0)
	selectedRankColumns := make([]model.AliasedExpr, 0)
	groupByColumns := make([]model.AliasedExpr, 0)
	namePrefix := ""
	for layerId, layer := range aggregation.layers {
		for _, metrics := range layer.currentMetricAggregations {
			for columnId, column := range metrics.selectedColumns {
				aliasedName := fmt.Sprintf("metric__%s%s%d", namePrefix, metrics.name, columnId)
				// TODO: check for collisions
				aliasedColumn := model.AliasedExpr{column, aliasedName}
				selectedColumns = append(selectedColumns, aliasedColumn)
			}
			// TODO
		}

		if layer.nextBucketAggregation != nil {
			bucketAggregation := layer.nextBucketAggregation
			// take care of bucket aggregation at level - 1
			namePrefix = fmt.Sprintf("%s%s__", namePrefix, bucketAggregation.name)
			prevGroupByColumns := groupByColumns

			// TODO: ...
			for columnId, column := range bucketAggregation.selectedColumns {
				aliasedName := fmt.Sprintf("aggr__%s%d", namePrefix, columnId)
				// TODO: check for collisions
				aliasedColumn := model.AliasedExpr{column, aliasedName}
				selectedColumns = append(selectedColumns, aliasedColumn)
				groupByColumns = append(groupByColumns, aliasedColumn)
			}
			columnId := len(bucketAggregation.selectedColumns)
			if bucketAggregation.orderBy != nil && len(bucketAggregation.orderBy) > 0 {
				// TODO: different columns
				orderBy := bucketAggregation.orderBy[0].Exprs[0]
				aliasedName := fmt.Sprintf("aggr__%s%d", namePrefix, columnId)
				columnId += 1

				partitionBy := []model.Expr{}
				if len(prevGroupByColumns) == 0 {
					partitionBy = []model.Expr{model.NewLiteral(1)}
				} else {
					for _, col := range prevGroupByColumns {
						partitionBy = append(partitionBy, newQuotedLiteral(col.Alias))
					}
				}

				if layerId < len(aggregation.layers)-1 {
					partColumnName := aliasedName + "_part"
					aliasedColumn := model.AliasedExpr{orderBy, partColumnName}
					selectedPartColumns = append(selectedPartColumns, aliasedColumn)
					// TODO: need proper aggregate, not just for count
					orderByAgg := model.WindowFunction{Name: "sum", // TODO: different too
						Args:        []model.Expr{newQuotedLiteral(partColumnName)},
						PartitionBy: partitionBy,
						OrderBy:     model.NewOrderByExprWithoutOrder(),
					}
					aliasedOrderByAgg := model.AliasedExpr{orderByAgg, aliasedName}
					selectedColumns = append(selectedColumns, aliasedOrderByAgg)
				} else {
					aliasedColumn := model.AliasedExpr{orderBy, aliasedName}
					selectedColumns = append(selectedColumns, aliasedColumn)
				}
				rankColum := model.WindowFunction{Name: "dense_rank",
					Args:        []model.Expr{},
					PartitionBy: partitionBy,
					// TODO: in order by we need key too
					OrderBy: model.NewOrderByExpr([]model.Expr{newQuotedLiteral(aliasedName)}, model.DescOrder),
				}
				aliasedRank := model.AliasedExpr{rankColum, aliasedName + "_rank"}
				selectedRankColumns = append(selectedRankColumns, aliasedRank)
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

	return &rankCte, nil
}

func pancakeGenerateQuery(aggregation *pancakeAggregation, table *clickhouse.Table) (*model.Query, error) {
	if aggregation == nil {
		return nil, errors.New("aggregation is nil in pancakeGenerateQuery")
	}

	resultSelectCommand, err := pancakeGenerateSelectCommand(aggregation, table)
	if err != nil {
		return nil, err
	}

	resutQuery := &model.Query{
		SelectCommand: *resultSelectCommand,
		TableName:     table.FullTableName(),
		// TODO: Rest is to be filled, some of them incompatible with current query.model
	}

	return resutQuery, nil
}
