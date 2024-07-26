// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package queryparser

import (
	"errors"
	"fmt"
	"quesma/clickhouse"
	"quesma/model"
)

func pancakeGenerateSelectCommand(aggregation *pancakeAggregation, table *clickhouse.Table) (*model.SelectCommand, error) {
	if aggregation == nil {
		return nil, errors.New("aggregation is nil in pancakeGenerateQuery")
	}

	selectedColumns := make([]model.Expr, 0)
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
				if layerId < len(aggregation.layers)-1 {
					partColumnName := aliasedName + "_part"
					aliasedColumn := model.AliasedExpr{orderBy, partColumnName}
					selectedColumns = append(selectedColumns, aliasedColumn)
					// TODO: need proper aggregate, not just for count
					partitionBy := []model.Expr{}
					if len(prevGroupByColumns) == 0 {
						partitionBy = []model.Expr{model.NewLiteral(1)}
					} else {
						for _, col := range prevGroupByColumns {
							partitionBy = append(partitionBy, model.NewLiteral(col.Alias))
						}
					}
					orderByAgg := model.WindowFunction{"sum",
						[]model.Expr{model.NewLiteral(partColumnName)},
						partitionBy,
						model.NewOrderByExprWithoutOrder(),
					}
					aliasedOrderByAgg := model.AliasedExpr{orderByAgg, aliasedName}
					selectedColumns = append(selectedColumns, aliasedOrderByAgg)
				} else {
					aliasedColumn := model.AliasedExpr{orderBy, aliasedName}
					selectedColumns = append(selectedColumns, aliasedColumn)
				}
			}
		}

	}

	groupByCasted := make([]model.Expr, 0, len(groupByColumns))
	for _, col := range groupByColumns {
		groupByCasted = append(groupByCasted, col)
	}

	result := model.SelectCommand{
		Columns:     selectedColumns,
		GroupBy:     groupByCasted,
		WhereClause: aggregation.whereClause,
		FromClause:  model.NewTableRef(table.FullTableName()),
	}

	return &result, nil
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
