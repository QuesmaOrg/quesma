// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package queryparser

import (
	"errors"
	"fmt"
	"quesma/model"
)

func pancakeGenerateSelectCommand(aggregation *pancakeAggregation) (*model.SelectCommand, error) {
	if aggregation == nil {
		return nil, errors.New("aggregation is nil in pancakeGenerateQuery")
	}

	selectedColumns := make([]model.Expr, 0)
	groupByColumns := make([]model.Expr, 0)
	namePrefix := ""
	for _, layer := range aggregation.layers {
		for _, metrics := range layer.currentMetricAggregations {
			for columnId, column := range metrics.selectedColumns {
				aliasedName := fmt.Sprintf("%s%s_%d", namePrefix, metrics.name, columnId)
				// TODO: check for collisions
				aliasedColumn := model.AliasedExpr{column, aliasedName}
				selectedColumns = append(selectedColumns, aliasedColumn)
			}
			// TODO
		}

		if layer.nextBucketAggregation != nil {
			// take care of bucket aggregation at level - 1
			namePrefix = fmt.Sprintf("%s%s_", namePrefix, layer.nextBucketAggregation.name)
			// TODO: ...
			for columnId, column := range layer.nextBucketAggregation.selectedColumns {
				aliasedName := fmt.Sprintf("%s_bucket_%d", namePrefix, columnId)
				// TODO: check for collisions
				aliasedColumn := model.AliasedExpr{column, aliasedName}
				selectedColumns = append(selectedColumns, aliasedColumn)
				if columnId < len(layer.nextBucketAggregation.selectedColumns)-1 {
					groupByColumns = append(groupByColumns, aliasedColumn)
				}
			}
		}

	}

	result := model.SelectCommand{
		Columns:     selectedColumns,
		GroupBy:     groupByColumns,
		WhereClause: aggregation.whereClause,
	}

	return &result, nil
}

func pancakeGenerateQuery(aggregation *pancakeAggregation) (*model.Query, error) {
	if aggregation == nil {
		return nil, errors.New("aggregation is nil in pancakeGenerateQuery")
	}

	resultSelectCommand, err := pancakeGenerateSelectCommand(aggregation)
	if err != nil {
		return nil, err
	}

	resutQuery := &model.Query{
		SelectCommand: *resultSelectCommand,
		// TODO: Rest is to be filled, some of them incompatible with current query.model
	}

	return resutQuery, nil
}
