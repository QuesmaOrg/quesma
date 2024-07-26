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
	if len(aggregation.bucketAggregations)+1 != len(aggregation.metricAggregations) {
		return nil, errors.New("number of bucket aggregations and metric aggregations does not match in pancakeGenerateQuery")
	}

	selectedColumns := make([]model.Expr, 0)
	groupByColumns := make([]model.Expr, 0)
	namePrefix := ""
	for level, _ := range aggregation.metricAggregations {
		if level > 0 {
			// take care of bucket aggregation at level - 1
			namePrefix = fmt.Sprintf("%s%s_", namePrefix, aggregation.bucketAggregations[level-1].name)
			// TODO: ...
			for columnId, column := range aggregation.bucketAggregations[level-1].selectedColumns {
				aliasedName := fmt.Sprintf("%s_bucket_%d", namePrefix, columnId)
				// TODO: check for collisions
				aliasedColumn := model.AliasedExpr{column, aliasedName}
				selectedColumns = append(selectedColumns, aliasedColumn)
				if columnId < len(aggregation.bucketAggregations[level-1].selectedColumns)-1 {
					groupByColumns = append(groupByColumns, aliasedColumn)
				}
			}
		}
		for _, metrics := range aggregation.metricAggregations[level] {
			for columnId, column := range metrics.selectedColumns {
				aliasedName := fmt.Sprintf("%s%s_%d", namePrefix, metrics.name, columnId)
				// TODO: check for collisions
				aliasedColumn := model.AliasedExpr{column, aliasedName}
				selectedColumns = append(selectedColumns, aliasedColumn)
			}
			// TODO
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
