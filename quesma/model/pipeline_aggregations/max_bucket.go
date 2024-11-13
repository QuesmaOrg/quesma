// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package pipeline_aggregations

import (
	"context"
	"fmt"
	"quesma/logger"
	"quesma/model"
	"quesma/queryprocessor"
	"quesma/util"
)

type MaxBucket struct {
	*PipelineAggregation
}

func NewMaxBucket(ctx context.Context, bucketsPath string) MaxBucket {
	return MaxBucket{PipelineAggregation: newPipelineAggregation(ctx, bucketsPath)}
}

func (query MaxBucket) AggregationType() model.AggregationType {
	return model.PipelineMetricsAggregation
}

// FIXME I think we should return all rows, not just 1
// Dunno why it's working, maybe I'm wrong.
// Let's wait for this until all pipeline merges, when I'll perform some more thorough tests.
func (query MaxBucket) TranslateSqlResponseToJson(rows []model.QueryResultRow) model.JsonMap {
	if len(rows) == 0 {
		logger.WarnWithCtx(query.ctx).Msg("no rows returned for max bucket aggregation")
		return model.JsonMap{}
	}
	if len(rows) > 1 {
		logger.WarnWithCtx(query.ctx).Msg("more than one row returned for max bucket aggregation")
	}
	if returnMap, ok := rows[0].LastColValue().(model.JsonMap); ok {
		return returnMap
	} else {
		logger.WarnWithCtx(query.ctx).Msgf("could not convert value to JsonMap: %v, type: %T", rows[0].LastColValue(), rows[0].LastColValue())
		return model.JsonMap{}
	}
}

func (query MaxBucket) CalculateResultWhenMissing(parentRows []model.QueryResultRow) []model.QueryResultRow {
	resultRows := make([]model.QueryResultRow, 0)
	if len(parentRows) == 0 {
		return resultRows // maybe null?
	}
	qp := queryprocessor.NewQueryProcessor(query.ctx)
	parentFieldsCnt := len(parentRows[0].Cols) - 2 // -2, because row is [parent_cols..., current_key, current_value]
	// in calculateSingleAvgBucket we calculate avg all current_keys with the same parent_cols
	// so we need to split into buckets based on parent_cols
	for _, parentRowsOneBucket := range qp.SplitResultSetIntoBuckets(parentRows, parentFieldsCnt) {
		resultRows = append(resultRows, query.calculateSingleMaxBucket(parentRowsOneBucket))
	}
	return resultRows
}

// we're sure len(parentRows) > 0
func (query MaxBucket) calculateSingleMaxBucket(parentRows []model.QueryResultRow) model.QueryResultRow {
	var resultValue any
	var resultKeys []any

	firstNonNilIndex := -1
	for i, row := range parentRows {
		if row.LastColValue() != nil {
			firstNonNilIndex = i
			break
		}
	}
	if firstNonNilIndex == -1 {
		resultRow := parentRows[0].Copy()
		resultRow.Cols[len(resultRow.Cols)-1].Value = model.JsonMap{
			"value": resultValue,
			"keys":  resultKeys,
		}
		return resultRow
	}

	if firstRowValueFloat, firstRowValueIsFloat := util.ExtractFloat64Maybe(parentRows[firstNonNilIndex].LastColValue()); firstRowValueIsFloat {
		// find max
		maxValue := firstRowValueFloat
		for _, row := range parentRows[firstNonNilIndex+1:] {
			value, ok := util.ExtractFloat64Maybe(row.LastColValue())
			if ok {
				maxValue = max(maxValue, value)
			} else {
				logger.WarnWithCtx(query.ctx).Msgf("could not convert value to float: %v, type: %T. Skipping", row.LastColValue(), row.LastColValue())
			}
		}
		resultValue = maxValue
		// find keys with max value
		for _, row := range parentRows[firstNonNilIndex:] {
			if value, ok := util.ExtractFloat64Maybe(row.LastColValue()); ok && value == maxValue {
				resultKeys = append(resultKeys, query.getKey(row))
			}
		}
	} else if firstRowValueInt, firstRowValueIsInt := util.ExtractInt64Maybe(parentRows[firstNonNilIndex].LastColValue()); firstRowValueIsInt {
		// find max
		maxValue := firstRowValueInt
		for _, row := range parentRows[firstNonNilIndex+1:] {
			value, ok := util.ExtractInt64Maybe(row.LastColValue())
			if ok {
				maxValue = max(maxValue, value)
			} else {
				logger.WarnWithCtx(query.ctx).Msgf("could not convert value to float: %v, type: %T. Skipping", row.LastColValue(), row.LastColValue())
			}
		}
		resultValue = maxValue
		// find keys with max value
		for _, row := range parentRows[firstNonNilIndex:] {
			if value, ok := util.ExtractInt64Maybe(row.LastColValue()); ok && value == maxValue {
				resultKeys = append(resultKeys, query.getKey(row))
			}
		}
	} else {
		logger.WarnWithCtx(query.ctx).Msgf("could not convert value to float or int: %v, type: %T. Returning nil.",
			parentRows[firstNonNilIndex].LastColValue(), parentRows[firstNonNilIndex].LastColValue())
	}

	resultRow := parentRows[0].Copy()
	resultRow.Cols[len(resultRow.Cols)-1].Value = model.JsonMap{
		"value": resultValue,
		"keys":  resultKeys,
	}
	return resultRow
}

func (query MaxBucket) String() string {
	return fmt.Sprintf("max_bucket(%s)", query.Parent)
}

func (query MaxBucket) PipelineAggregationType() model.PipelineAggregationType {
	return model.PipelineSiblingAggregation
}
