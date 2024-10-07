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

type MinBucket struct {
	*PipelineAggregation
}

func NewMinBucket(ctx context.Context, bucketsPath string) MinBucket {
	return MinBucket{PipelineAggregation: newPipelineAggregation(ctx, bucketsPath)}
}

func (query MinBucket) AggregationType() model.AggregationType {
	return model.PipelineMetricsAggregation
}

func (query MinBucket) TranslateSqlResponseToJson(rows []model.QueryResultRow) model.JsonMap {
	if len(rows) == 0 {
		logger.WarnWithCtx(query.ctx).Msg("no rows returned for min bucket aggregation")
		return model.JsonMap{}
	}
	if len(rows) > 1 {
		logger.WarnWithCtx(query.ctx).Msg("more than one row returned for min bucket aggregation")
	}
	if returnMap, ok := rows[0].LastColValue().(model.JsonMap); ok {
		return returnMap
	}
	logger.WarnWithCtx(query.ctx).Msgf("could not convert value to JsonMap: %v, type: %T", rows[0].LastColValue(), rows[0].LastColValue())
	return model.JsonMap{}
}

func (query MinBucket) CalculateResultWhenMissing(parentRows []model.QueryResultRow) []model.QueryResultRow {
	resultRows := make([]model.QueryResultRow, 0)
	if len(parentRows) == 0 {
		return resultRows // maybe null?
	}
	qp := queryprocessor.NewQueryProcessor(query.ctx)
	parentFieldsCnt := len(parentRows[0].Cols) - 2 // -2, because row is [parent_cols..., current_key, current_value]
	// in calculateSingleAvgBucket we calculate avg all current_keys with the same parent_cols
	// so we need to split into buckets based on parent_cols
	if parentFieldsCnt < 0 {
		logger.WarnWithCtx(query.ctx).Msgf("parentFieldsCnt is less than 0: %d", parentFieldsCnt)
	}
	for _, parentRowsOneBucket := range qp.SplitResultSetIntoBuckets(parentRows, parentFieldsCnt) {
		resultRows = append(resultRows, query.calculateSingleMinBucket(parentRowsOneBucket))
	}
	return resultRows
}

// we're sure len(parentRows) > 0
func (query MinBucket) calculateSingleMinBucket(parentRows []model.QueryResultRow) model.QueryResultRow {
	var resultValue any
	var resultKeys []any
	if firstRowValueFloat, firstRowValueIsFloat := util.ExtractFloat64Maybe(parentRows[0].LastColValue()); firstRowValueIsFloat {
		// find min
		minValue := firstRowValueFloat
		for _, row := range parentRows[1:] {
			value, ok := util.ExtractFloat64Maybe(row.LastColValue())
			if ok {
				minValue = min(minValue, value)
			} else {
				logger.WarnWithCtx(query.ctx).Msgf("could not convert value to float: %v, type: %T. Skipping", row.LastColValue(), row.LastColValue())
			}
		}
		resultValue = minValue
		// find keys with min value
		for _, row := range parentRows {
			if value, ok := util.ExtractFloat64Maybe(row.LastColValue()); ok && value == minValue {
				resultKeys = append(resultKeys, query.getKey(row))
			}
		}
	} else if firstRowValueInt, firstRowValueIsInt := util.ExtractInt64Maybe(parentRows[0].LastColValue()); firstRowValueIsInt {
		// find min
		minValue := firstRowValueInt
		for _, row := range parentRows[1:] {
			value, ok := util.ExtractInt64Maybe(row.LastColValue())
			if ok {
				minValue = min(minValue, value)
			} else {
				logger.WarnWithCtx(query.ctx).Msgf("could not convert value to int: %v, type: %T. Skipping", row.LastColValue(), row.LastColValue())
			}
		}
		resultValue = minValue
		// find keys with min value
		for _, row := range parentRows {
			if value, ok := util.ExtractInt64Maybe(row.LastColValue()); ok && value == minValue {
				resultKeys = append(resultKeys, query.getKey(row))
			}
		}
	}

	resultRow := parentRows[0].Copy()
	resultRow.Cols[len(resultRow.Cols)-1].Value = model.JsonMap{
		"value": resultValue,
		"keys":  resultKeys,
	}
	return resultRow
}

func (query MinBucket) String() string {
	return fmt.Sprintf("min_bucket(%s)", query.Parent)
}

func (query MinBucket) PipelineAggregationType() model.PipelineAggregationType {
	return model.PipelineSiblingAggregation
}
