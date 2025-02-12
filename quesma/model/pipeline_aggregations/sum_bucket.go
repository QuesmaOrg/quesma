// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package pipeline_aggregations

import (
	"context"
	"fmt"
	"github.com/QuesmaOrg/quesma/quesma/logger"
	"github.com/QuesmaOrg/quesma/quesma/model"
	"github.com/QuesmaOrg/quesma/quesma/util"
	"time"
)

type SumBucket struct {
	*PipelineAggregation
}

func NewSumBucket(ctx context.Context, bucketsPath string) SumBucket {
	return SumBucket{PipelineAggregation: newPipelineAggregation(ctx, bucketsPath)}
}

func (query SumBucket) AggregationType() model.AggregationType {
	return model.PipelineMetricsAggregation
}

func (query SumBucket) TranslateSqlResponseToJson(rows []model.QueryResultRow) model.JsonMap {
	if len(rows) == 0 {
		logger.WarnWithCtx(query.ctx).Msg("no rows returned for average bucket aggregation")
		return model.JsonMap{}
	}
	if len(rows) > 1 {
		logger.WarnWithCtx(query.ctx).Msg("more than one row returned for average bucket aggregation")
	}
	if returnMap, ok := rows[0].LastColValue().(model.JsonMap); ok {
		return returnMap
	}
	logger.WarnWithCtx(query.ctx).Msgf("could not convert value to JsonMap: %v, type: %T", rows[0].LastColValue(), rows[0].LastColValue())
	return model.JsonMap{}
}

func (query SumBucket) CalculateResultWhenMissing(parentRows []model.QueryResultRow) []model.QueryResultRow {
	resultRows := make([]model.QueryResultRow, 0)
	if len(parentRows) == 0 {
		return resultRows // maybe null?
	}
	parentFieldsCnt := len(parentRows[0].Cols) - 2 // -2, because row is [parent_cols..., current_key, current_value]
	// in calculateSingleAvgBucket we calculate avg all current_keys with the same parent_cols
	// so we need to split into buckets based on parent_cols
	if parentFieldsCnt < 0 {
		logger.WarnWithCtx(query.ctx).Msgf("parentFieldsCnt is less than 0: %d", parentFieldsCnt)
	}
	for _, parentRowsOneBucket := range model.SplitResultSetIntoBuckets(parentRows, parentFieldsCnt) {
		resultRows = append(resultRows, query.calculateSingleSumBucket(parentRowsOneBucket))
	}
	return resultRows
}

// we're sure len(parentRows) > 0
func (query SumBucket) calculateSingleSumBucket(parentRows []model.QueryResultRow) model.QueryResultRow {
	var resultValue any

	firstNonNilIndex := model.FirstNonNilIndex(parentRows)
	if firstNonNilIndex == -1 {
		resultRow := parentRows[0].Copy()
		resultRow.Cols[len(resultRow.Cols)-1].Value = model.JsonMap{
			"value": resultValue,
		}
		return resultRow
	}

	if firstRowValueFloat, firstRowValueIsFloat := util.ExtractFloat64Maybe(parentRows[firstNonNilIndex].LastColValue()); firstRowValueIsFloat {
		sum := firstRowValueFloat
		for _, row := range parentRows[firstNonNilIndex+1:] {
			if value, ok := util.ExtractFloat64Maybe(row.LastColValue()); ok {
				sum += value
			} else {
				logger.WarnWithCtx(query.ctx).Msgf("could not convert value to float: %v, type: %T. Skipping", row.LastColValue(), row.LastColValue())
			}
		}
		resultValue = sum
	} else if firstRowValueInt, firstRowValueIsInt := util.ExtractInt64Maybe(parentRows[firstNonNilIndex].LastColValue()); firstRowValueIsInt {
		sum := firstRowValueInt
		for _, row := range parentRows[firstNonNilIndex+1:] {
			if value, ok := util.ExtractInt64Maybe(row.LastColValue()); ok {
				sum += value
			} else {
				logger.WarnWithCtx(query.ctx).Msgf("could not convert value to int: %v, type: %T. Skipping", row.LastColValue(), row.LastColValue())
			}
		}
		resultValue = sum
	} else if firstRowValueTime, firstRowValueIsTime := parentRows[firstNonNilIndex].LastColValue().(time.Time); firstRowValueIsTime {
		sum := firstRowValueTime.UnixMilli()
		for _, row := range parentRows[firstNonNilIndex+1:] {
			if value, ok := row.LastColValue().(time.Time); ok {
				sum += value.UnixMilli()
			} else {
				logger.WarnWithCtx(query.ctx).Msgf("could not convert value to int: %v, type: %T. Skipping", row.LastColValue(), row.LastColValue())
			}
		}
		resultValue = sum
	} else {
		logger.WarnWithCtx(query.ctx).Msgf("could not convert value to float/int/date: %v, type: %T. Returning nil.",
			parentRows[firstNonNilIndex].LastColValue(), parentRows[firstNonNilIndex].LastColValue())
	}

	resultRow := parentRows[0].Copy()
	resultRow.Cols[len(resultRow.Cols)-1].Value = model.JsonMap{
		"value": resultValue,
	}
	return resultRow
}

func (query SumBucket) String() string {
	return fmt.Sprintf("sum_bucket(%s)", query.Parent)
}

func (query SumBucket) PipelineAggregationType() model.PipelineAggregationType {
	return model.PipelineSiblingAggregation
}
