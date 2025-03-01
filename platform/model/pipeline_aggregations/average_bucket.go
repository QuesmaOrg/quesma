// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package pipeline_aggregations

import (
	"context"
	"fmt"
	"github.com/QuesmaOrg/quesma/platform/logger"
	"github.com/QuesmaOrg/quesma/platform/model"
	"github.com/QuesmaOrg/quesma/platform/util"
)

type AverageBucket struct {
	*PipelineAggregation
}

func NewAverageBucket(ctx context.Context, bucketsPath string) AverageBucket {
	return AverageBucket{PipelineAggregation: newPipelineAggregation(ctx, bucketsPath)}
}

func (query AverageBucket) AggregationType() model.AggregationType {
	return model.PipelineMetricsAggregation
}

func (query AverageBucket) TranslateSqlResponseToJson(rows []model.QueryResultRow) model.JsonMap {
	return translateSqlResponseToJsonCommon(query.ctx, rows, query.String())
}

func (query AverageBucket) CalculateResultWhenMissing(parentRows []model.QueryResultRow) []model.QueryResultRow {
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
		resultRows = append(resultRows, query.calculateSingleAvgBucket(parentRowsOneBucket))
	}
	return resultRows
}

// we're sure len(parentRows) > 0
func (query AverageBucket) calculateSingleAvgBucket(parentRows []model.QueryResultRow) model.QueryResultRow {
	if len(parentRows) == 0 {
		logger.WarnWithCtx(query.ctx).Msg("no parent rows, should NEVER happen")
		return model.QueryResultRow{}
	}

	var resultValue float64
	rowsCnt := 0
	if _, firstRowValueIsFloat := util.ExtractFloat64Maybe(parentRows[0].LastColValue()); firstRowValueIsFloat {
		sum := 0.0
		for _, parentRow := range parentRows {
			value, ok := util.ExtractFloat64Maybe(parentRow.LastColValue())
			if ok {
				sum += value
				rowsCnt++
			} else {
				logger.WarnWithCtx(query.ctx).Msgf("could not convert value to float: %v, type: %T. Skipping", parentRow.LastColValue(), parentRow.LastColValue())
			}
		}
		resultValue = sum / float64(rowsCnt)
	} else {
		var sum int64
		for _, parentRow := range parentRows {
			value, ok := util.ExtractInt64Maybe(parentRow.LastColValue())
			if ok {
				sum += value
				rowsCnt++
			} else {
				logger.WarnWithCtx(query.ctx).Msgf("could not convert value to int: %v, type: %T. Skipping", parentRow.LastColValue(), parentRow.LastColValue())
			}
		}
		resultValue = float64(sum) / float64(rowsCnt)
	}

	resultRow := parentRows[0].Copy()
	resultRow.Cols[len(resultRow.Cols)-1].Value = resultValue
	return resultRow
}

func (query AverageBucket) String() string {
	return fmt.Sprintf("avg_bucket(%s)", query.Parent)
}

func (query AverageBucket) PipelineAggregationType() model.PipelineAggregationType {
	return model.PipelineSiblingAggregation
}
