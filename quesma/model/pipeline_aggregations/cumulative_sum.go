// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package pipeline_aggregations

import (
	"context"
	"fmt"
	"github.com/QuesmaOrg/quesma/quesma/logger"
	"github.com/QuesmaOrg/quesma/quesma/model"
	"github.com/QuesmaOrg/quesma/quesma/util"
)

// We fully support this aggregation.
// Description: A parent pipeline aggregation which calculates the cumulative sum of a specified metric
// in a parent histogram (or date_histogram) aggregation.
// The specified metric must be numeric and the enclosing histogram must have min_doc_count set to 0 (default for histogram aggregations).
// https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-pipeline-cumulative-sum-aggregation.html

type CumulativeSum struct {
	*PipelineAggregation
}

func NewCumulativeSum(ctx context.Context, bucketsPath string) CumulativeSum {
	return CumulativeSum{PipelineAggregation: newPipelineAggregation(ctx, bucketsPath)}
}

func (query CumulativeSum) AggregationType() model.AggregationType {
	return model.PipelineBucketAggregation
}

func (query CumulativeSum) TranslateSqlResponseToJson(rows []model.QueryResultRow) model.JsonMap {
	return translateSqlResponseToJsonCommon(query.ctx, rows, query.String())
}

func (query CumulativeSum) CalculateResultWhenMissing(parentRows []model.QueryResultRow) []model.QueryResultRow {
	resultRows := make([]model.QueryResultRow, 0, len(parentRows))
	if len(parentRows) == 0 {
		return resultRows
	}

	if _, firstRowValueIsFloat := util.ExtractFloat64Maybe(parentRows[0].LastColValue()); firstRowValueIsFloat {
		sum := 0.0
		for _, parentRow := range parentRows {
			value, ok := util.ExtractFloat64Maybe(parentRow.LastColValue())
			if ok {
				sum += value
			} else {
				logger.WarnWithCtx(query.ctx).Msgf("could not convert value to float: %v, type: %T. Skipping", parentRow.LastColValue(), parentRow.LastColValue())
			}
			resultRow := parentRow.Copy()
			resultRow.Cols[len(resultRow.Cols)-1].Value = sum
			resultRows = append(resultRows, resultRow)
		}
	} else { // cumulative sum must be on numeric, so if it's not float64, it should always be int
		var sum int64
		for _, parentRow := range parentRows {
			value, ok := util.ExtractInt64Maybe(parentRow.LastColValue())
			if ok {
				sum += value
			} else {
				logger.WarnWithCtx(query.ctx).Msgf("could not convert value to int: %v, type: %T. Skipping", parentRow.LastColValue(), parentRow.LastColValue())
			}
			resultRow := parentRow.Copy()
			resultRow.Cols[len(resultRow.Cols)-1].Value = sum
			resultRows = append(resultRows, resultRow)
		}
	}

	return resultRows
}

func (query CumulativeSum) String() string {
	return fmt.Sprintf("cumulative_sum(%s)", query.Parent)
}

func (query CumulativeSum) PipelineAggregationType() model.PipelineAggregationType {
	return model.PipelineParentAggregation
}
