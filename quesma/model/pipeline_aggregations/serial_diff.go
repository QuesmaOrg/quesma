// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package pipeline_aggregations

import (
	"context"
	"fmt"
	"quesma/model"
)

type SerialDiff struct {
	ctx context.Context
	PipelineAggregation
	lag int
}

func NewSerialDiff(ctx context.Context, bucketsPath string, lag int) SerialDiff {
	return SerialDiff{
		ctx:                 ctx,
		PipelineAggregation: newPipelineAggregation(ctx, bucketsPath),
		lag:                 lag,
	}
}

func (query SerialDiff) AggregationType() model.AggregationType {
	return model.PipelineAggregation
}

func (query SerialDiff) PipelineAggregationType() model.AggregationType {
	return model.BucketAggregation
}

func (query SerialDiff) TranslateSqlResponseToJson(rows []model.QueryResultRow, level int) model.JsonMap {
	return translateSqlResponseToJsonCommon(query.ctx, rows, query.String())
}

func (query SerialDiff) CalculateResultWhenMissing(parentRows []model.QueryResultRow) []model.QueryResultRow {
	return calculateResultWhenMissingCommonForDiffAggregations(query.ctx, parentRows, query.lag)
}

func (query SerialDiff) String() string {
	return fmt.Sprintf("serial_diff(parent: %s, lag: %d)", query.Parent, query.lag)
}
