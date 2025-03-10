// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package pipeline_aggregations

import (
	"context"
	"fmt"
	"github.com/QuesmaOrg/quesma/platform/model"
)

type SerialDiff struct {
	lag int
	*PipelineAggregation
}

func NewSerialDiff(ctx context.Context, bucketsPath string, lag int) SerialDiff {
	return SerialDiff{lag: lag, PipelineAggregation: newPipelineAggregation(ctx, bucketsPath)}
}

func (query SerialDiff) AggregationType() model.AggregationType {
	return model.PipelineBucketAggregation
}

func (query SerialDiff) TranslateSqlResponseToJson(rows []model.QueryResultRow) model.JsonMap {
	return translateSqlResponseToJsonCommon(query.ctx, rows, query.String())
}

func (query SerialDiff) CalculateResultWhenMissing(parentRows []model.QueryResultRow) []model.QueryResultRow {
	return calculateResultWhenMissingCommonForDiffAggregations(query.ctx, parentRows, query.lag)
}

func (query SerialDiff) String() string {
	return fmt.Sprintf("serial_diff(parent: %s, lag: %d)", query.Parent, query.lag)
}

func (query SerialDiff) PipelineAggregationType() model.PipelineAggregationType {
	return model.PipelineParentAggregation
}
