// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package pipeline_aggregations

import (
	"context"
	"fmt"
	"quesma/model"
)

// Derivative is just Serial Diff, with lag = 1

const derivativeLag = 1

type Derivative struct {
	ctx context.Context
	PipelineAggregation
}

func NewDerivative(ctx context.Context, bucketsPath string) Derivative {
	return Derivative{ctx: ctx, PipelineAggregation: newPipelineAggregation(ctx, bucketsPath)}
}

func (query Derivative) AggregationType() model.AggregationType {
	return model.PipelineAggregation
}

func (query Derivative) PipelineAggregationType() model.AggregationType {
	return model.BucketAggregation
}

func (query Derivative) TranslateSqlResponseToJson(rows []model.QueryResultRow, level int) model.JsonMap {
	return translateSqlResponseToJsonCommon(query.ctx, rows, query.String())
}

func (query Derivative) CalculateResultWhenMissing(parentRows []model.QueryResultRow) []model.QueryResultRow {
	return calculateResultWhenMissingCommonForDiffAggregations(query.ctx, parentRows, derivativeLag)
}

func (query Derivative) String() string {
	return fmt.Sprintf("derivative(%s)", query.Parent)
}
