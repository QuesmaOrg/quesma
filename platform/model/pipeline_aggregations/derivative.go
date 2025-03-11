// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package pipeline_aggregations

import (
	"context"
	"fmt"
	"github.com/QuesmaOrg/quesma/platform/model"
)

// Derivative is just Serial Diff, with lag = 1

const derivativeLag = 1

type Derivative struct {
	*PipelineAggregation
}

func NewDerivative(ctx context.Context, bucketsPath string) Derivative {
	return Derivative{PipelineAggregation: newPipelineAggregation(ctx, bucketsPath)}
}

func (query Derivative) AggregationType() model.AggregationType {
	return model.PipelineBucketAggregation
}

func (query Derivative) TranslateSqlResponseToJson(rows []model.QueryResultRow) model.JsonMap {
	return translateSqlResponseToJsonCommon(query.ctx, rows, query.String())
}

func (query Derivative) CalculateResultWhenMissing(parentRows []model.QueryResultRow) []model.QueryResultRow {
	return calculateResultWhenMissingCommonForDiffAggregations(query.ctx, parentRows, derivativeLag)
}

func (query Derivative) String() string {
	return fmt.Sprintf("derivative(%s)", query.Parent)
}

func (query Derivative) PipelineAggregationType() model.PipelineAggregationType {
	return model.PipelineParentAggregation
}
