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
	ctx     context.Context
	Parent  string
	IsCount bool
}

func NewDerivative(ctx context.Context, bucketsPath string) Derivative {
	isCount := bucketsPath == BucketsPathCount
	return Derivative{ctx: ctx, Parent: bucketsPath, IsCount: isCount}
}

func (query Derivative) IsBucketAggregation() bool {
	return false
}

func (query Derivative) TranslateSqlResponseToJson(rows []model.QueryResultRow, level int) model.JsonMap {
	return translateSqlResponseToJsonCommon(query.ctx, rows, query.String())
}

func (query Derivative) CalculateResultWhenMissing(qwa *model.Query, parentRows []model.QueryResultRow) []model.QueryResultRow {
	return calculateResultWhenMissingCommonForDiffAggregations(query.ctx, parentRows, derivativeLag)
}

func (query Derivative) PostprocessResults(rowsFromDB []model.QueryResultRow) []model.QueryResultRow {
	return rowsFromDB
}

func (query Derivative) String() string {
	return fmt.Sprintf("derivative(%s)", query.Parent)
}
