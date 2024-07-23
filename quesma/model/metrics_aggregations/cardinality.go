// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package metrics_aggregations

import (
	"context"
	"quesma/model"
)

type Cardinality struct {
	ctx context.Context
}

func NewCardinality(ctx context.Context) Cardinality {
	return Cardinality{ctx: ctx}
}

func (query Cardinality) AggregationType() model.AggregationType {
	return model.MetricsAggregation
}

func (query Cardinality) TranslateSqlResponseToJson(rows []model.QueryResultRow, level int) model.JsonMap {
	return metricsTranslateSqlResponseToJson(query.ctx, rows, level)
}

func (query Cardinality) String() string {
	return "cardinality"
}

