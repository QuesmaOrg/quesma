// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package metrics_aggregations

import (
	"context"
	"github.com/QuesmaOrg/quesma/platform/model"
)

type ValueCount struct {
	ctx context.Context
}

func NewValueCount(ctx context.Context) ValueCount {
	return ValueCount{ctx: ctx}
}

func (query ValueCount) AggregationType() model.AggregationType {
	return model.MetricsAggregation
}

func (query ValueCount) TranslateSqlResponseToJson(rows []model.QueryResultRow) model.JsonMap {
	return metricsTranslateSqlResponseToJsonZeroDefault(query.ctx, rows)
}

func (query ValueCount) String() string {
	return "value_count"
}
