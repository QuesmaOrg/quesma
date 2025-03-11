// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package metrics_aggregations

import (
	"context"
	"github.com/QuesmaOrg/quesma/platform/clickhouse"
	"github.com/QuesmaOrg/quesma/platform/model"
)

type Min struct {
	ctx       context.Context
	fieldType clickhouse.DateTimeType
}

func NewMin(ctx context.Context, fieldType clickhouse.DateTimeType) Min {
	return Min{ctx: ctx, fieldType: fieldType}
}

func (query Min) AggregationType() model.AggregationType {
	return model.MetricsAggregation
}

func (query Min) TranslateSqlResponseToJson(rows []model.QueryResultRow) model.JsonMap {
	return metricsTranslateSqlResponseToJsonWithFieldTypeCheck(query.ctx, rows, query.fieldType)
}

func (query Min) String() string {
	return "min"
}
