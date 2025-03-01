// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package metrics_aggregations

import (
	"context"
	"github.com/QuesmaOrg/quesma/platform/clickhouse"
	"github.com/QuesmaOrg/quesma/platform/model"
)

type Sum struct {
	ctx       context.Context
	fieldType clickhouse.DateTimeType
}

func NewSum(ctx context.Context, fieldType clickhouse.DateTimeType) Sum {
	return Sum{ctx: ctx, fieldType: fieldType}
}

func (query Sum) AggregationType() model.AggregationType {
	return model.MetricsAggregation
}

func (query Sum) TranslateSqlResponseToJson(rows []model.QueryResultRow) model.JsonMap {
	return metricsTranslateSqlResponseToJsonWithFieldTypeCheck(query.ctx, rows, query.fieldType)
}

func (query Sum) String() string {
	return "sum"
}
