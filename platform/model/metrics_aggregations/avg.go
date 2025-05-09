// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package metrics_aggregations

import (
	"context"
	"github.com/QuesmaOrg/quesma/platform/clickhouse"
	"github.com/QuesmaOrg/quesma/platform/model"
)

type Avg struct {
	ctx       context.Context
	fieldType clickhouse.DateTimeType
}

func NewAvg(ctx context.Context, fieldType clickhouse.DateTimeType) Avg {
	return Avg{ctx: ctx, fieldType: fieldType}
}

func (query Avg) AggregationType() model.AggregationType {
	return model.MetricsAggregation
}

func (query Avg) TranslateSqlResponseToJson(rows []model.QueryResultRow) model.JsonMap {
	return metricsTranslateSqlResponseToJsonWithFieldTypeCheck(query.ctx, rows, query.fieldType)
}

func (query Avg) String() string {
	return "avg"
}
