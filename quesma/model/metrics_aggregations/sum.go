// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package metrics_aggregations

import (
	"context"
	"quesma/clickhouse"
	"quesma/model"
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

func (query Sum) TranslateSqlResponseToJson(rows []model.QueryResultRow, level int) model.JsonMap {
	return metricsTranslateSqlResponseToJsonWithFieldTypeCheck(query.ctx, rows, level, query.fieldType)
}

func (query Sum) String() string {
	return "sum"
}

func (query Sum) PostprocessResults(rowsFromDB []model.QueryResultRow) []model.QueryResultRow {
	return rowsFromDB
}
