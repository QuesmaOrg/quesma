// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package metrics_aggregations

import (
	"context"
	"quesma/clickhouse"
	"quesma/model"
)

type Min struct {
	ctx       context.Context
	fieldType clickhouse.DateTimeType
}

func NewMin(ctx context.Context, fieldType clickhouse.DateTimeType) Min {
	return Min{ctx: ctx, fieldType: fieldType}
}

func (query Min) IsBucketAggregation() bool {
	return false
}

func (query Min) TranslateSqlResponseToJson(rows []model.QueryResultRow, level int) []model.JsonMap {
	return metricsTranslateSqlResponseToJsonWithFieldTypeCheck(query.ctx, rows, level, query.fieldType)
}

func (query Min) String() string {
	return "min"
}

func (query Min) PostprocessResults(rowsFromDB []model.QueryResultRow) []model.QueryResultRow {
	return rowsFromDB
}
