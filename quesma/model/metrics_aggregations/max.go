// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package metrics_aggregations

import (
	"context"
	"quesma/clickhouse"
	"quesma/model"
)

type Max struct {
	ctx       context.Context
	fieldType clickhouse.DateTimeType
}

func NewMax(ctx context.Context, fieldType clickhouse.DateTimeType) Max {
	return Max{ctx: ctx, fieldType: fieldType}
}

func (query Max) IsBucketAggregation() bool {
	return false
}

func (query Max) TranslateSqlResponseToJson(rows []model.QueryResultRow, level int) model.JsonMap {
	return metricsTranslateSqlResponseToJsonWithFieldTypeCheck(query.ctx, rows, level, query.fieldType)
}

func (query Max) String() string {
	return "max"
}

func (query Max) PostprocessResults(rowsFromDB []model.QueryResultRow) []model.QueryResultRow {
	return rowsFromDB
}

func (query Max) MetricsAggregation() {}

func (query Max) ColumnsNr() int {
	return 1
}
