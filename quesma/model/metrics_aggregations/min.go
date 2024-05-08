package metrics_aggregations

import (
	"context"
	"mitmproxy/quesma/clickhouse"
	"mitmproxy/quesma/model"
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

func (query Min) CalculateResultIfMissing(model.QueryResultRow, []model.QueryResultRow) model.QueryResultRow {
	return model.QueryResultRow{}
}

func (query Min) String() string {
	return "min"
}
