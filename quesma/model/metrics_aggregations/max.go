package metrics_aggregations

import (
	"context"
	"mitmproxy/quesma/clickhouse"
	"mitmproxy/quesma/model"
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

func (query Max) TranslateSqlResponseToJson(rows []model.QueryResultRow, level int) []model.JsonMap {
	return metricsTranslateSqlResponseToJsonWithFieldTypeCheck(query.ctx, rows, level, query.fieldType)
}

func (query Max) CalculateResultIfMissing(model.QueryResultRow, []model.QueryResultRow) model.QueryResultRow {
	return model.QueryResultRow{}
}

func (query Max) String() string {
	return "max"
}
