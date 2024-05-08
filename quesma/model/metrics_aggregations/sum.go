package metrics_aggregations

import (
	"context"
	"mitmproxy/quesma/clickhouse"
	"mitmproxy/quesma/model"
)

type Sum struct {
	ctx       context.Context
	fieldType clickhouse.DateTimeType
}

func NewSum(ctx context.Context, fieldType clickhouse.DateTimeType) Sum {
	return Sum{ctx: ctx, fieldType: fieldType}
}

func (query Sum) IsBucketAggregation() bool {
	return false
}

func (query Sum) TranslateSqlResponseToJson(rows []model.QueryResultRow, level int) []model.JsonMap {
	return metricsTranslateSqlResponseToJsonWithFieldTypeCheck(query.ctx, rows, level, query.fieldType)
}

func (query Sum) CalculateResultIfMissing(model.QueryResultRow, []model.QueryResultRow) model.QueryResultRow {
	return model.QueryResultRow{}
}

func (query Sum) String() string {
	return "sum"
}
