package metrics_aggregations

import (
	"context"
	"mitmproxy/quesma/clickhouse"
	"mitmproxy/quesma/model"
)

type Avg struct {
	ctx       context.Context
	fieldType clickhouse.DateTimeType
}

func NewAvg(ctx context.Context, fieldType clickhouse.DateTimeType) Avg {
	return Avg{ctx: ctx, fieldType: fieldType}
}

func (query Avg) IsBucketAggregation() bool {
	return false
}

func (query Avg) TranslateSqlResponseToJson(rows []model.QueryResultRow, level int) []model.JsonMap {
	return metricsTranslateSqlResponseToJsonWithFieldTypeCheck(query.ctx, rows, level, query.fieldType)
}

func (query Avg) String() string {
	return "avg"
}
