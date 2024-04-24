package metrics_aggregations

import (
	"context"
	"mitmproxy/quesma/model"
)

type Min struct {
	ctx context.Context
}

func NewMin(ctx context.Context) Min {
	return Min{ctx: ctx}
}

func (query Min) IsBucketAggregation() bool {
	return false
}

func (query Min) TranslateSqlResponseToJson(rows []model.QueryResultRow, level int) []model.JsonMap {
	return metricsTranslateSqlResponseToJson(query.ctx, rows, level)
}

func (query Min) String() string {
	return "min"
}
