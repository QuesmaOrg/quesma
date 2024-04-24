package metrics_aggregations

import (
	"context"
	"mitmproxy/quesma/model"
)

type Sum struct {
	ctx context.Context
}

func NewSum(ctx context.Context) Sum {
	return Sum{ctx: ctx}
}

func (query Sum) IsBucketAggregation() bool {
	return false
}

func (query Sum) TranslateSqlResponseToJson(rows []model.QueryResultRow, level int) []model.JsonMap {
	return metricsTranslateSqlResponseToJson(query.ctx, rows, level)
}

func (query Sum) String() string {
	return "sum"
}
