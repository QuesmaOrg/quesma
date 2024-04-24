package metrics_aggregations

import (
	"context"
	"mitmproxy/quesma/model"
)

type Max struct {
	ctx context.Context
}

func NewMax(ctx context.Context) Max {
	return Max{ctx: ctx}
}

func (query Max) IsBucketAggregation() bool {
	return false
}

func (query Max) TranslateSqlResponseToJson(rows []model.QueryResultRow, level int) []model.JsonMap {
	return metricsTranslateSqlResponseToJson(query.ctx, rows, level)
}

func (query Max) String() string {
	return "max"
}
