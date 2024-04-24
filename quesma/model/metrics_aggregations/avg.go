package metrics_aggregations

import (
	"context"
	"mitmproxy/quesma/model"
)

type Avg struct {
	ctx context.Context
}

func NewAvg(ctx context.Context) Avg {
	return Avg{ctx: ctx}
}

func (query Avg) IsBucketAggregation() bool {
	return false
}

func (query Avg) TranslateSqlResponseToJson(rows []model.QueryResultRow, level int) []model.JsonMap {
	return metricsTranslateSqlResponseToJson(query.ctx, rows, level)
}

func (query Avg) String() string {
	return "avg"
}
