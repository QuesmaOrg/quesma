package metrics_aggregations

import (
	"context"
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/model"
)

type ValueCount struct {
	ctx context.Context
}

func NewValueCount(ctx context.Context) ValueCount {
	return ValueCount{ctx: ctx}
}

func (query ValueCount) IsBucketAggregation() bool {
	return false
}

func (query ValueCount) TranslateSqlResponseToJson(rows []model.QueryResultRow, level int) []model.JsonMap {
	var value any = nil
	if len(rows) > 0 {
		value = rows[0].Cols[level].Value
	} else {
		logger.WarnWithCtx(query.ctx).Msg("Nn rows returned for value_count aggregation")
	}
	return []model.JsonMap{{
		"value": value,
	}}
}

func (query ValueCount) String() string {
	return "value_count"
}
