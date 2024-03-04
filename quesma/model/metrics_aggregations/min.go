package metrics_aggregations

import (
	"mitmproxy/quesma/model"
)

type Min struct{}

func (query Min) IsBucketAggregation() bool {
	return false
}

func (query Min) TranslateSqlResponseToJson(rows []model.QueryResultRow, level int) []model.JsonMap {
	return metricsTranslateSqlResponseToJson(rows, level)
}

func (query Min) String() string {
	return "min"
}
