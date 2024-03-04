package metrics_aggregations

import (
	"mitmproxy/quesma/model"
)

type Max struct{}

func (query Max) IsBucketAggregation() bool {
	return false
}

func (query Max) TranslateSqlResponseToJson(rows []model.QueryResultRow, level int) []model.JsonMap {
	return metricsTranslateSqlResponseToJson(rows, level)
}

func (query Max) String() string {
	return "max"
}
