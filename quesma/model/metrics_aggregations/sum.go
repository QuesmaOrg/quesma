package metrics_aggregations

import (
	"mitmproxy/quesma/model"
)

type Sum struct{}

func (query Sum) IsBucketAggregation() bool {
	return false
}

func (query Sum) TranslateSqlResponseToJson(rows []model.QueryResultRow, level int) []model.JsonMap {
	return metricsTranslateSqlResponseToJson(rows, level)
}

func (query Sum) String() string {
	return "sum"
}
