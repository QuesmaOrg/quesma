package metrics_aggregations

import (
	"mitmproxy/quesma/model"
)

type Avg struct{}

func (query Avg) IsBucketAggregation() bool {
	return false
}

func (query Avg) TranslateSqlResponseToJson(rows []model.QueryResultRow, level int) []model.JsonMap {
	return metricsTranslateSqlResponseToJson(rows, level)
}

func (query Avg) String() string {
	return "avg"
}
