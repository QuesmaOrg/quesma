package metrics_aggregations

import (
	"mitmproxy/quesma/model"
)

type QueryTypeAvg struct{}

func (qt QueryTypeAvg) IsBucketAggregation() bool {
	return false
}

func (qt QueryTypeAvg) TranslateSqlResponseToJson(rows []model.QueryResultRow, level int) []model.JsonMap {
	return metricsTranslateSqlResponseToJson(rows, level)
}

func (qt QueryTypeAvg) String() string {
	return "avg"
}
