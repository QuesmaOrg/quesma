package metrics_aggregations

import (
	"mitmproxy/quesma/model"
)

type QueryTypeMax struct{}

func (qt QueryTypeMax) IsBucketAggregation() bool {
	return false
}

func (qt QueryTypeMax) TranslateSqlResponseToJson(rows []model.QueryResultRow) []model.JsonMap {
	return model.MetricsTranslateSqlResponseToJson(rows)
}

func (qt QueryTypeMax) String() string {
	return "max"
}
