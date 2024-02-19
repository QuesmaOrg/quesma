package metrics_aggregations

import (
	"mitmproxy/quesma/model"
)

type QueryTypeSum struct{}

func (qt QueryTypeSum) IsBucketAggregation() bool {
	return false
}

func (qt QueryTypeSum) TranslateSqlResponseToJson(rows []model.QueryResultRow) []model.JsonMap {
	return model.MetricsTranslateSqlResponseToJson(rows)
}

func (qt QueryTypeSum) String() string {
	return "sum"
}
