package metrics_aggregations

import (
	"mitmproxy/quesma/model"
)

type QueryTypeMin struct{}

func (qt QueryTypeMin) IsBucketAggregation() bool {
	return false
}

func (qt QueryTypeMin) TranslateSqlResponseToJson(rows []model.QueryResultRow) []model.JsonMap {
	return model.MetricsTranslateSqlResponseToJson(rows)
}

func (qt QueryTypeMin) String() string {
	return "min"
}
