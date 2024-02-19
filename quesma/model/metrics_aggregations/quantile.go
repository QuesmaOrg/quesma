package metrics_aggregations

import (
	"mitmproxy/quesma/model"
)

type QueryTypeQuantile struct{}

func (qt QueryTypeQuantile) IsBucketAggregation() bool {
	return false
}

// TODO implement correct
func (qt QueryTypeQuantile) TranslateSqlResponseToJson(rows []model.QueryResultRow) []model.JsonMap {
	return model.MetricsTranslateSqlResponseToJson(rows)
}

func (qt QueryTypeQuantile) String() string {
	return "quantile"
}
