package metrics_aggregations

import "mitmproxy/quesma/model"

type QueryTypeTopMetrics struct{}

func (qt QueryTypeTopMetrics) IsBucketAggregation() bool {
	return false
}

// TODO implement correct
func (qt QueryTypeTopMetrics) TranslateSqlResponseToJson(rows []model.QueryResultRow) []model.JsonMap {
	return []model.JsonMap{{
		"avg": rows[0].Cols[0].Value,
	}}
}

func (qt QueryTypeTopMetrics) String() string {
	return "top_metrics"
}
