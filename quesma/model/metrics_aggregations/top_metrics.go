package metrics_aggregations

import "mitmproxy/quesma/model"

type TopMetrics struct{}

func (query TopMetrics) IsBucketAggregation() bool {
	return false
}

// TODO implement correct
func (query TopMetrics) TranslateSqlResponseToJson(rows []model.QueryResultRow, level int) []model.JsonMap {
	return []model.JsonMap{{
		"avg": rows[0].Cols[level].Value,
	}}
}

func (query TopMetrics) String() string {
	return "top_metrics"
}
