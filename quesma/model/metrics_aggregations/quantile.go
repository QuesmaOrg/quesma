package metrics_aggregations

import (
	"mitmproxy/quesma/model"
)

type Quantile struct{}

func (query Quantile) IsBucketAggregation() bool {
	return false
}

// TODO implement correct
func (query Quantile) TranslateSqlResponseToJson(rows []model.QueryResultRow, level int) []model.JsonMap {
	return metricsTranslateSqlResponseToJson(rows, level)
}

func (query Quantile) String() string {
	return "quantile"
}
