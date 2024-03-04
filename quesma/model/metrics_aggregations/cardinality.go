package metrics_aggregations

import (
	"mitmproxy/quesma/model"
)

type Cardinality struct{}

func (query Cardinality) IsBucketAggregation() bool {
	return false
}

func (query Cardinality) TranslateSqlResponseToJson(rows []model.QueryResultRow, level int) []model.JsonMap {
	return metricsTranslateSqlResponseToJson(rows, level)
}

func (query Cardinality) String() string {
	return "cardinality"
}
