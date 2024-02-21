package metrics_aggregations

import (
	"mitmproxy/quesma/model"
)

type QueryTypeCardinality struct{}

func (qt QueryTypeCardinality) IsBucketAggregation() bool {
	return false
}

func (qt QueryTypeCardinality) TranslateSqlResponseToJson(rows []model.QueryResultRow, level int) []model.JsonMap {
	return metricsTranslateSqlResponseToJson(rows, level)
}

func (qt QueryTypeCardinality) String() string {
	return "cardinality"
}
