package metrics_aggregations

import (
	"mitmproxy/quesma/model"
)

type QueryTypeCardinality struct{}

func (qt QueryTypeCardinality) IsBucketAggregation() bool {
	return false
}

func (qt QueryTypeCardinality) TranslateSqlResponseToJson(rows []model.QueryResultRow) []model.JsonMap {
	return model.MetricsTranslateSqlResponseToJson(rows)
}

func (qt QueryTypeCardinality) String() string {
	return "cardinality"
}
