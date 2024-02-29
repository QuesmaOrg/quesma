package metrics_aggregations

import (
	"mitmproxy/quesma/model"
)

type QueryTypeValueCount struct{}

func (qt QueryTypeValueCount) IsBucketAggregation() bool {
	return false
}

func (qt QueryTypeValueCount) TranslateSqlResponseToJson(rows []model.QueryResultRow, level int) []model.JsonMap {
	var value any = nil
	if len(rows) > 0 {
		value = rows[0].Cols[level].Value
	}
	return []model.JsonMap{{
		"value": value,
	}}
}

func (qt QueryTypeValueCount) String() string {
	return "value_count"
}
