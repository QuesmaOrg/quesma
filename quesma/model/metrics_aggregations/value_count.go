package metrics_aggregations

import (
	"mitmproxy/quesma/model"
)

type ValueCount struct{}

func (query ValueCount) IsBucketAggregation() bool {
	return false
}

func (query ValueCount) TranslateSqlResponseToJson(rows []model.QueryResultRow, level int) []model.JsonMap {
	var value any = nil
	if len(rows) > 0 {
		value = rows[0].Cols[level].Value
	}
	return []model.JsonMap{{
		"value": value,
	}}
}

func (query ValueCount) String() string {
	return "value_count"
}
