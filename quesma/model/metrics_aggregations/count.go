package metrics_aggregations

import (
	"mitmproxy/quesma/model"
)

type Count struct{}

func (query Count) IsBucketAggregation() bool {
	return false
}

func (query Count) TranslateSqlResponseToJson(rows []model.QueryResultRow, level int) []model.JsonMap {
	var response []model.JsonMap
	for _, row := range rows {
		response = append(response, model.JsonMap{row.Cols[level].ColName: row.Cols[level].Value})
	}
	return response
}

func (query Count) String() string {
	return "count"
}
