package bucket_aggregations

import (
	"mitmproxy/quesma/model"
)

type Terms struct{}

func (query Terms) IsBucketAggregation() bool {
	return true
}

func (query Terms) TranslateSqlResponseToJson(rows []model.QueryResultRow, level int) []model.JsonMap {
	var response []model.JsonMap
	for _, row := range rows {
		response = append(response, model.JsonMap{
			"key":       row.Cols[len(row.Cols)-2].Value,
			"doc_count": row.Cols[len(row.Cols)-1].Value,
		})
	}
	return response
}

func (query Terms) String() string {
	return "terms"
}
