package bucket_aggregations

import (
	"mitmproxy/quesma/model"
)

type Histogram struct{}

func (query Histogram) IsBucketAggregation() bool {
	return true
}

func (query Histogram) TranslateSqlResponseToJson(rows []model.QueryResultRow, level int) []model.JsonMap {
	var response []model.JsonMap
	for _, row := range rows {
		response = append(response, model.JsonMap{
			"key":       row.Cols[level-1].Value,
			"doc_count": row.Cols[level].Value,
		})
	}
	return response
}

func (query Histogram) String() string {
	return "histogram"
}
