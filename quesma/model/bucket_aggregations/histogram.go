package bucket_aggregations

import "mitmproxy/quesma/model"

type QueryTypeHistogram struct{}

func (qt QueryTypeHistogram) IsBucketAggregation() bool {
	return true
}

func (qt QueryTypeHistogram) TranslateSqlResponseToJson(rows []model.QueryResultRow, level int) []model.JsonMap {
	var response []model.JsonMap
	for _, row := range rows {
		response = append(response, model.JsonMap{
			"key":       row.Cols[level-1].Value,
			"doc_count": row.Cols[level].Value,
		})
	}
	return response
}

func (qt QueryTypeHistogram) String() string {
	return "histogram"
}
