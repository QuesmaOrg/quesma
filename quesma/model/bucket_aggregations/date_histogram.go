package bucket_aggregations

import "mitmproxy/quesma/model"

type QueryTypeDateHistogram struct{}

func (qt QueryTypeDateHistogram) IsBucketAggregation() bool {
	return true
}

func (qt QueryTypeDateHistogram) TranslateSqlResponseToJson(rows []model.QueryResultRow, level int) []model.JsonMap {
	var response []model.JsonMap
	for _, row := range rows {
		response = append(response, model.JsonMap{
			"key":           row.Cols[level-1].Value,
			"doc_count":     row.Cols[level].Value,
			"key_as_string": row.Cols[level+1].Value,
		})
	}
	return response
}

func (qt QueryTypeDateHistogram) String() string {
	return "date_histogram"
}
