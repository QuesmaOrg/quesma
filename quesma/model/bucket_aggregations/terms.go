package bucket_aggregations

import "mitmproxy/quesma/model"

type QueryTypeTerms struct{}

func (qt QueryTypeTerms) IsBucketAggregation() bool {
	return true
}

func (qt QueryTypeTerms) TranslateSqlResponseToJson(rows []model.QueryResultRow, level int) []model.JsonMap {
	var response []model.JsonMap
	for _, row := range rows {
		response = append(response, model.JsonMap{
			"key":       row.Cols[level-1].Value,
			"doc_count": row.Cols[level].Value,
		})
	}
	return response
}

func (qt QueryTypeTerms) String() string {
	return "terms"
}
