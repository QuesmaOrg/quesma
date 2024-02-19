package bucket_aggregations

import "mitmproxy/quesma/model"

type QueryTypeTerms struct{}

func (qt QueryTypeTerms) IsBucketAggregation() bool {
	return true
}

func (qt QueryTypeTerms) TranslateSqlResponseToJson(rows []model.QueryResultRow) []model.JsonMap {
	response := make([]model.JsonMap, len(rows))
	for i, row := range rows {
		response[i] = make(model.JsonMap, len(row.Cols))
		for _, col := range row.Cols {
			response[i][col.ColName] = col.Value
		}
	}
	return response
}

func (qt QueryTypeTerms) String() string {
	return "terms"
}
