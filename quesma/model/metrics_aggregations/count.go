package metrics_aggregations

import "mitmproxy/quesma/model"

type QueryTypeCount struct{}

func (qt QueryTypeCount) IsBucketAggregation() bool {
	return false
}

func (qt QueryTypeCount) TranslateSqlResponseToJson(rows []model.QueryResultRow) []model.JsonMap {
	var response []model.JsonMap
	for _, row := range rows {
		response = append(response, model.JsonMap{row.Cols[0].ColName: row.Cols[0].Value})
	}
	return response
}

func (qt QueryTypeCount) String() string {
	return "count"
}
