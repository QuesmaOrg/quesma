package metrics_aggregations

import "mitmproxy/quesma/model"

type QueryTypeCount struct{}

func (qt QueryTypeCount) IsBucketAggregation() bool {
	return false
}

// TODO is that correct? If so, why not bucket? Seems to work now...
func (qt QueryTypeCount) TranslateSqlResponseToJson(rows []model.QueryResultRow, level int) []model.JsonMap {
	var response []model.JsonMap
	for _, row := range rows {
		response = append(response, model.JsonMap{row.Cols[level].ColName: row.Cols[level].Value})
	}
	return response
}

func (qt QueryTypeCount) String() string {
	return "count"
}
