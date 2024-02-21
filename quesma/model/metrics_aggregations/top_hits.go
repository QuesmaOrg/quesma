package metrics_aggregations

import "mitmproxy/quesma/model"

type QueryTypeTopHits struct{}

func (qt QueryTypeTopHits) IsBucketAggregation() bool {
	return false
}

// TODO implement correct
func (qt QueryTypeTopHits) TranslateSqlResponseToJson(rows []model.QueryResultRow, level int) []model.JsonMap {
	response := make([]model.JsonMap, len(rows))
	for i, row := range rows {
		response[i] = make(model.JsonMap, len(row.Cols))
		for _, col := range row.Cols[level:] {
			response[i][col.ColName] = col.Value
		}
	}
	return response
}

func (qt QueryTypeTopHits) String() string {
	return "top_hits"
}
