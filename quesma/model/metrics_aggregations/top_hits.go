package metrics_aggregations

import "mitmproxy/quesma/model"

type TopHits struct{}

func (query TopHits) IsBucketAggregation() bool {
	return false
}

// TODO implement correct
func (query TopHits) TranslateSqlResponseToJson(rows []model.QueryResultRow, level int) []model.JsonMap {
	response := make([]model.JsonMap, len(rows))
	for i, row := range rows {
		response[i] = make(model.JsonMap, len(row.Cols))
		for _, col := range row.Cols[level:] {
			response[i][col.ColName] = col.Value
		}
	}
	return response
}

func (query TopHits) String() string {
	return "top_hits"
}
