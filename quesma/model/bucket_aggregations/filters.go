package bucket_aggregations

import (
	"mitmproxy/quesma/model"
)

type Filters struct {
}

func (query Filters) IsBucketAggregation() bool {
	return true
}

func (query Filters) TranslateSqlResponseToJson(rows []model.QueryResultRow, level int) []model.JsonMap {
	var value any = nil
	if len(rows) > 0 {
		value = rows[0].Cols[len(rows[0].Cols)-1].Value
	}
	return []model.JsonMap{{
		"doc_count": value,
	}}
}

func (query Filters) String() string {
	return "filters"
}
