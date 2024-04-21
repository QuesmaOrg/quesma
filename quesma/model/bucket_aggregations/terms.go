package bucket_aggregations

import (
	"mitmproxy/quesma/model"
)

type Terms struct {
	significant bool // true <=> significant_terms, false <=> terms
}

func NewTerms(significant bool) Terms {
	return Terms{significant: significant}
}

func (query Terms) IsBucketAggregation() bool {
	return true
}

func (query Terms) TranslateSqlResponseToJson(rows []model.QueryResultRow, level int) []model.JsonMap {
	var response []model.JsonMap
	for _, row := range rows {
		docCount := row.Cols[len(row.Cols)-1].Value
		bucket := model.JsonMap{
			"key":       row.Cols[len(row.Cols)-2].Value,
			"doc_count": docCount,
		}
		if query.significant {
			bucket["score"] = docCount
			bucket["bg_count"] = docCount
		}
		response = append(response, bucket)
	}
	return response
}

func (query Terms) String() string {
	if !query.significant {
		return "terms"
	}
	return "significant_terms"
}
