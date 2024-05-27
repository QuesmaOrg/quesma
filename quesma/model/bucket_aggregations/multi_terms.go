package bucket_aggregations

import (
	"context"
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/model"
)

type MultiTerms struct {
	ctx         context.Context
	significant bool // true <=> significant_terms, false <=> terms
}

func NewMultiTerms(ctx context.Context, significant bool) Terms {
	return Terms{ctx: ctx, significant: significant}
}

func (query MultiTerms) IsBucketAggregation() bool {
	return true
}

func (query MultiTerms) TranslateSqlResponseToJson(rows []model.QueryResultRow, level int) []model.JsonMap {
	var response []model.JsonMap
	if len(rows) > 0 && len(rows[0].Cols) < 2 {
		logger.ErrorWithCtx(query.ctx).Msgf(
			"unexpected number of columns in terms aggregation response, len: %d, rows[0]: %v", len(rows[0].Cols), rows[0])
	}
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

func (query MultiTerms) String() string {
	if !query.significant {
		return "terms"
	}
	return "significant_terms"
}

func (query MultiTerms) PostprocessResults(rowsFromDB []model.QueryResultRow) []model.QueryResultRow {
	return rowsFromDB
}
