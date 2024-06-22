package bucket_aggregations

import (
	"context"
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/model"
)

type Terms struct {
	ctx         context.Context
	significant bool // true <=> significant_terms, false <=> terms
	size        int
}

func NewTerms(ctx context.Context, significant bool, size int) Terms {
	return Terms{ctx: ctx, significant: significant, size: size}
}

func (query Terms) IsBucketAggregation() bool {
	return true
}

func (query Terms) TranslateSqlResponseToJson(rows []model.QueryResultRow, level int) []model.JsonMap {
	var response []model.JsonMap
	if len(rows) > 0 && len(rows[0].Cols) < 2 {
		logger.ErrorWithCtx(query.ctx).Msgf(
			"unexpected number of columns in terms aggregation response, len: %d, rows[0]: %v", len(rows[0].Cols), rows[0])
	}
	for i, row := range rows {
		if i >= query.size {
			break
		}
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
	// TODO: we should return 'doc_count_error_upper_bound' and too 'sum_other_doc_count' too
	return response
}

func (query Terms) String() string {
	if !query.significant {
		return "terms"
	}
	return "significant_terms"
}

func (query Terms) PostprocessResults(rowsFromDB []model.QueryResultRow) []model.QueryResultRow {
	return rowsFromDB
}
