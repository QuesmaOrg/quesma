package bucket_aggregations

import (
	"context"
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/model"
)

type Histogram struct {
	ctx context.Context
}

func NewHistogram(ctx context.Context) Histogram {
	return Histogram{ctx: ctx}
}

func (query Histogram) IsBucketAggregation() bool {
	return true
}

func (query Histogram) TranslateSqlResponseToJson(rows []model.QueryResultRow, level int) []model.JsonMap {
	if len(rows) > 0 && len(rows[0].Cols) < 2 {
		logger.ErrorWithCtx(query.ctx).Msgf(
			"unexpected number of columns in histogram aggregation response, len(rows[0].Cols): "+
				"%d, level: %d", len(rows[0].Cols), level,
		)
	}
	var response []model.JsonMap
	for _, row := range rows {
		response = append(response, model.JsonMap{
			"key":       row.Cols[level-1].Value,
			"doc_count": row.Cols[level].Value,
		})
	}
	return response
}

func (query Histogram) String() string {
	return "histogram"
}
