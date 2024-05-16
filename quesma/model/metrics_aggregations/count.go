package metrics_aggregations

import (
	"context"
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/model"
)

type Count struct {
	ctx context.Context
}

func NewCount(ctx context.Context) Count {
	return Count{ctx: ctx}
}

func (query Count) IsBucketAggregation() bool {
	return false
}

func (query Count) TranslateSqlResponseToJson(rows []model.QueryResultRow, level int) []model.JsonMap {
	var response []model.JsonMap
	if len(rows) == 0 {
		logger.WarnWithCtx(query.ctx).Msg("no rows returned for count aggregation")
	}
	for _, row := range rows {
		response = append(response, model.JsonMap{"doc_count": row.Cols[level].Value})
	}
	return response
}

func (query Count) String() string {
	return "count"
}

func (query Count) PostprocessResults(rowsFromDB []model.QueryResultRow) []model.QueryResultRow {
	return rowsFromDB
}
