package metrics_aggregations

import (
	"context"
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/model"
)

type TopHits struct {
	ctx context.Context
}

func NewTopHits(ctx context.Context) TopHits {
	return TopHits{ctx: ctx}
}

func (query TopHits) IsBucketAggregation() bool {
	return false
}

// TODO implement correct
func (query TopHits) TranslateSqlResponseToJson(rows []model.QueryResultRow, level int) []model.JsonMap {
	response := make([]model.JsonMap, len(rows))
	if len(rows) > 0 && level >= len(rows[0].Cols) {
		logger.WarnWithCtx(query.ctx).Msg("no columns returned for top_hits aggregation")
	}
	for i, row := range rows {
		response[i] = make(model.JsonMap, len(row.Cols)-level)
		for _, col := range row.Cols[level:] {
			response[i][col.ColName] = col.Value
		}
	}
	return response
}

func (query TopHits) String() string {
	return "top_hits"
}

func (query TopHits) PostprocessResults(rowsFromDB []model.QueryResultRow) []model.QueryResultRow {
	return rowsFromDB
}
