package typical_queries

import (
	"context"
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
	return make([]model.JsonMap, 0)
}

func (query Count) String() string {
	return "count (non-aggregation)"
}

func (query Count) PostprocessResults(rowsFromDB []model.QueryResultRow) []model.QueryResultRow {
	return rowsFromDB
}
