package bucket_aggregations

import (
	"context"
	"mitmproxy/quesma/model"
)

type Bool struct {
	ctx context.Context
	Sql model.SimpleQuery
}

func NewBool(ctx context.Context, sql model.SimpleQuery) Bool {
	return Bool{ctx: ctx, Sql: sql}
}

func (query Bool) IsBucketAggregation() bool {
	return true // let's say it's true
}

// won't be called
func (query Bool) TranslateSqlResponseToJson(rows []model.QueryResultRow, level int) []model.JsonMap {
	return nil
}

func (query Bool) String() string {
	return "bool"
}

func (query Bool) PostprocessResults(rowsFromDB []model.QueryResultRow) []model.QueryResultRow {
	return rowsFromDB
}
