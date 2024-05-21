package bucket_aggregations

import (
	"context"
	"mitmproxy/quesma/model"
)

type RandomSampler struct {
	ctx context.Context
}

func NewRandomSampler(ctx context.Context) RandomSampler {
	return RandomSampler{ctx: ctx}
}

func (query RandomSampler) IsBucketAggregation() bool {
	return true
}

// won't be called for now
func (query RandomSampler) TranslateSqlResponseToJson(rows []model.QueryResultRow, level int) []model.JsonMap {
	return nil
}

func (query RandomSampler) String() string {
	return "random_sampler"
}

func (query RandomSampler) PostprocessResults(rowsFromDB []model.QueryResultRow) []model.QueryResultRow {
	return rowsFromDB
}
