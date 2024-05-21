package bucket_aggregations

import (
	"context"
	"mitmproxy/quesma/model"
)

type Sampler struct {
	ctx context.Context
}

func NewSampler(ctx context.Context) Sampler {
	return Sampler{ctx: ctx}
}

func (query Sampler) IsBucketAggregation() bool {
	return true
}

// won't be called for now
func (query Sampler) TranslateSqlResponseToJson(rows []model.QueryResultRow, level int) []model.JsonMap {
	return nil
}

func (query Sampler) String() string {
	return "sampler"
}

func (query Sampler) PostprocessResults(rowsFromDB []model.QueryResultRow) []model.QueryResultRow {
	return rowsFromDB
}
