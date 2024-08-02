// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package bucket_aggregations

import (
	"context"
	"quesma/model"
)

// TODO proper implementation, now don't do any sampling here
type Sampler struct {
	ctx context.Context
}

func NewSampler(ctx context.Context) Sampler {
	return Sampler{ctx: ctx}
}

func (query Sampler) AggregationType() model.AggregationType {
	return model.BucketAggregation
}

func (query Sampler) TranslateSqlResponseToJson(rows []model.QueryResultRow, level int) model.JsonMap {
	return model.JsonMap{}
}

func (query Sampler) String() string {
	return "sampler"
}
