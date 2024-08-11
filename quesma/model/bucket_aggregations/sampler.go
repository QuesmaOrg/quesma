// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package bucket_aggregations

import (
	"context"
	"fmt"
	"quesma/model"
)

type Sampler struct {
	ctx  context.Context
	size int // "shard_size" from the request
}

func NewSampler(ctx context.Context, size int) Sampler {
	return Sampler{ctx: ctx, size: size}
}

func (query Sampler) AggregationType() model.AggregationType {
	return model.BucketAggregation
}

func (query Sampler) TranslateSqlResponseToJson(rows []model.QueryResultRow, level int) model.JsonMap {
	return nil
}

func (query Sampler) String() string {
	return fmt.Sprintf("%s(size: %d)", "sampler", query.size)
}

func (query Sampler) GetSampleLimit() int {
	return query.size
}
