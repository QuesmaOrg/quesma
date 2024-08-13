// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package bucket_aggregations

import (
	"context"
	"fmt"
	"quesma/model"
)

// RandomSampler
// We're missing one functionality from ElasticSearch, but it doesn't seem of any use for most of the users.
// Specifically, we're discarding an optional `seed` parameter of random_sample aggregation.
// Moreover, supporting truely random sampling would reduce the performance and does not make much sense in the context
// analytical SQL. Reading 64KB cost similarly as reading 1KB and skipping 63KB.
type RandomSampler struct {
	ctx         context.Context
	probability float64
	seed        int
}

func NewRandomSampler(ctx context.Context, probability float64, seed int) RandomSampler {
	return RandomSampler{ctx: ctx, probability: probability, seed: seed}
}

func (query RandomSampler) AggregationType() model.AggregationType {
	return model.BucketAggregation
}

func (query RandomSampler) TranslateSqlResponseToJson(rows []model.QueryResultRow, level int) model.JsonMap {
	panic("does not create new results") // eventually we should add count
}

func (query RandomSampler) String() string {
	return fmt.Sprintf("%s(probability: %f)", "random_sampler", query.probability)
}

func (query RandomSampler) GetSampleLimit() int {
	return 20000 // TODO temporary, to be fixed
}
