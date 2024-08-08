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
// From ElasticSearch's docs: "When a seed is provided, the random subset of documents is the same between calls."
// Same seed should be working with us, because 'SAMPLE' clause we're using in Clickhouse is also deterministic.
// But an Elastic's user could send a request twice with different seed and obtain 2 different result sets, which is so far not possible with us.
type RandomSampler struct {
	ctx         context.Context
	probability float64
}

func NewRandomSampler(ctx context.Context, probability float64) RandomSampler {
	return RandomSampler{ctx: ctx, probability: probability}
}

func (query RandomSampler) AggregationType() model.AggregationType {
	return model.BucketAggregation
}

func (query RandomSampler) TranslateSqlResponseToJson(rows []model.QueryResultRow, level int) model.JsonMap {
	return nil
}

func (query RandomSampler) String() string {
	return fmt.Sprintf("%s(probability: %f)", "random_sampler", query.probability)
}

// TODO test with (random)sampler in the middle of request tree
