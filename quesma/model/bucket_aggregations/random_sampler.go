// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package bucket_aggregations

import (
	"context"
	"fmt"
	"github.com/QuesmaOrg/quesma/quesma/logger"
	"github.com/QuesmaOrg/quesma/quesma/model"
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

func (query RandomSampler) TranslateSqlResponseToJson(rows []model.QueryResultRow) model.JsonMap {
	if len(rows) == 0 {
		logger.WarnWithCtx(query.ctx).Msg("no rows returned for random sampler")
		return make(model.JsonMap, 0)
	}
	return model.JsonMap{"doc_count": rows[0].Cols[0].Value}
}

func (query RandomSampler) String() string {
	return fmt.Sprintf("%s(probability: %f)", "random_sampler", query.probability)
}

func (query RandomSampler) GetSampleLimit() int {
	return 20000 // TODO temporary, to be fixed
}

func (query RandomSampler) DoesNotHaveGroupBy() bool {
	return true
}
