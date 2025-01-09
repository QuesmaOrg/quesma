// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package bucket_aggregations

import (
	"context"
	"fmt"
	"github.com/QuesmaOrg/quesma/quesma/logger"
	"github.com/QuesmaOrg/quesma/quesma/model"
)

const shardSizeToSampleLimitRatio = 4

type Sampler struct {
	ctx  context.Context
	size int // "shard_size" from the request. We do 'LIMIT size' in the SQL query (currently only if sampler is top-most aggregation)
}

func NewSampler(ctx context.Context, size int) Sampler {
	return Sampler{ctx: ctx, size: size}
}

func (query Sampler) AggregationType() model.AggregationType {
	return model.BucketAggregation
}

func (query Sampler) TranslateSqlResponseToJson(rows []model.QueryResultRow) model.JsonMap {
	if len(rows) == 0 {
		logger.WarnWithCtx(query.ctx).Msg("no rows returned for sampler")
		return make(model.JsonMap, 0)
	}
	return model.JsonMap{"doc_count": rows[0].Cols[0].Value}
}

func (query Sampler) String() string {
	return fmt.Sprintf("%s(size: %d)", "sampler", query.size)
}

func (query Sampler) GetSampleLimit() int {
	return shardSizeToSampleLimitRatio * query.size
}

func (query Sampler) DoesNotHaveGroupBy() bool {
	return true
}
