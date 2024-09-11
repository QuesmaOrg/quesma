// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package pipeline_aggregations

import (
	"context"
	"quesma/logger"
	"quesma/model"
)

type BucketScript struct {
	ctx context.Context
}

func NewBucketScript(ctx context.Context) BucketScript {
	return BucketScript{ctx: ctx}
}

func (query BucketScript) AggregationType() model.AggregationType {
	return model.PipelineMetricsAggregation // not sure
}

func (query BucketScript) TranslateSqlResponseToJson(rows []model.QueryResultRow) model.JsonMap {
	if len(rows) == 0 {
		logger.WarnWithCtx(query.ctx).Msg("no rows returned for bucket script aggregation")
		return model.JsonMap{"value": 0}
	}
	var response []model.JsonMap
	for _, row := range rows {
		response = append(response, model.JsonMap{"value": row.Cols[0].Value})
	}
	return model.JsonMap{
		"buckets": response,
	}
}

func (query BucketScript) CalculateResultWhenMissing(*model.Query, []model.QueryResultRow) []model.QueryResultRow {
	return []model.QueryResultRow{}
}

func (query BucketScript) String() string {
	return "bucket script"
}
