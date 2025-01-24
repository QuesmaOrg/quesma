// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package metrics_aggregations

import (
	"context"
	"github.com/QuesmaOrg/quesma/quesma/logger"
	"github.com/QuesmaOrg/quesma/quesma/model"
)

type Count struct {
	ctx context.Context
}

func NewCount(ctx context.Context) Count {
	return Count{ctx: ctx}
}

func (query Count) AggregationType() model.AggregationType {
	return model.MetricsAggregation
}

func (query Count) TranslateSqlResponseToJson(rows []model.QueryResultRow) model.JsonMap {
	if len(rows) == 0 {
		logger.WarnWithCtx(query.ctx).Msg("no rows returned for count aggregation")
		return make(model.JsonMap, 0)
	}
	if len(rows) > 1 {
		logger.WarnWithCtx(query.ctx).Msg("More than one row returned for count aggregation")
	}
	return model.JsonMap{"doc_count": rows[0].Cols[0]}
}

func (query Count) String() string {
	return "count"
}
