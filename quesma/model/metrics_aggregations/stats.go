// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package metrics_aggregations

import (
	"context"
	"github.com/QuesmaOrg/quesma/quesma/logger"
	"github.com/QuesmaOrg/quesma/quesma/model"
)

type Stats struct {
	ctx context.Context
}

func NewStats(ctx context.Context) Stats {
	return Stats{ctx: ctx}
}

func (query Stats) AggregationType() model.AggregationType {
	return model.MetricsAggregation
}

var statsColumnsInOrder = []string{"count", "min", "max", "avg", "sum"} // we always ask for such order of columns

func (query Stats) TranslateSqlResponseToJson(rows []model.QueryResultRow) model.JsonMap {
	if len(rows) == 0 {
		logger.WarnWithCtx(query.ctx).Msg("no rows returned for stats aggregation")
		return model.JsonMap{
			"value": nil, // not completely sure if it's a good return value, but it looks fine to me. We should always get 1 row, not 0 anyway.
		}
	}
	if len(rows) > 1 {
		logger.WarnWithCtx(query.ctx).Msgf("more than one row returned for stats aggregation, using only first. rows[0]: %+v, rows[1]: %+v", rows[0], rows[1])
	}

	resultMap := make(model.JsonMap)
	for i, v := range rows[0].Cols {
		resultMap[statsColumnsInOrder[i]] = v.Value
	}
	return resultMap
}

func (query Stats) String() string {
	return "stats"
}

func (query Stats) ColumnIdx(name string) int {
	for i, column := range statsColumnsInOrder {
		if column == name {
			return i
		}
	}

	logger.ErrorWithCtx(query.ctx).Msgf("stats column %s not found", name)
	return -1
}
