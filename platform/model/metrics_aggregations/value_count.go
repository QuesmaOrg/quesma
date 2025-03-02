// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package metrics_aggregations

import (
	"context"
	"github.com/QuesmaOrg/quesma/platform/logger"
	"github.com/QuesmaOrg/quesma/platform/model"
)

type ValueCount struct {
	ctx context.Context
}

func NewValueCount(ctx context.Context) ValueCount {
	return ValueCount{ctx: ctx}
}

func (query ValueCount) AggregationType() model.AggregationType {
	return model.MetricsAggregation
}

func (query ValueCount) TranslateSqlResponseToJson(rows []model.QueryResultRow) model.JsonMap {
	var value any = nil
	if len(rows) > 0 {
		value = rows[0].Cols[0].Value
	} else {
		logger.WarnWithCtx(query.ctx).Msg("Nn rows returned for value_count aggregation")
	}
	return model.JsonMap{
		"value": value,
	}
}

func (query ValueCount) String() string {
	return "value_count"
}
