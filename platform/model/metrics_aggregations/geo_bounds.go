// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package metrics_aggregations

import (
	"context"
	"github.com/QuesmaOrg/quesma/platform/logger"
	"github.com/QuesmaOrg/quesma/platform/model"
)

type GeoBounds struct {
	ctx context.Context
}

func NewGeoBounds(ctx context.Context) GeoBounds {
	return GeoBounds{ctx: ctx}
}

func (query GeoBounds) AggregationType() model.AggregationType {
	return model.MetricsAggregation
}

func (query GeoBounds) TranslateSqlResponseToJson(rows []model.QueryResultRow) model.JsonMap {
	if len(rows) == 0 {
		logger.ErrorWithCtx(query.ctx).Msg("GeoBounds: expected at least one row in the result")
		return model.JsonMap{}
	}
	if len(rows[0].Cols) < 4 {
		logger.ErrorWithCtx(query.ctx).Msgf("GeoBounds: expected at least 4 columns in the result, got: %v", rows[0].Cols)
		return model.JsonMap{}
	}

	return model.JsonMap{
		"bounds": model.JsonMap{
			"top_left": model.JsonMap{
				"lon": rows[0].Cols[0].Value,
				"lat": rows[0].Cols[1].Value,
			},
			"bottom_right": model.JsonMap{
				"lat": rows[0].Cols[2].Value,
				"lon": rows[0].Cols[3].Value,
			},
		},
	}
}

func (query GeoBounds) String() string {
	return "geo_bounds"
}
