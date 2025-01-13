// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package metrics_aggregations

import (
	"context"
	"github.com/QuesmaOrg/quesma/quesma/model"
)

type GeoCentroid struct {
	ctx context.Context
}

func NewGeoCentroid(ctx context.Context) GeoCentroid {
	return GeoCentroid{ctx: ctx}
}

func (query GeoCentroid) AggregationType() model.AggregationType {
	return model.MetricsAggregation
}

func (query GeoCentroid) TranslateSqlResponseToJson(rows []model.QueryResultRow) model.JsonMap {
	if len(rows) == 0 {
		return model.JsonMap{}
	}
	if len(rows[0].Cols) == 3 {
		// new logic
		return model.JsonMap{
			"count": rows[0].Cols[2].Value,
			"location": model.JsonMap{
				"lat": rows[0].Cols[0].Value,
				"lon": rows[0].Cols[1].Value,
			},
		}
	}
	// TODO: Remove after pancakes
	location := model.JsonMap{
		"lat": rows[0].Cols[3].Value,
		"lon": rows[0].Cols[4].Value,
	}
	return model.JsonMap{
		"count":    rows[0].Cols[5].Value,
		"location": location,
	}
}

func (query GeoCentroid) String() string {
	return "geo_centroid"
}
