// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package bucket_aggregations

import (
	"context"
	"fmt"
	"quesma/logger"
	"quesma/model"
	"quesma/util"
)

type GeoTileGrid struct {
	ctx           context.Context
	precisionZoom int
}

func NewGeoTileGrid(ctx context.Context, precisionZoom int) GeoTileGrid {
	return GeoTileGrid{ctx: ctx, precisionZoom: precisionZoom}
}

func (query GeoTileGrid) AggregationType() model.AggregationType {
	return model.BucketAggregation
}

func (query GeoTileGrid) TranslateSqlResponseToJson(rows []model.QueryResultRow) model.JsonMap {
	if len(rows) > 0 && len(rows[0].Cols) < 4 {
		logger.ErrorWithCtx(query.ctx).Msgf(
			"unexpected number of columns in geotile_grid aggregation response, len(rows[0].Cols): %d",
			len(rows[0].Cols),
		)
	}

	buckets := make([]model.JsonMap, 0, len(rows))
	for _, row := range rows {
		buckets = append(buckets, model.JsonMap{
			"key":       query.calcKey(row.Cols),
			"doc_count": row.LastColValue(),
		})
	}
	return model.JsonMap{
		"buckets": buckets,
	}
}

func (query GeoTileGrid) calcKey(cols []model.QueryResultCol) string {
	x := int64(util.ExtractFloat64(cols[0].Value))
	y := int64(util.ExtractFloat64(cols[1].Value))
	return fmt.Sprintf("%d/%d/%d", query.precisionZoom, x, y)
}

func (query GeoTileGrid) String() string {
	return "geotile_grid"
}
