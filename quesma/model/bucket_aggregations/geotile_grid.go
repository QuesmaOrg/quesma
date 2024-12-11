// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package bucket_aggregations

import (
	"context"
	"quesma/logger"
	"quesma/model"
	"quesma/util"
	"strconv"
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
		x := int64(util.ExtractFloat64(row.Cols[0].Value))
		y := int64(util.ExtractFloat64(row.Cols[1].Value))
		key := strconv.Itoa(query.precisionZoom) + "/" + strconv.FormatInt(x, 10) + "/" + strconv.FormatInt(y, 10)
		response = append(response, model.JsonMap{
			"key":       key,
			"doc_count": row.LastColValue(),
		})
	}
	return model.JsonMap{
		"buckets": buckets,
	}
}

func (query GeoTileGrid) calcKey(cols []model.QueryResultCol) string {
	zoom, _ := util.ExtractFloat64(cols[0].Value)
	x, _ := util.ExtractFloat64(cols[1].Value)
	y, _ := util.ExtractFloat64(cols[2].Value)
	return strconv.FormatInt(int64(zoom), 10) + "/" + strconv.FormatInt(int64(x), 10) + "/" + strconv.FormatInt(int64(y), 10)
}

func (query GeoTileGrid) String() string {
	return "geotile_grid"
}
