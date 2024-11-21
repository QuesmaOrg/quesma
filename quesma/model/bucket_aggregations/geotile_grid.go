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
	ctx context.Context
}

func NewGeoTileGrid(ctx context.Context) GeoTileGrid {
	return GeoTileGrid{ctx: ctx}
}

func (query GeoTileGrid) AggregationType() model.AggregationType {
	return model.BucketAggregation
}

func (query GeoTileGrid) TranslateSqlResponseToJson(rows []model.QueryResultRow) model.JsonMap {
	if len(rows) > 0 && len(rows[0].Cols) < 3 {
		logger.ErrorWithCtx(query.ctx).Msgf(
			"unexpected number of columns in geotile_grid aggregation response, len(rows[0].Cols): %d",
			len(rows[0].Cols),
		)
	}
	var response []model.JsonMap
	for _, row := range rows {
		zoomAsFloat, _ := util.ExtractFloat64(row.Cols[0].Value)
		zoom := int64(zoomAsFloat)
		xAsFloat, _ := util.ExtractFloat64(row.Cols[1].Value)
		x := int64(xAsFloat)
		yAsFloat, _ := util.ExtractFloat64(row.Cols[2].Value)
		y := int64(yAsFloat)
		key := strconv.FormatInt(zoom, 10) + "/" + strconv.FormatInt(x, 10) + "/" + strconv.FormatInt(y, 10)
		response = append(response, model.JsonMap{
			"key":       key,
			"doc_count": row.LastColValue(),
		})
	}
	return model.JsonMap{
		"buckets": response,
	}
}

func (query GeoTileGrid) String() string {
	return "geotile_grid"
}
