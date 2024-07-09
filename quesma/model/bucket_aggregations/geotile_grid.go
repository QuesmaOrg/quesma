// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package bucket_aggregations

import (
	"context"
	"quesma/logger"
	"quesma/model"
	"strconv"
)

type GeoTileGrid struct {
	ctx context.Context
}

func NewGeoTileGrid(ctx context.Context) GeoTileGrid {
	return GeoTileGrid{ctx: ctx}
}

func (query GeoTileGrid) IsBucketAggregation() bool {
	return true
}

func (query GeoTileGrid) TranslateSqlResponseToJson(rows []model.QueryResultRow, level int) []model.JsonMap {
	if len(rows) > 0 && len(rows[0].Cols) < 3 {
		logger.ErrorWithCtx(query.ctx).Msgf(
			"unexpected number of columns in geotile_grid aggregation response, len(rows[0].Cols): "+
				"%d, level: %d", len(rows[0].Cols), level,
		)
	}
	var response []model.JsonMap
	for _, row := range rows {
		zoom := int64(row.Cols[0].Value.(float64))
		x := int64(row.Cols[1].Value.(float64))
		y := int64(row.Cols[2].Value.(float64))
		key := strconv.FormatInt(zoom, 10) + "/" + strconv.FormatInt(x, 10) + "/" + strconv.FormatInt(y, 10)
		response = append(response, model.JsonMap{
			"key":       key,
			"doc_count": row.LastColValue(),
		})
	}
	return response
}

func (query GeoTileGrid) String() string {
	return "geotile_grid"
}

func (query GeoTileGrid) PostprocessResults(rowsFromDB []model.QueryResultRow) []model.QueryResultRow {
	return rowsFromDB
}
