// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package bucket_aggregations

import (
	"context"
	"fmt"
	"github.com/QuesmaOrg/quesma/platform/logger"
	"github.com/QuesmaOrg/quesma/platform/model"
	"github.com/QuesmaOrg/quesma/platform/util"
	"reflect"
)

// GeoTileGrid Warning: we don't handle 'bounds' and 'shard_size' parameters, and proceed like they didn't exist.
// We log every time they arrive in the request, so we should know when they're used and that we should implement them
// (shouldn't be too hard).
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

func CheckParamsGeotileGrid(ctx context.Context, paramsRaw any) error {
	requiredParams := map[string]string{"field": "string"}
	optionalParams := map[string]string{
		"precision":  "int",
		"bounds":     "object", // TODO implement proper check
		"size":       "int",
		"shard_size": "int",
	}
	logIfYouSeeThemParams := []string{"bounds", "shard_size"}

	params, ok := paramsRaw.(model.JsonMap)
	if !ok {
		return fmt.Errorf("params is not a map, but %+v", paramsRaw)
	}

	// check if required are present
	for paramName, paramType := range requiredParams {
		paramVal, exists := params[paramName]
		if !exists {
			return fmt.Errorf("required parameter %s not found in params", paramName)
		}
		if reflect.TypeOf(paramVal).Name() != paramType { // TODO I'll make a small rewrite to not use reflect here
			return fmt.Errorf("required parameter %s is not of type %s, but %T", paramName, paramType, paramVal)
		}
	}

	// check if only required/optional are present
	for paramName := range params {
		if _, isRequired := requiredParams[paramName]; !isRequired {
			wantedType, isOptional := optionalParams[paramName]
			if !isOptional {
				return fmt.Errorf("unexpected parameter %s found in Geotile Grid params %v", paramName, params)
			}
			switch wantedType {
			case "int":
				if ok = util.IsInt32(params[paramName]); !ok {
					return fmt.Errorf("optional parameter %s is not of type %s, but %T", paramName, wantedType, params[paramName])
				}
			case "object":
				// TODO implement proper check
				continue
			default:
				return fmt.Errorf("unsupported type %s for optional parameter %s", wantedType, paramName)
			}
		}
	}

	// additional check for precision is in range [0, 29] (we checked above that it's int as wanted)
	if precisionRaw, exists := params["precision"]; exists {
		precision := int(util.ExtractNumeric64(precisionRaw))
		if precision < 0 || precision > 29 {
			return fmt.Errorf("precision value %d is out of bounds", precision)
		}
	}

	// log if you see them
	for _, paramToLog := range logIfYouSeeThemParams {
		if _, exists := params[paramToLog]; exists {
			logger.WarnWithCtx(ctx).Msgf("we didn't expect %s in Geotile Grid params %v", paramToLog, params)
		}
	}

	return nil
}
