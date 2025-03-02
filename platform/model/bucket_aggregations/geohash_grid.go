package bucket_aggregations

import (
	"context"
	"fmt"
	"github.com/QuesmaOrg/quesma/platform/logger"
	"github.com/QuesmaOrg/quesma/platform/model"
	"reflect"
)

type GeoHashGrid struct {
	ctx context.Context
}

func NewGeoHashGrid(ctx context.Context) GeoHashGrid {
	return GeoHashGrid{ctx: ctx}
}

func (query GeoHashGrid) AggregationType() model.AggregationType {
	return model.BucketAggregation
}

func (query GeoHashGrid) TranslateSqlResponseToJson(rows []model.QueryResultRow) model.JsonMap {
	buckets := make([]model.JsonMap, 0, len(rows))
	for _, row := range rows {
		if len(row.Cols) < 2 {
			logger.ErrorWithCtx(query.ctx).Msgf(
				"unexpected number of columns in geohash_grid aggregation response, len(rows[0].Cols): %d",
				len(row.Cols),
			)
			return model.JsonMap{"buckets": []model.JsonMap{}}
		}

		buckets = append(buckets, model.JsonMap{
			"key":       row.SecondLastColValue(),
			"doc_count": row.LastColValue(),
		})
	}

	return model.JsonMap{
		"buckets": buckets,
	}
}

func (query GeoHashGrid) String() string {
	return "geohash_grid"
}

// TODO make part of QueryType interface and implement for all aggregations
// TODO add bad requests to tests
// Doing so will ensure we see 100% of what we're interested in in our logs (now we see ~95%)
func CheckParamsGeohashGrid(ctx context.Context, paramsRaw any) error {
	requiredParams := map[string]string{
		"field": "string",
	}
	optionalParams := map[string]string{
		"bounds":     "map",
		"precision":  "float64", // TODO should be int, low priority for fixing
		"shard_size": "float64", // TODO should be int, low priority for fixing
		"size":       "float64", // TODO should be int, low priority for fixing
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
				return fmt.Errorf("unexpected parameter %s found in IP Range params %v", paramName, params)
			}
			if reflect.TypeOf(params[paramName]).Name() != wantedType { // TODO I'll make a small rewrite to not use reflect here
				return fmt.Errorf("optional parameter %s is not of type %s, but %T", paramName, wantedType, params[paramName])
			}
		}
	}

	// log if you see them
	for _, warnParam := range logIfYouSeeThemParams {
		if _, exists := params[warnParam]; exists {
			logger.WarnWithCtxAndThrottling(ctx, "ip_prefix", warnParam, "we didn't expect %s in IP Range params %v", warnParam, params)
		}
	}

	return nil
}
