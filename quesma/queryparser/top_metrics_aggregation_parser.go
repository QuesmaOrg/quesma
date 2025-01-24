// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package queryparser

import (
	"context"
	"github.com/QuesmaOrg/quesma/quesma/logger"
	"github.com/QuesmaOrg/quesma/quesma/model"
)

func (cw *ClickhouseQueryTranslator) ParseTopMetricsAggregation(queryMap QueryMap) metricsAggregation {
	var fields []model.Expr
	metrics, exists := queryMap["metrics"]
	if exists {
		var fieldList []interface{}
		if fields, ok := metrics.([]interface{}); ok {
			fieldList = fields
		} else {
			fieldList = append(fieldList, metrics)
		}
		fields = cw.getFieldNames(fieldList)
	} else {
		logger.WarnWithCtx(cw.Ctx).Msg("no metrics field found in query")
	}
	var sortBy, order string
	if sort, exists := queryMap["sort"]; exists {
		if sortAsQueryMap, ok := sort.(QueryMap); ok {
			sortBy, order = getFirstKeyValue(cw.Ctx, sortAsQueryMap)
		} else {
			logger.WarnWithCtx(cw.Ctx).Msgf("sort field is not a query map, sort: %v", sort)
		}
	} else {
		logger.WarnWithCtx(cw.Ctx).Msg("no sort field found in top_metrics query")
	}

	const defaultSize = 1
	var size int
	if sizeRaw, exists := queryMap["size"]; exists {
		if sizeFloat, ok := sizeRaw.(float64); ok {
			size = int(sizeFloat)
		} else {
			logger.WarnWithCtx(cw.Ctx).Msgf("size field is not a float64, type: %T, size: %v", sizeRaw, sizeRaw)
			size = defaultSize
		}
	} else {
		size = defaultSize
	}
	return metricsAggregation{
		AggrType:  "top_metrics",
		Fields:    fields,
		FieldType: metricsAggregationDefaultFieldType, // don't need to check, it's unimportant for this aggregation
		SortBy:    sortBy,
		Size:      size,
		Order:     order,
	}
}

func getFirstKeyValue(ctx context.Context, queryMap QueryMap) (string, string) {
	for k, v := range queryMap {
		vAsString, ok := v.(string)
		if !ok {
			logger.WarnWithCtx(ctx).Msgf("value is not a string (type: %T). key: %v, value: %v", v, k, v)
		}
		return k, vAsString
	}
	return "", ""
}

func (cw *ClickhouseQueryTranslator) getFieldNames(fields []interface{}) (exprs []model.Expr) {
	for _, field := range fields {
		if fName, ok := field.(QueryMap)["field"]; ok {
			if fieldName, ok := fName.(string); ok {
				exprs = append(exprs, model.NewColumnRef(fieldName))
			} else {
				logger.WarnWithCtx(cw.Ctx).Msgf("field %v is not a string (type: %T). Might be correct, might not. Check it out.", fName, fName)
			}
		}
	}
	return exprs
}
