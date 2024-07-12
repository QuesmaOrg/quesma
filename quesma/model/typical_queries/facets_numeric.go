// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package typical_queries

import (
	"context"
	"quesma/logger"
	"quesma/model"
	"quesma/util"
)

// FacetsNumeric There's no such aggregation in Elastic. It's a special type of a simple bucket aggregation request,
// that so far we handle differently than via standard handling, for optimization purposes.
//
// It's aggs part looks like this:
//
//	"aggs": {
//		"sample": {
//			"aggs": {
//				"max_value": { "max": { "field": "some-numeric-field" } },
//				"min_value": { "min": { "field": "some-numeric-field" } },
//				"sample_count": { "value_count": { "field": "some-numeric-field" } },
//				"top_values": { "terms": { "field": "some-numeric-field", "size": 10 } }
//			},
//			"sampler": { "shard_size": 5000 }
//		}
//	}
type FacetsNumeric struct {
	ctx context.Context
}

func NewFacetsNumeric(ctx context.Context) FacetsNumeric {
	return FacetsNumeric{ctx: ctx}
}

func (query FacetsNumeric) IsBucketAggregation() bool {
	return true
}

func (query FacetsNumeric) TranslateSqlResponseToJson(rows []model.QueryResultRow, level int) model.JsonMap {
	aggregations := facetsTranslateSqlResponseToJson(query.ctx, rows)

	firstNotNullValueIndex := 0
	for i, row := range rows {
		if row.Cols[model.ResultColKeyIndex].Value != nil {
			firstNotNullValueIndex = i
			break
		}
	}
	if firstNotNullValueIndex == len(rows) {
		aggregations["sample"].(model.JsonMap)["min_value"] = nil
		aggregations["sample"].(model.JsonMap)["max_value"] = nil
	} else {
		// Loops below might be a bit slow, as we check types in every iteration.
		// If we see performance issues, we might do separate loop for each type, but it'll be a lot of copy-paste.
		switch rows[firstNotNullValueIndex].Cols[model.ResultColKeyIndex].Value.(type) {
		case int64, uint64, *int64, *uint64, int8, uint8, *int8, *uint8, int16, uint16, *int16, *uint16, int32, uint32, *int32, *uint32:
			firstNotNullValue := util.ExtractInt64(rows[firstNotNullValueIndex].Cols[model.ResultColKeyIndex].Value)
			minValue, maxValue := firstNotNullValue, firstNotNullValue
			for _, row := range rows[firstNotNullValueIndex+1:] {
				if row.Cols[model.ResultColKeyIndex].Value != nil {
					value := util.ExtractInt64(row.Cols[model.ResultColKeyIndex].Value)
					maxValue = max(maxValue, value)
					minValue = min(minValue, value)
				}
			}
			aggregations["sample"].(model.JsonMap)["min_value"] = model.JsonMap{"value": minValue}
			aggregations["sample"].(model.JsonMap)["max_value"] = model.JsonMap{"value": maxValue}
		case float64, *float64, float32, *float32:
			firstNotNullValue := util.ExtractFloat64(rows[firstNotNullValueIndex].Cols[model.ResultColKeyIndex].Value)
			minValue, maxValue := firstNotNullValue, firstNotNullValue
			for _, row := range rows[firstNotNullValueIndex+1:] {
				if row.Cols[model.ResultColKeyIndex].Value != nil {
					value := util.ExtractFloat64(row.Cols[model.ResultColKeyIndex].Value)
					maxValue = max(maxValue, value)
					minValue = min(minValue, value)
				}
			}
			aggregations["sample"].(model.JsonMap)["min_value"] = model.JsonMap{"value": minValue}
			aggregations["sample"].(model.JsonMap)["max_value"] = model.JsonMap{"value": maxValue}
		default:
			logger.WarnWithCtx(query.ctx).Msgf("unknown type for numeric facet: %T, value: %v",
				rows[0].Cols[model.ResultColKeyIndex].Value, rows[0].Cols[model.ResultColKeyIndex].Value)
			aggregations["sample"].(model.JsonMap)["min_value"] = model.JsonMap{"value": nil}
			aggregations["sample"].(model.JsonMap)["max_value"] = model.JsonMap{"value": nil}
		}
	}

	return aggregations
}

func (query FacetsNumeric) String() string {
	return "facets_numeric"
}

func (query FacetsNumeric) PostprocessResults(rowsFromDB []model.QueryResultRow) []model.QueryResultRow {
	return rowsFromDB
}
