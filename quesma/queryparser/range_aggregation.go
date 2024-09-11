// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package queryparser

import (
	"quesma/logger"
	"quesma/model/bucket_aggregations"
)

func (cw *ClickhouseQueryTranslator) parseRangeAggregation(rangePart QueryMap) bucket_aggregations.Range {
	field := cw.parseFieldField(rangePart, "range")
	var ranges []any
	if rangesRaw, ok := rangePart["ranges"]; ok {
		ranges, ok = rangesRaw.([]any)
		if !ok {
			logger.WarnWithCtx(cw.Ctx).Msgf("ranges is not an array, but %T, value: %v. Using empty array", rangesRaw, rangesRaw)
		}
	} else {
		logger.WarnWithCtx(cw.Ctx).Msg("no ranges in range aggregation. Using empty array.")
	}
	intervals := make([]bucket_aggregations.Interval, 0, len(ranges))
	for _, Range := range ranges {
		rangePartMap := Range.(QueryMap)
		var from, to float64
		if fromRaw, ok := rangePartMap["from"]; ok {
			from, ok = fromRaw.(float64)
			if !ok {
				logger.WarnWithCtx(cw.Ctx).Msgf("from is not a float64: %v, type: %T", fromRaw, fromRaw)
				from = bucket_aggregations.IntervalInfiniteRange
			}
		} else {
			from = bucket_aggregations.IntervalInfiniteRange
		}
		if toRaw, ok := rangePartMap["to"]; ok {
			to, ok = toRaw.(float64)
			if !ok {
				logger.WarnWithCtx(cw.Ctx).Msgf("to is not a float64: %v, type: %T", toRaw, toRaw)
				to = bucket_aggregations.IntervalInfiniteRange
			}
		} else {
			to = bucket_aggregations.IntervalInfiniteRange
		}
		intervals = append(intervals, bucket_aggregations.NewInterval(from, to))
	}
	if keyedRaw, exists := rangePart["keyed"]; exists {
		if keyed, ok := keyedRaw.(bool); ok {
			return bucket_aggregations.NewRange(cw.Ctx, field, intervals, keyed)
		} else {
			logger.WarnWithCtx(cw.Ctx).Msgf("keyed is not a bool, but %T, value: %v", keyedRaw, keyedRaw)
		}
	}
	return bucket_aggregations.NewRangeWithDefaultKeyed(cw.Ctx, field, intervals)
}
