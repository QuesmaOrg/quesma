// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package queryparser

import (
	"github.com/barkimedes/go-deepcopy"
	"quesma/logger"
	"quesma/model"
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

func (cw *ClickhouseQueryTranslator) processRangeAggregation(currentAggr, FULL *aggrQueryBuilder, Range bucket_aggregations.Range,
	queryCurrentLevel QueryMap, aggregationsAccumulator *[]*model.Query, metadata JsonMap) {

	// build this aggregation
	for _, interval := range Range.Intervals {
		stmt := Range.Expr
		currentAggr.SelectCommand.Columns = append(currentAggr.SelectCommand.Columns, interval.ToSQLSelectQuery(stmt))
	}
	if !Range.Keyed {
		// there's a difference in output structure whether the range is keyed or not
		// it can be easily modeled in our code via setting last aggregator's .Empty to true/false
		if len(currentAggr.Aggregators) > 0 {
			currentAggr.Aggregators[len(currentAggr.Aggregators)-1].SplitOverHowManyFields = 1
		} else {
			logger.ErrorWithCtx(cw.Ctx).Msg("no aggregators in currentAggr")
		}
	}
	*aggregationsAccumulator = append(*aggregationsAccumulator, currentAggr.buildBucketAggregation(metadata))
	currentAggr.SelectCommand.Columns = currentAggr.SelectCommand.Columns[:len(currentAggr.SelectCommand.Columns)-len(Range.Intervals)]

	// build subaggregations
	aggs, hasAggs := queryCurrentLevel["aggs"].(QueryMap)
	if !hasAggs {
		return
	}
	// TODO now we run a separate query for each range.
	// it's much easier to code it this way, but that can, quite easily, be improved.
	// Range aggregation with subaggregations should be a quite rare case, so I'm leaving that for later.
	whereBeforeNesting := currentAggr.whereBuilder
	for _, interval := range Range.Intervals {
		stmt := Range.Expr
		currentAggr.whereBuilder = model.CombineWheres(
			cw.Ctx, currentAggr.whereBuilder,
			model.NewSimpleQuery(interval.ToWhereClause(stmt), true),
		)
		currentAggr.Aggregators = append(currentAggr.Aggregators, model.NewAggregator(interval.String()))
		aggsCopy, err := deepcopy.Anything(aggs)
		if err == nil {
			currentAggr.Type = model.NewUnknownAggregationType(cw.Ctx)
			cw.parseAggregationNames(currentAggr, FULL, aggsCopy.(QueryMap), aggregationsAccumulator)
		} else {
			logger.ErrorWithCtx(cw.Ctx).Msgf("deepcopy 'aggs' map error: %v. Skipping current range's interval: %v, aggs: %v", err, interval, aggs)
		}
		currentAggr.Aggregators = currentAggr.Aggregators[:len(currentAggr.Aggregators)-1]
		currentAggr.whereBuilder = whereBeforeNesting
	}
}
