package queryparser

import (
	"github.com/barkimedes/go-deepcopy"
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/model"
	"mitmproxy/quesma/model/bucket_aggregations"
	"mitmproxy/quesma/queryparser/aexp"
	"strconv"
)

func (cw *ClickhouseQueryTranslator) parseRangeAggregation(rangePart QueryMap) bucket_aggregations.Range {
	fieldName := cw.parseFieldField(rangePart, "range")
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
			// TODO we should not store Quoted field name in the range struct
			return bucket_aggregations.NewRange(cw.Ctx, strconv.Quote(fieldName), intervals, keyed)
		} else {
			logger.WarnWithCtx(cw.Ctx).Msgf("keyed is not a bool, but %T, value: %v", keyedRaw, keyedRaw)
		}
	}
	return bucket_aggregations.NewRangeWithDefaultKeyed(cw.Ctx, strconv.Quote(fieldName), intervals)
}

func (cw *ClickhouseQueryTranslator) processRangeAggregation(currentAggr *aggrQueryBuilder, Range bucket_aggregations.Range,
	queryCurrentLevel QueryMap, aggregationsAccumulator *[]model.Query, metadata JsonMap) {

	// build this aggregation
	for _, interval := range Range.Intervals {
		currentAggr.NonSchemaFields = append(
			currentAggr.NonSchemaFields,
			interval.ToSQLSelectQuery(Range.QuotedFieldName),
		)

		// TODO XXXX
		currentAggr.Columns = append(currentAggr.Columns, model.SelectColumn{Expression: aexp.SQL{Query: interval.ToSQLSelectQuery(Range.QuotedFieldName)}})

	}
	if !Range.Keyed {
		// there's a difference in output structure whether the range is keyed or not
		// it can be easily modeled in our code via setting last aggregator's .Empty to true/false
		if len(currentAggr.Aggregators) > 0 {
			currentAggr.Aggregators[len(currentAggr.Aggregators)-1].Empty = false
		} else {
			logger.ErrorWithCtx(cw.Ctx).Msg("no aggregators in currentAggr")
		}
	}
	*aggregationsAccumulator = append(*aggregationsAccumulator, currentAggr.buildBucketAggregation(metadata))
	currentAggr.NonSchemaFields = currentAggr.NonSchemaFields[:len(currentAggr.NonSchemaFields)-len(Range.Intervals)]
	currentAggr.Columns = currentAggr.Columns[:len(currentAggr.Columns)-len(Range.Intervals)]

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
		var fieldName string
		if f, err := strconv.Unquote(Range.QuotedFieldName); err != nil {
			logger.Error().Msgf("Unquoting field name in range aggregation failed: %v", err)
			fieldName = f
		} else {
			fieldName = Range.QuotedFieldName
		}
		currentAggr.whereBuilder = model.CombineWheres(
			cw.Ctx, currentAggr.whereBuilder,
			model.NewSimpleQuery(interval.ToWhereClause(fieldName), true),
		)
		currentAggr.Aggregators = append(currentAggr.Aggregators, model.NewAggregatorEmpty(interval.String()))
		aggsCopy, err := deepcopy.Anything(aggs)
		if err == nil {
			currentAggr.Type = model.NewUnknownAggregationType(cw.Ctx)
			cw.parseAggregationNames(currentAggr, aggsCopy.(QueryMap), aggregationsAccumulator)
		} else {
			logger.ErrorWithCtx(cw.Ctx).Msgf("deepcopy 'aggs' map error: %v. Skipping current range's interval: %v, aggs: %v", err, interval, aggs)
		}
		currentAggr.Aggregators = currentAggr.Aggregators[:len(currentAggr.Aggregators)-1]
		currentAggr.whereBuilder = whereBeforeNesting
	}
}
