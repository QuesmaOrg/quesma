package queryparser

import (
	"fmt"
	"github.com/barkimedes/go-deepcopy"
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/model"
	"mitmproxy/quesma/model/bucket_aggregations"
	"strconv"
	"unicode"
)

// parser

func (cw *ClickhouseQueryTranslator) parseDateRangeAggregation(dateRange QueryMap) bucket_aggregations.DateRange {
	var fieldName, format string
	if field, exists := dateRange["field"]; exists {
		if fieldNameRaw, ok := field.(string); ok {
			fieldName = cw.Table.ResolveField(cw.Ctx, fieldNameRaw)
		} else {
			logger.WarnWithCtx(cw.Ctx).Msgf("field specified for date range aggregation is not a string. Using empty. Querymap: %v", dateRange)
		}
	} else {
		logger.WarnWithCtx(cw.Ctx).Msgf("no field specified for date range aggregation. Using empty. Querymap: %v", dateRange)
	}
	var ranges []any
	var ok bool
	if formatRaw, exists := dateRange["format"]; exists {
		if formatParsed, ok := formatRaw.(string); ok {
			format = formatParsed
		} else {
			logger.WarnWithCtx(cw.Ctx).Msgf("format specified for date range aggregation is not a string. Using empty. Querymap: %v", dateRange)
		}
	}
	if rangesRaw, exists := dateRange["ranges"]; exists {
		if ranges, ok = rangesRaw.([]any); !ok {
			logger.WarnWithCtx(cw.Ctx).Msgf("ranges specified for date range aggregation is not an array. Using empty. Querymap: %v", dateRange)
		}
	} else {
		logger.WarnWithCtx(cw.Ctx).Msgf("no ranges specified for date range aggregation. Using empty. Querymap: %v", dateRange)
	}
	intervals := make([]bucket_aggregations.DateTimeInterval, 0, len(ranges))
	selectColumnsNr := len(ranges) // we query Clickhouse for every unbounded part of interval (begin and end)
	for _, Range := range ranges {
		rangeMap := Range.(QueryMap)
		var intervalBegin, intervalEnd string
		from, exists := rangeMap["from"]
		if exists {
			if fromRaw, ok := from.(string); ok {
				intervalBegin = cw.parseDateTimeInClickhouseMathLanguage(fromRaw)
				selectColumnsNr++
			} else {
				logger.WarnWithCtx(cw.Ctx).Msgf("from specified for date range aggregation is not a string. Querymap: %v "+
					"Using default (unbounded).", dateRange)
				intervalBegin = bucket_aggregations.UnboundedInterval
			}
		} else {
			intervalBegin = bucket_aggregations.UnboundedInterval
		}
		to, exists := rangeMap["to"]
		if exists {
			if toRaw, ok := to.(string); ok {
				intervalEnd = cw.parseDateTimeInClickhouseMathLanguage(toRaw)
				selectColumnsNr++
			} else {
				logger.WarnWithCtx(cw.Ctx).Msgf("To specified for date range aggregation is not a string. Querymap: %v "+
					"Using default (unbounded).", dateRange)
				intervalEnd = bucket_aggregations.UnboundedInterval
			}
		} else {
			intervalEnd = bucket_aggregations.UnboundedInterval
		}
		intervals = append(intervals, bucket_aggregations.NewDateTimeInterval(intervalBegin, intervalEnd))
	}
	return bucket_aggregations.NewDateRange(cw.Ctx, strconv.Quote(fieldName), format, intervals, selectColumnsNr)
}

// parseDateTimeInClickhouseMathLanguage parses dateTime from Clickhouse's format
// It's described here: https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-daterange-aggregation.html
// Maybe not 100% of it is implemented, not sure.
func (cw *ClickhouseQueryTranslator) parseDateTimeInClickhouseMathLanguage(dateTime string) string {
	// So far we've seen only either:
	// 1. 2024-01-01 format
	if cw.isSimpleDate(dateTime) {
		return "'" + dateTime + "'"
	}
	// 2. expressions like now() or now()-1d
	parsedWithoutRounding := parseDateMathExpression(dateTime)
	return cw.addRoundingToClickhouseDateTime(dateTime, parsedWithoutRounding)
}

// isSimpleDate returns true if the given dateTime is a simple date string in format 2024-04-15
func (cw *ClickhouseQueryTranslator) isSimpleDate(dateTime string) bool {
	if len(dateTime) != len("2024-04-15") {
		return false
	}
	for _, idx := range []int{0, 1, 2, 3, 5, 6, 8, 9} {
		if !unicode.IsDigit(rune(dateTime[idx])) {
			return false
		}
	}
	for _, idx := range []int{4, 7} {
		if dateTime[idx] != '-' {
			return false
		}
	}
	return true
}

// addRoundingToClickhouseDateTime adds rounding that might be present in Clickhouse's format.
// If it exists, is at the end of dateTime in a "/[char]" format, e.g. /d, /w, /M, /Y
// It means, e.g. /M, that we want to round to the beginning of the month.
// It's done via Clickhouse's functions toStartOfDay, toStartOfWeek, toStartOfMonth, toStartOfYear.
func (cw *ClickhouseQueryTranslator) addRoundingToClickhouseDateTime(dateTime string, parsedWithoutRounding string) string {
	const defaultRounding = 'd'
	var roundingFunction = map[rune]string{
		'd': "toStartOfDay",
		'w': "toStartOfWeek",
		'M': "toStartOfMonth",
		'Y': "toStartOfYear",
	}
	if len(dateTime) < len("/d") || dateTime[len(dateTime)-2] != '/' {
		return parsedWithoutRounding
	}
	switch dateTime[len(dateTime)-1] {
	case 'd', 'w', 'M', 'Y':
		return roundingFunction[rune(dateTime[len(dateTime)-1])] + "(" + parsedWithoutRounding + ")"
	default:
		logger.Error().Msgf("unknown rounding character %c in dateTime %s. Defaulting to /%s", dateTime[len(dateTime)-1], dateTime, string(defaultRounding))
		return roundingFunction[defaultRounding] + "(" + parsedWithoutRounding + ")"
	}
}

// processor

func (cw *ClickhouseQueryTranslator) processDateRangeAggregation(currentAggr *aggrQueryBuilder, dateRange bucket_aggregations.DateRange,
	queryCurrentLevel QueryMap, aggregationsAccumulator *[]model.QueryWithAggregation, metadata JsonMap) {

	// build this aggregation
	nonSchemaFieldsAdded := len(dateRange.Intervals)
	for _, interval := range dateRange.Intervals {
		currentAggr.NonSchemaFields = append(currentAggr.NonSchemaFields, interval.ToSQLSelectQuery(dateRange.QuotedFieldName))
		if sqlSelect, selectNeeded := interval.BeginTimestampToSQL(); selectNeeded {
			currentAggr.NonSchemaFields = append(currentAggr.NonSchemaFields, sqlSelect)
			nonSchemaFieldsAdded++
		}
		if sqlSelect, selectNeeded := interval.EndTimestampToSQL(); selectNeeded {
			currentAggr.NonSchemaFields = append(currentAggr.NonSchemaFields, sqlSelect)
			nonSchemaFieldsAdded++
		}
	}
	if len(currentAggr.Aggregators) > 0 {
		currentAggr.Aggregators[len(currentAggr.Aggregators)-1].Empty = false
	} else {
		logger.ErrorWithCtx(cw.Ctx).Msg("no aggregators in currentAggr")
	}
	*aggregationsAccumulator = append(*aggregationsAccumulator, currentAggr.buildBucketAggregation(metadata))
	currentAggr.NonSchemaFields = currentAggr.NonSchemaFields[:len(currentAggr.NonSchemaFields)-nonSchemaFieldsAdded]

	// build subaggregations
	aggs, hasAggs := queryCurrentLevel["aggs"].(QueryMap)
	if !hasAggs {
		return
	}

	// TODO now we run a separate query for each range.
	// it's much easier to code it this way, but that can, quite easily, be improved.
	// Range aggregation with subaggregations should be a quite rare case, so I'm leaving that for later.
	whereBeforeNesting := currentAggr.whereBuilder
	for _, interval := range dateRange.Intervals {
		fmt.Println("tutu")
		currentAggr.whereBuilder = cw.combineWheres(
			currentAggr.whereBuilder,
			newSimpleQuery(NewSimpleStatement(interval.ToWhereClause(dateRange.QuotedFieldName)), true),
		)
		// currentAggr.NonSchemaFields = append(currentAggr.NonSchemaFields, interval.String()+strconv.Itoa(i))
		// currentAggr.Aggregators = append(currentAggr.Aggregators, model.NewAggregatorEmpty(interval.String()+strconv.Itoa(i)))
		aggsCopy, err := deepcopy.Anything(aggs)
		if err == nil {
			currentAggr.Type = model.NewUnknownAggregationType(cw.Ctx)
			cw.parseAggregation(currentAggr, aggsCopy.(QueryMap), aggregationsAccumulator)
		} else {
			logger.ErrorWithCtx(cw.Ctx).Msgf("deepcopy 'aggs' map error: %v. Skipping current range's interval: %v, aggs: %v", err, interval, aggs)
		}
		// currentAggr.Aggregators = currentAggr.Aggregators[:len(currentAggr.Aggregators)-1]
		//currentAggr.NonSchemaFields = currentAggr.NonSchemaFields[:len(currentAggr.NonSchemaFields)-1]
		currentAggr.whereBuilder = whereBeforeNesting
	}
}
