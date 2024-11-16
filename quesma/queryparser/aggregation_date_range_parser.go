// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package queryparser

import (
	"fmt"
	"quesma/logger"
	"quesma/model/bucket_aggregations"
	"unicode"
)

// paramsRaw - in a proper request should be of QueryMap type.
func (cw *ClickhouseQueryTranslator) parseDateRangeAggregation(aggregation *pancakeAggregationTreeNode, paramsRaw any) (err error) {
	params, ok := paramsRaw.(QueryMap)
	if !ok {
		return fmt.Errorf("date_range is not a map, but %T, value: %v", paramsRaw, paramsRaw)
	}

	var fieldName, format string

	if field, exists := params["field"]; exists {
		if fieldNameRaw, ok := field.(string); ok {
			fieldName = cw.ResolveField(cw.Ctx, fieldNameRaw)
		} else {
			return fmt.Errorf("field specified for date range aggregation is not a string. Params: %v", params)
		}
	} else {
		return fmt.Errorf("no field specified for date range aggregation. Params: %v", params)
	}

	if formatRaw, exists := params["format"]; exists {
		if formatParsed, ok := formatRaw.(string); ok {
			format = formatParsed
		} else {
			logger.WarnWithCtx(cw.Ctx).Msgf("format specified for date range aggregation is not a string. Using empty. Params: %v", params)
		}
	}

	var ranges []any
	if rangesRaw, exists := params["ranges"]; exists {
		if ranges, ok = rangesRaw.([]any); !ok {
			return fmt.Errorf("ranges specified for date range aggregation is not an array, params: %v", params)
		}
	} else {
		return fmt.Errorf("no ranges specified for date range aggregation, params: %v", params)
	}

	intervals := make([]bucket_aggregations.DateTimeInterval, 0, len(ranges))
	selectColumnsNr := len(ranges) // we query Clickhouse for every unbounded part of interval (begin and end)
	for _, Range := range ranges {
		rangeMap := Range.(QueryMap)
		var intervalBegin, intervalEnd string
		from, exists := rangeMap["from"]
		if exists {
			if fromRaw, ok := from.(string); ok {
				intervalBegin, err = cw.parseDateTimeInClickhouseMathLanguage(fromRaw)
				if err != nil {
					return err
				}
				selectColumnsNr++
			} else {
				logger.WarnWithCtx(cw.Ctx).Msgf("from specified for date range aggregation is not a string, params: %v "+
					"using default (unbounded).", params)
				intervalBegin = bucket_aggregations.UnboundedInterval
			}
		} else {
			intervalBegin = bucket_aggregations.UnboundedInterval
		}
		to, exists := rangeMap["to"]
		if exists {
			if toRaw, ok := to.(string); ok {
				intervalEnd, err = cw.parseDateTimeInClickhouseMathLanguage(toRaw)
				if err != nil {
					return err
				}
				selectColumnsNr++
			} else {
				logger.WarnWithCtx(cw.Ctx).Msgf("To specified for date range aggregation is not a string, params: %v "+
					"using default (unbounded).", params)
				intervalEnd = bucket_aggregations.UnboundedInterval
			}
		} else {
			intervalEnd = bucket_aggregations.UnboundedInterval
		}
		intervals = append(intervals, bucket_aggregations.NewDateTimeInterval(intervalBegin, intervalEnd))
	}

	// TODO: keep for reference as relative time, but no longer needed
	/*
		for _, interval := range dateRangeParsed.Intervals {

			aggregation.selectedColumns = append(aggregation.selectedColumns, interval.ToSQLSelectQuery(dateRangeParsed.FieldName))

			if sqlSelect, selectNeeded := interval.BeginTimestampToSQL(); selectNeeded {
				aggregation.selectedColumns = append(aggregation.selectedColumns, sqlSelect)
			}
			if sqlSelect, selectNeeded := interval.EndTimestampToSQL(); selectNeeded {
				aggregation.selectedColumns = append(aggregation.selectedColumns, sqlSelect)
			}
		}*/

	aggregation.queryType = bucket_aggregations.NewDateRange(cw.Ctx, fieldName, format, intervals, selectColumnsNr)
	return nil
}

// parseDateTimeInClickhouseMathLanguage parses dateTime from Clickhouse's format
// It's described here: https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-daterange-aggregation.html
// Maybe not 100% of it is implemented, not sure.
func (cw *ClickhouseQueryTranslator) parseDateTimeInClickhouseMathLanguage(dateTime string) (string, error) {
	// So far we've seen only either:
	// 1. 2024-01-01 format
	if cw.isSimpleDate(dateTime) {
		return "'" + dateTime + "'", nil
	}
	// 2. expressions like now() or now()-1d
	res, err := cw.parseDateMathExpression(dateTime)
	if err != nil {
		return "", err
	}
	return res, nil

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
