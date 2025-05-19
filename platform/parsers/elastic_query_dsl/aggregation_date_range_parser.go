// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package elastic_query_dsl

import (
	"fmt"
	"github.com/QuesmaOrg/quesma/platform/model/bucket_aggregations"
	"unicode"
)

func (cw *ClickhouseQueryTranslator) parseDateRangeAggregation(aggregation *pancakeAggregationTreeNode, params QueryMap) (err error) {
	field := cw.parseFieldField(params, "date_range")
	if field == nil {
		return fmt.Errorf("no field specified for date range aggregation, params: %v", params)
	}
	format := cw.parseStringField(params, "format", "")
	ranges, err := cw.parseArrayField(params, "ranges")
	if err != nil {
		return err
	}

	intervals := make([]bucket_aggregations.DateTimeInterval, 0, len(ranges))
	selectColumnsNr := len(ranges) // we query Clickhouse for every unbounded part of interval (begin and end)
	for _, rangeRaw := range ranges {
		rangeMap, ok := rangeRaw.(QueryMap)
		if !ok {
			return fmt.Errorf("range is not a map, but %T, range: %v", rangeRaw, rangeRaw)
		}

		const defaultIntervalBound = bucket_aggregations.UnboundedInterval
		intervalBegin := defaultIntervalBound
		if from := cw.parseStringField(rangeMap, "from", defaultIntervalBound); from != defaultIntervalBound {
			intervalBegin, err = cw.parseDateTimeInClickhouseMathLanguage(from)
			if err != nil {
				return err
			}
			selectColumnsNr++
		}

		intervalEnd := bucket_aggregations.UnboundedInterval
		if to := cw.parseStringField(rangeMap, "to", defaultIntervalBound); to != defaultIntervalBound {
			intervalEnd, err = cw.parseDateTimeInClickhouseMathLanguage(to)
			if err != nil {
				return err
			}
			selectColumnsNr++
		}
		intervals = append(intervals, bucket_aggregations.NewDateTimeInterval(intervalBegin, intervalEnd))
	}

	aggregation.queryType = bucket_aggregations.NewDateRange(cw.Ctx, field, format, intervals, selectColumnsNr)
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
