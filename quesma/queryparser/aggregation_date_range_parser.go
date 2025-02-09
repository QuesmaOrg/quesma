// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package queryparser

import (
	"fmt"
	"github.com/QuesmaOrg/quesma/quesma/model"
	"github.com/QuesmaOrg/quesma/quesma/model/bucket_aggregations"
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

		const defaultIntervalBound = bucket_aggregations.UnboundedIntervalString
		var intervalBegin model.Expr
		if from := cw.parseStringField(rangeMap, "from", defaultIntervalBound); from != defaultIntervalBound {
			intervalBegin, err = cw.parseDateTimeInClickhouseMathLanguage(field, from)
			if err != nil {
				return err
			}
			selectColumnsNr++
		}

		var intervalEnd model.Expr
		if to := cw.parseStringField(rangeMap, "to", defaultIntervalBound); to != defaultIntervalBound {
			intervalEnd, err = cw.parseDateTimeInClickhouseMathLanguage(field, to)
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
func (cw *ClickhouseQueryTranslator) parseDateTimeInClickhouseMathLanguage(field model.Expr, dateTime string) (model.Expr, error) {
	// So far we've seen only either:
	// 1. 2024-01-01 format TODO update
	dateManager := NewDateManager(cw.Ctx)
	if funcName, expr := dateManager.ParseDateUsualFormat(dateTime); expr != nil {
		return model.NewFunction(funcName, expr), nil
	}
	// 2. expressions like now() or now()-1d
	res, err := cw.parseDateMathExpression(dateTime)
	if err != nil {
		return nil, err
	}
	return res, nil

}
