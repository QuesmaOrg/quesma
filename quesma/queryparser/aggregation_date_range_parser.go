// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package queryparser

import (
	"errors"
	"fmt"
	"quesma/logger"
	"quesma/model/bucket_aggregations"
	"quesma/schema"
	"unicode"
)

func (cw *ClickhouseQueryTranslator) parseDateRangeAggregation(dateRange QueryMap) (bucket_aggregations.DateRange, error) {
	var err error
	var fieldName, format string

	if cw.SchemaRegistry == nil {
		logger.Error().Msg("Schema registry is not set")
		return bucket_aggregations.DateRange{}, errors.New("schema registry is not set")
	}

	schemaInstance, exists := cw.SchemaRegistry.FindSchema(schema.TableName(cw.Table.Name))
	if !exists {
		logger.Error().Msgf("Schema fot table %s not found", cw.Table.Name)
		return bucket_aggregations.DateRange{}, fmt.Errorf("schema fot table %s not found", cw.Table.Name)
	}

	_ = schemaInstance
	if field, exists := dateRange["field"]; exists {
		if fieldNameRaw, ok := field.(string); ok {
			//fieldName = schemaInstance.Fields[schema.FieldName(fieldNameRaw)].InternalPropertyName.AsString()
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
				intervalBegin, err = cw.parseDateTimeInClickhouseMathLanguage(fromRaw)
				if err != nil {
					return bucket_aggregations.DateRange{}, err
				}
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
				intervalEnd, err = cw.parseDateTimeInClickhouseMathLanguage(toRaw)
				if err != nil {
					return bucket_aggregations.DateRange{}, err
				}
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
	return bucket_aggregations.NewDateRange(cw.Ctx, fieldName, format, intervals, selectColumnsNr), nil
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
