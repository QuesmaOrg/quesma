package queryparser

import (
	"fmt"
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/model"
	"mitmproxy/quesma/model/bucket_aggregations"
	"strconv"
)

func (cw *ClickhouseQueryTranslator) parseHistogramAggregation(queryMap QueryMap) (success bool, aggregation model.QueryType, err error) {
	histogramRaw, exists := queryMap["histogram"]
	if !exists {
		fmt.Println("NOT EXISTS, AGGR: ", aggregation)
		return
	}

	delete(queryMap, "histogram")
	histogram, ok := histogramRaw.(QueryMap)
	if !ok {
		logger.WarnWithCtx(cw.Ctx).Msgf("date_histogram is not a map, but %T, value: %v. Skipping", histogramRaw, histogramRaw)
		return
	}

	fieldName, isFieldNameFromScript := cw.parseFieldFieldMaybeScript(histogram, "histogram")
	var fieldNameProperlyQuoted string
	if isFieldNameFromScript {
		fieldNameProperlyQuoted = fieldName
	} else {
		fieldNameProperlyQuoted = strconv.Quote(fieldName)
	}
	var interval float64
	intervalRaw, ok := histogram["interval"]
	if !ok {
		logger.WarnWithCtx(cw.Ctx).Msgf("interval not found in histogram: %v", histogram)
	}
	switch intervalTyped := intervalRaw.(type) {
	case string:
		var err error
		interval, err = strconv.ParseFloat(intervalTyped, 64)
		if err != nil {
			interval = 1.0
			logger.WarnWithCtx(cw.Ctx).Err(err).Msgf("failed to parse interval: %v. Using default (1.0)", intervalRaw)
		}
	case int:
		interval = float64(intervalTyped)
	case float64:
		interval = intervalTyped
	default:
		interval = 1.0
		logger.WarnWithCtx(cw.Ctx).Msgf("unexpected type of interval: %T, value: %v. Using default (1.0)", intervalTyped, intervalTyped)
	}
	minDocCount := cw.parseMinDocCount(histogram)
	return true, bucket_aggregations.NewHistogram(cw.Ctx, fieldNameProperlyQuoted, interval, minDocCount), nil
}

func (cw *ClickhouseQueryTranslator) processHistogramAggregation(aggrBuilder *aggrQueryBuilder, histogram bucket_aggregations.Histogram) {
	groupByStr := histogram.FieldNameProperlyQuoted
	if histogram.Interval != 1.0 {
		groupByStr = fmt.Sprintf("floor(%s / %f) * %f",
			histogram.FieldNameProperlyQuoted, histogram.Interval, histogram.Interval)
	}
	aggrBuilder.GroupByFields = append(aggrBuilder.GroupByFields, groupByStr)
	aggrBuilder.OrderBy = append(aggrBuilder.OrderBy, groupByStr)
	aggrBuilder.NonSchemaFields = append(aggrBuilder.NonSchemaFields, groupByStr)
}
