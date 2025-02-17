// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package queryparser

import (
	"fmt"
	"github.com/QuesmaOrg/quesma/quesma/clickhouse"
	"github.com/QuesmaOrg/quesma/quesma/logger"
	"github.com/QuesmaOrg/quesma/quesma/model"
	"regexp"
	"slices"
	"strconv"
)

const keyedDefaultValuePercentileRanks = true

type metricsAggregation struct {
	AggrType            string
	Fields              []model.Expr            // on these fields we're doing aggregation. Array, because e.g. 'top_hits' can have multiple fields
	OrderBy             []model.OrderByExpr     // only for top_hits
	FieldType           clickhouse.DateTimeType // field type of FieldNames[0]. If it's a date field, a slightly different response is needed
	Percentiles         map[string]float64      // Only for percentiles and percentile_ranks aggregation
	Keyed               bool                    // Only for percentiles aggregation
	CutValues           []string                // Only for percentile_ranks
	SortBy              string                  // Only for top_metrics
	Size                int                     // Only for top_metrics
	Order               string                  // Only for top_metrics
	IsFieldNameCompound bool                    // Only for a few aggregations, where we have only 1 field. It's a compound, so e.g. toHour(timestamp), not just "timestamp"
	sigma               float64                 // only for standard deviation
}

type aggregationParser = func(queryMap QueryMap) (model.QueryType, error)

const metricsAggregationDefaultFieldType = clickhouse.Invalid

// Tries to parse metrics aggregation from queryMap. If it's not a metrics aggregation, returns false.
func (cw *ClickhouseQueryTranslator) tryMetricsAggregation(queryMap QueryMap) (metricAggregation metricsAggregation, success bool) {
	if len(queryMap) != 1 {
		return metricsAggregation{}, false
	}
	const dateInSchemaExpected = false

	// full list: https://www.elastic.co/guide/en/elasticsearch/reference/current/search-Aggregations-metrics.html
	// shouldn't be hard to handle others, if necessary

	metricsAggregations := []string{"sum", "avg", "min", "max", "cardinality", "value_count", "stats", "geo_centroid"}
	for k, v := range queryMap {
		if slices.Contains(metricsAggregations, k) {
			field, isFromScript := cw.parseFieldFieldMaybeScript(v, k)

			return metricsAggregation{
				AggrType:            k,
				Fields:              []model.Expr{field},
				FieldType:           cw.GetDateTimeTypeFromSelectClause(cw.Ctx, field, dateInSchemaExpected),
				IsFieldNameCompound: isFromScript,
			}, true
		}
	}

	if percentile, ok := queryMap["percentiles"]; ok {
		percentileMap, ok := percentile.(QueryMap)
		if !ok {
			logger.WarnWithCtx(cw.Ctx).Msgf("percentiles is not a map, but %T, value: %v. Using empty map.", percentile, percentile)
		}
		field, keyed, percentiles := cw.parsePercentilesAggregation(percentileMap)

		return metricsAggregation{
			AggrType:    "quantile",
			Fields:      []model.Expr{field},
			FieldType:   cw.GetDateTimeTypeFromSelectClause(cw.Ctx, field, dateInSchemaExpected),
			Percentiles: percentiles,
			Keyed:       keyed,
		}, true
	}

	if topMetrics, ok := queryMap["top_metrics"]; ok {
		topMetricsMap, ok := topMetrics.(QueryMap)
		if !ok {
			logger.WarnWithCtx(cw.Ctx).Msgf("top_metrics is not a map, but %T, value: %v. Using empty map.", topMetrics, topMetrics)
		}
		topMetricsAggrParams := cw.ParseTopMetricsAggregation(topMetricsMap)
		return topMetricsAggrParams, true
	}

	if parsedTopHits, ok := cw.parseTopHits(queryMap); ok {
		return parsedTopHits, true
	}

	// Shortcut here. Percentile_ranks has "field" and a list of "values"
	// I'm keeping all of them in `fieldNames' array for "simplicity".
	if percentileRanks, ok := queryMap["percentile_ranks"]; ok {
		var cutValuesRaw []any
		if values, exists := percentileRanks.(QueryMap)["values"]; exists {
			cutValuesRaw, ok = values.([]any)
			if !ok {
				logger.WarnWithCtx(cw.Ctx).Msgf("values in percentile_ranks is not an array, but %T, value: %v. Using empty array.", values, values)
			}
		} else {
			logger.WarnWithCtx(cw.Ctx).Msg("no values in percentile_ranks. Using empty array.")
		}

		percentiles := make(map[string]float64, len(cutValuesRaw))
		cutValues := make([]string, 0, len(cutValuesRaw))
		for _, cutValue := range cutValuesRaw {
			switch cutValueTyped := cutValue.(type) {
			case float64:
				asFloat := strconv.FormatFloat(cutValueTyped, 'f', -1, 64)
				cutValues = append(cutValues, asFloat)
				percentiles[asFloat] = cutValueTyped
			case int64:
				asInt := strconv.FormatInt(cutValueTyped, 10)
				cutValues = append(cutValues, asInt)
				percentiles[asInt] = float64(cutValueTyped)
			default:
				logger.WarnWithCtx(cw.Ctx).Msgf("cutValue in percentile_ranks is not a number, but %T, value: %v. Skipping.", cutValue, cutValue)
			}
		}

		var keyed bool
		if keyedRaw, ok := percentileRanks.(QueryMap)["keyed"]; ok {
			if keyed, ok = keyedRaw.(bool); !ok {
				logger.WarnWithCtx(cw.Ctx).Msgf("keyed specified for percentiles aggregation is not a boolean. Querymap: %v", queryMap)
				keyed = keyedDefaultValuePercentileRanks
			}
		} else {
			keyed = keyedDefaultValuePercentileRanks
		}

		return metricsAggregation{
			AggrType:    "percentile_ranks",
			Fields:      []model.Expr{cw.parseFieldField(percentileRanks, "percentile_ranks")},
			FieldType:   metricsAggregationDefaultFieldType, // don't need to check, it's unimportant for this aggregation
			Keyed:       keyed,
			CutValues:   cutValues,
			Percentiles: percentiles,
		}, true
	}

	if extendedStatsRaw, exists := queryMap["extended_stats"]; exists {
		extendedStats, ok := extendedStatsRaw.(QueryMap)
		if !ok {
			logger.WarnWithCtx(cw.Ctx).Msgf("extended_stats is not a map, but %T, value: %v. Skipping.", extendedStatsRaw, extendedStatsRaw)
			return metricsAggregation{}, false
		}
		const defaultSigma = 2.0
		sigma := defaultSigma
		if sigmaRaw, exists := extendedStats["sigma"]; exists {
			sigma, ok = sigmaRaw.(float64)
			if !ok {
				logger.WarnWithCtx(cw.Ctx).Msgf("sigma in extended_stats is not a float64, but %T, value: %v. Using default.", sigmaRaw, sigmaRaw)
				sigma = defaultSigma
			}
		}
		return metricsAggregation{
			AggrType: "extended_stats",
			Fields:   []model.Expr{cw.parseFieldField(extendedStats, "extended_stats")},
			sigma:    sigma,
		}, true
	}

	return metricsAggregation{}, false
}

func (cw *ClickhouseQueryTranslator) parseTopHits(queryMap QueryMap) (parsedTopHits metricsAggregation, success bool) {
	paramsRaw, ok := queryMap["top_hits"]
	if !ok {
		return
	}
	params, ok := paramsRaw.(QueryMap)
	if !ok {
		logger.WarnWithCtx(cw.Ctx).Msgf("top_hits is not a map, but %T, value: %v. Skipping", paramsRaw, paramsRaw)
		return
	}

	const defaultSize = 1
	size := cw.parseSize(params, defaultSize)

	orderBy, err := cw.parseOrder(params, []model.Expr{})
	if err != nil {
		logger.WarnWithCtx(cw.Ctx).Msgf("error parsing order in top_hits: %v", err)
		return
	}
	if len(orderBy) == 1 && orderBy[0].IsCountDesc() { // we don't need count DESC
		orderBy = []model.OrderByExpr{}
	}

	return metricsAggregation{
		AggrType:  "top_hits",
		Fields:    cw.parseSourceField(params["_source"]),
		FieldType: metricsAggregationDefaultFieldType, // don't need to check, it's unimportant for this aggregation
		Size:      size,
		OrderBy:   orderBy,
	}, true
}

// It's not 100% full support, but 2 most common ones: source: string, and source: {includes: []string}
// https://www.elastic.co/guide/en/elasticsearch/reference/current/search-fields.html#source-filtering
func (cw *ClickhouseQueryTranslator) parseSourceField(source any) (fields []model.Expr) {
	if source == nil {
		logger.WarnWithCtx(cw.Ctx).Msgf("no _source in top_hits not supported. Using empty.")
		return
	}

	if sourceAsStr, ok := source.(string); ok {
		return []model.Expr{model.NewColumnRef(sourceAsStr)}
	}

	sourceMap, ok := source.(QueryMap)
	if !ok {
		logger.WarnWithCtx(cw.Ctx).Msgf("_source in top_hits is not a string nor a map, but %T, value: %v. Using empty.", source, source)
		return
	}
	includesRaw, ok := sourceMap["includes"]
	if !ok {
		logger.WarnWithCtx(cw.Ctx).Msgf("Empty _source['includes'] in top_hits not supported. Using empty.")
		return
	}
	includes, ok := includesRaw.([]any)
	if !ok {
		logger.WarnWithCtx(cw.Ctx).Msgf("_source['includes'] in top_hits is not an array, but %T, value: %v. Using empty.", includesRaw, includesRaw)
	}

	for i, fieldNameRaw := range includes {
		if fieldName, ok := fieldNameRaw.(string); ok {
			fields = append(fields, model.NewColumnRef(fieldName))
		} else {
			logger.WarnWithCtx(cw.Ctx).Msgf("field %d in top_hits is not a string. Field's type: %T, value: %v. Skipping.",
				i, fieldNameRaw, fieldNameRaw)
		}
	}

	return
}

// parseFieldField returns field 'field' from shouldBeMap, which should be a string. Logs some warnings in case of errors, and returns "" then
func (cw *ClickhouseQueryTranslator) parseFieldField(shouldBeMap any, aggregationType string) model.Expr {
	Map, ok := shouldBeMap.(QueryMap)
	if !ok {
		logger.WarnWithCtx(cw.Ctx).Msgf("%s aggregation is not a map, but %T, value: %v", aggregationType, shouldBeMap, shouldBeMap)
		return nil
	}
	if fieldRaw, ok := Map["field"]; ok {
		if field, ok := fieldRaw.(string); ok {
			return model.NewColumnRef(field)
		} else {
			logger.WarnWithCtx(cw.Ctx).Msgf("field is not a string, but %T, value: %v", fieldRaw, fieldRaw)
		}
	} else {
		logger.WarnWithCtx(cw.Ctx).Msgf("field not found in %s aggregation: %v", aggregationType, Map)
	}
	return nil
}

func (cw *ClickhouseQueryTranslator) parseIntField(queryMap QueryMap, fieldName string, defaultValue int) int {
	if valueRaw, exists := queryMap[fieldName]; exists {
		if asFloat, ok := valueRaw.(float64); ok {
			return int(asFloat)
		}
		logger.WarnWithCtx(cw.Ctx).Msgf("%s is not an float64, but %T, value: %v. Using default: %d", fieldName, valueRaw, valueRaw, defaultValue)
	}
	return defaultValue
}

func (cw *ClickhouseQueryTranslator) parseInt64Field(queryMap QueryMap, fieldName string, defaultValue int64) int64 {
	if valueRaw, exists := queryMap[fieldName]; exists {
		if asFloat, ok := valueRaw.(float64); ok {
			return int64(asFloat)
		}
		logger.WarnWithCtx(cw.Ctx).Msgf("%s is not an float64, but %T, value: %v. Using default: %d", fieldName, valueRaw, valueRaw, defaultValue)
	}
	return defaultValue
}

func (cw *ClickhouseQueryTranslator) parseFloatField(queryMap QueryMap, fieldName string, defaultValue float64) float64 {
	if valueRaw, exists := queryMap[fieldName]; exists {
		if asFloat, ok := valueRaw.(float64); ok {
			return asFloat
		}
		logger.WarnWithCtx(cw.Ctx).Msgf("%s is not an float64, but %T, value: %v. Using default: %f", fieldName, valueRaw, valueRaw, defaultValue)
	}
	return defaultValue
}

func (cw *ClickhouseQueryTranslator) parseStringField(queryMap QueryMap, fieldName string, defaultValue string) string {
	if valueRaw, exists := queryMap[fieldName]; exists {
		if asString, ok := valueRaw.(string); ok {
			return asString
		}
		logger.WarnWithCtx(cw.Ctx).Msgf("%s is not a string, but %T, value: %v. Using default: %s", fieldName, valueRaw, valueRaw, defaultValue)
	}
	return defaultValue
}

func (cw *ClickhouseQueryTranslator) parseStringFieldExistCheck(queryMap QueryMap, fieldName string) (value string, exists bool) {
	if valueRaw, exists := queryMap[fieldName]; exists {
		if asString, ok := valueRaw.(string); ok {
			return asString, true
		}
		logger.WarnWithCtx(cw.Ctx).Msgf("%s is not a string, but %T, value: %v", fieldName, valueRaw, valueRaw)
	}
	return "", false
}

func (cw *ClickhouseQueryTranslator) parseArrayField(queryMap QueryMap, fieldName string) ([]any, error) {
	if valueRaw, exists := queryMap[fieldName]; exists {
		if asArray, ok := valueRaw.([]any); ok {
			return asArray, nil
		}
		return nil, fmt.Errorf("%s is not an array, but %T, value: %v", fieldName, valueRaw, valueRaw)
	}
	return nil, fmt.Errorf("array field '%s' not found in aggregation queryMap: %v", fieldName, queryMap)
}

func (cw *ClickhouseQueryTranslator) parseBoolField(queryMap QueryMap, fieldName string, defaultValue bool) bool {
	if valueRaw, exists := queryMap[fieldName]; exists {
		if asBool, ok := valueRaw.(bool); ok {
			return asBool
		}
		logger.WarnWithCtx(cw.Ctx).Msgf("%s is not a bool, but %T, value: %v. Using default: %v", fieldName, valueRaw, valueRaw, defaultValue)
	}
	return defaultValue
}

// parseFieldFieldMaybeScript is basically almost a copy of parseFieldField above, but it also handles a basic script, if "field" is missing.
func (cw *ClickhouseQueryTranslator) parseFieldFieldMaybeScript(shouldBeMap any, aggregationType string) (field model.Expr, isFromScript bool) {
	Map, ok := shouldBeMap.(QueryMap)
	if !ok {
		logger.WarnWithCtx(cw.Ctx).Msgf("%s aggregation is not a map, but %T, value: %v", aggregationType, shouldBeMap, shouldBeMap)
		return
	}
	// maybe "field" field
	if fieldRaw, ok := Map["field"]; ok {
		if field, ok := fieldRaw.(string); ok {
			return model.NewColumnRef(field), false
		} else {
			logger.WarnWithCtx(cw.Ctx).Msgf("field is not a string, but %T, value: %v", fieldRaw, fieldRaw)
		}
	}

	// else: maybe script
	if field, isFromScript = cw.parseFieldFromScriptField(Map); !isFromScript {
		logger.WarnWithCtx(cw.Ctx).Msgf("field not found in %s aggregation: %v", aggregationType, Map)
	}
	return
}

// parseFieldFromScriptField returns (field, true) if parsing succeeded, (model.SelectColumn{}, false) otherwise.
func (cw *ClickhouseQueryTranslator) parseFieldFromScriptField(queryMap QueryMap) (field model.Expr, success bool) {
	scriptRaw, exists := queryMap["script"]
	if !exists {
		return
	}
	script, ok := scriptRaw.(QueryMap)
	if !ok {
		logger.WarnWithCtx(cw.Ctx).Msgf("script is not a JsonMap, but %T, value: %v", scriptRaw, scriptRaw)
		return
	}

	sourceRaw, exists := script["source"]
	if !exists {
		logger.WarnWithCtx(cw.Ctx).Msgf("source not found in script: %v", script)
		return
	}
	source, ok := sourceRaw.(string)
	if !ok {
		logger.WarnWithCtx(cw.Ctx).Msgf("source is not a string, but %T, value: %v", sourceRaw, sourceRaw)
	}

	// a) source must look like "doc['field_name'].value.getHour()" or "doc['field_name'].value.hourOfDay"
	wantedRegex := regexp.MustCompile(`^doc\['(\w+)']\.value\.(?:getHour\(\)|hourOfDay)$`)
	matches := wantedRegex.FindStringSubmatch(source)
	if len(matches) == 2 {
		return model.NewFunction("toHour", model.NewColumnRef(matches[1])), true
	}

	// b) source: "if (doc['field_name_1'].value == doc['field_name_2'].value") { return 1; } else { return 0; }"
	wantedRegex = regexp.MustCompile(`^if \(doc\['(.*)\.value']\.value == doc\['(.*)\.value'].value\) \{ \n  return 1; \n} else \{ \n  return 0; \n}$`)
	matches = wantedRegex.FindStringSubmatch(source)
	if len(matches) == 3 {
		return model.NewInfixExpr(model.NewColumnRef(matches[1]), "=", model.NewColumnRef(matches[2])), true
	}

	return
}

// quoteArray returns a new array with the same elements, but quoted
func quoteArray(array []string) []string {
	quotedArray := make([]string, 0, len(array))
	for _, el := range array {
		quotedArray = append(quotedArray, strconv.Quote(el))
	}
	return quotedArray
}
