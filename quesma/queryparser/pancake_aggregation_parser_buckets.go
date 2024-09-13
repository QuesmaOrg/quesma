// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package queryparser

import (
	"fmt"
	"quesma/clickhouse"
	"quesma/logger"
	"quesma/model"
	"quesma/model/bucket_aggregations"
	"quesma/util"
	"sort"
	"strconv"
	"strings"
)

func (cw *ClickhouseQueryTranslator) pancakeTryBucketAggregation(aggregation *pancakeAggregationTreeNode, queryMap QueryMap) (success bool, err error) {

	success = true // returned in most cases
	if histogramRaw, ok := queryMap["histogram"]; ok {
		histogram, ok := histogramRaw.(QueryMap)
		if !ok {
			return false, fmt.Errorf("histogram is not a map, but %T, value: %v", histogramRaw, histogramRaw)
		}

		var interval float64
		intervalRaw, ok := histogram["interval"]
		if !ok {
			return false, fmt.Errorf("interval not found in histogram: %v", histogram)
		}
		switch intervalTyped := intervalRaw.(type) {
		case string:
			var err error
			interval, err = strconv.ParseFloat(intervalTyped, 64)
			if err != nil {
				return false, fmt.Errorf("failed to parse interval: %v", intervalRaw)
			}
		case int:
			interval = float64(intervalTyped)
		case float64:
			interval = intervalTyped
		default:
			interval = 1.0
			logger.WarnWithCtx(cw.Ctx).Msgf("unexpected type of interval: %T, value: %v. Will use 1.0.", intervalTyped, intervalTyped)
		}
		minDocCount := cw.parseMinDocCount(histogram)
		aggregation.queryType = bucket_aggregations.NewHistogram(cw.Ctx, interval, minDocCount)

		field, _ := cw.parseFieldFieldMaybeScript(histogram, "histogram")
		field, didWeAddMissing := cw.addMissingParameterIfPresent(field, histogram)
		if !didWeAddMissing {
			aggregation.filterOutEmptyKeyBucket = true
		}

		var col model.Expr
		if interval != 1.0 {
			// col as string is: fmt.Sprintf("floor(%s / %f) * %f", fieldNameProperlyQuoted, interval, interval)
			col = model.NewInfixExpr(
				model.NewFunction("floor", model.NewInfixExpr(field, "/", model.NewLiteral(interval))),
				"*",
				model.NewLiteral(interval),
			)
		} else {
			col = field
		}

		aggregation.selectedColumns = append(aggregation.selectedColumns, col)
		aggregation.orderBy = append(aggregation.orderBy, model.NewOrderByExprWithoutOrder(col))

		delete(queryMap, "histogram")
		return success, nil
	}
	if dateHistogramRaw, ok := queryMap["date_histogram"]; ok {
		dateHistogram, ok := dateHistogramRaw.(QueryMap)
		if !ok {
			return false, fmt.Errorf("date_histogram is not a map, but %T, value: %v", dateHistogramRaw, dateHistogramRaw)
		}
		field := cw.parseFieldField(dateHistogram, "date_histogram")
		minDocCount := cw.parseMinDocCount(dateHistogram)
		timezone := cw.parseStringField(dateHistogram, "time_zone", "")
		interval, intervalType := cw.extractInterval(dateHistogram)
		dateTimeType := cw.Table.GetDateTimeTypeFromExpr(cw.Ctx, field)

		if dateTimeType == clickhouse.Invalid {
			logger.WarnWithCtx(cw.Ctx).Msgf("invalid date time type for field %s", field)
		}

		dateHistogramAggr := bucket_aggregations.NewDateHistogram(
			cw.Ctx, field, interval, timezone, minDocCount, intervalType, dateTimeType)
		aggregation.queryType = dateHistogramAggr

		sqlQuery := dateHistogramAggr.GenerateSQL()
		aggregation.selectedColumns = append(aggregation.selectedColumns, sqlQuery)
		aggregation.orderBy = append(aggregation.orderBy, model.NewOrderByExprWithoutOrder(sqlQuery))

		delete(queryMap, "date_histogram")
		return success, nil
	}
	for _, termsType := range []string{"terms", "significant_terms"} {
		termsRaw, ok := queryMap[termsType]
		if !ok {
			continue
		}
		terms, ok := termsRaw.(QueryMap)
		if !ok {
			return false, fmt.Errorf("%s is not a map, but %T, value: %v", termsType, termsRaw, termsRaw)
		}

		fieldExpression := cw.parseFieldField(terms, termsType)
		fieldExpression, didWeAddMissing := cw.addMissingParameterIfPresent(fieldExpression, terms)
		if !didWeAddMissing {
			aggregation.filterOutEmptyKeyBucket = true
		}

		size := 10
		if sizeRaw, ok := terms["size"]; ok {
			if sizeParsed, ok := sizeRaw.(float64); ok {
				size = int(sizeParsed)
			} else {
				logger.WarnWithCtx(cw.Ctx).Msgf("size is not an float64, but %T, value: %v. Using default", sizeRaw, sizeRaw)
			}
		}

		orderBy := cw.parseOrder(terms, queryMap, []model.Expr{fieldExpression})
		aggregation.queryType = bucket_aggregations.NewTerms(cw.Ctx, termsType == "significant_terms", orderBy[0]) // TODO probably full, not [0]
		aggregation.selectedColumns = append(aggregation.selectedColumns, fieldExpression)
		aggregation.limit = size
		aggregation.orderBy = orderBy

		delete(queryMap, termsType)
		return success, nil
	}
	if multiTermsRaw, exists := queryMap["multi_terms"]; exists {
		multiTerms, ok := multiTermsRaw.(QueryMap)
		if !ok {
			logger.WarnWithCtx(cw.Ctx).Msgf("multi_terms is not a map, but %T, value: %v", multiTermsRaw, multiTermsRaw)
		}

		const defaultSize = 10
		size := cw.parseIntField(multiTerms, "size", defaultSize)

		aggregation.limit = size

		var fieldsNr int
		if termsRaw, exists := multiTerms["terms"]; exists {
			terms, ok := termsRaw.([]any)
			if !ok {
				logger.WarnWithCtx(cw.Ctx).Msgf("terms is not an array, but %T, value: %v. Using empty array", termsRaw, termsRaw)
			}
			fieldsNr = len(terms)
			columns := make([]model.Expr, 0, fieldsNr)
			for _, term := range terms {
				columns = append(columns, cw.parseFieldField(term, "multi_terms"))
			}
			aggregation.selectedColumns = append(aggregation.selectedColumns, columns...)
			aggregation.orderBy = append(aggregation.orderBy, cw.parseOrder(multiTerms, queryMap, columns)...)
		} else {
			logger.WarnWithCtx(cw.Ctx).Msg("no terms in multi_terms")
		}

		aggregation.queryType = bucket_aggregations.NewMultiTerms(cw.Ctx, fieldsNr)
		aggregation.limit = size

		delete(queryMap, "multi_terms")
		return success, nil
	}
	if rangeRaw, ok := queryMap["range"]; ok {
		rangeMap, ok := rangeRaw.(QueryMap)
		if !ok {
			logger.WarnWithCtx(cw.Ctx).Msgf("range is not a map, but %T, value: %v. Using empty map", rangeRaw, rangeRaw)
		}
		Range := cw.parseRangeAggregation(rangeMap)
		aggregation.queryType = Range
		if Range.Keyed {
			aggregation.isKeyed = true
		}
		delete(queryMap, "range")
		return success, nil
	}
	if dateRangeRaw, ok := queryMap["date_range"]; ok {
		dateRange, ok := dateRangeRaw.(QueryMap)
		if !ok {
			logger.WarnWithCtx(cw.Ctx).Msgf("date_range is not a map, but %T, value: %v. Using empty map", dateRangeRaw, dateRangeRaw)
		}
		dateRangeParsed, err := cw.parseDateRangeAggregation(dateRange)
		if err != nil {
			logger.ErrorWithCtx(cw.Ctx).Err(err).Msg("failed to parse date_range aggregation")
			return false, err
		}
		aggregation.queryType = dateRangeParsed
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

		delete(queryMap, "date_range")
		return success, nil
	}
	if geoTileGridRaw, ok := queryMap["geotile_grid"]; ok {
		geoTileGrid, ok := geoTileGridRaw.(QueryMap)
		if !ok {
			logger.WarnWithCtx(cw.Ctx).Msgf("geotile_grid is not a map, but %T, value: %v", geoTileGridRaw, geoTileGridRaw)
		}
		var precisionZoom float64
		precisionRaw, ok := geoTileGrid["precision"]
		if ok {
			switch cutValueTyped := precisionRaw.(type) {
			case float64:
				precisionZoom = cutValueTyped
			}
		}
		field := cw.parseFieldField(geoTileGrid, "geotile_grid")
		aggregation.queryType = bucket_aggregations.NewGeoTileGrid(cw.Ctx)

		// That's bucket (group by) formula for geotile_grid
		// zoom/x/y
		//	SELECT precisionZoom as zoom,
		//	    FLOOR(((toFloat64("Location::lon") + 180.0) / 360.0) * POWER(2, zoom)) AS x_tile,
		//	    FLOOR(
		//	        (
		//	            1 - LOG(TAN(RADIANS(toFloat64("Location::lat"))) + (1 / COS(RADIANS(toFloat64("Location::lat"))))) / PI()
		//	        ) / 2.0 * POWER(2, zoom)
		//	    ) AS y_tile, count()
		//	FROM
		//	     kibana_sample_data_flights Group by zoom, x_tile, y_tile

		zoomLiteral := model.NewLiteral(precisionZoom)

		fieldName, err := strconv.Unquote(model.AsString(field))
		if err != nil {
			return false, err
		}
		lon := model.NewGeoLon(fieldName)
		lat := model.NewGeoLat(fieldName)

		toFloatFunLon := model.NewFunction("toFloat64", lon)
		var infixX model.Expr
		infixX = model.NewParenExpr(model.NewInfixExpr(toFloatFunLon, "+", model.NewLiteral(180.0)))
		infixX = model.NewParenExpr(model.NewInfixExpr(infixX, "/", model.NewLiteral(360.0)))
		infixX = model.NewInfixExpr(infixX, "*",
			model.NewFunction("POWER", model.NewLiteral(2), zoomLiteral))
		xTile := model.NewFunction("FLOOR", infixX)
		toFloatFunLat := model.NewFunction("toFloat64", lat)
		radians := model.NewFunction("RADIANS", toFloatFunLat)
		tan := model.NewFunction("TAN", radians)
		cos := model.NewFunction("COS", radians)
		Log := model.NewFunction("LOG", model.NewInfixExpr(tan, "+",
			model.NewParenExpr(model.NewInfixExpr(model.NewLiteral(1), "/", cos))))

		FloorContent := model.NewInfixExpr(
			model.NewInfixExpr(
				model.NewParenExpr(
					model.NewInfixExpr(model.NewInfixExpr(model.NewLiteral(1), "-", Log), "/",
						model.NewLiteral("PI()"))), "/",
				model.NewLiteral(2.0)), "*",
			model.NewFunction("POWER", model.NewLiteral(2), zoomLiteral))
		yTile := model.NewFunction("FLOOR", FloorContent)

		aggregation.selectedColumns = append(aggregation.selectedColumns, model.NewLiteral(fmt.Sprintf("CAST(%f AS Float32)", precisionZoom)))
		aggregation.selectedColumns = append(aggregation.selectedColumns, xTile)
		aggregation.selectedColumns = append(aggregation.selectedColumns, yTile)

		delete(queryMap, "geotile_grid")
		return success, err
	}
	if sampler, ok := queryMap["sampler"]; ok {
		aggregation.queryType = cw.parseSampler(sampler)
		delete(queryMap, "sampler")
		return
	}
	if randomSampler, ok := queryMap["random_sampler"]; ok {
		aggregation.queryType = cw.parseRandomSampler(randomSampler)
		delete(queryMap, "random_sampler")
		return
	}
	if isFilters, filterAggregation := cw.parseFilters(queryMap); isFilters {
		sort.Slice(filterAggregation.Filters, func(i, j int) bool { // stable order is required for tests and caching
			return filterAggregation.Filters[i].Name < filterAggregation.Filters[j].Name
		})
		aggregation.isKeyed = true
		aggregation.queryType = filterAggregation
		delete(queryMap, "filters")
		return
	}
	success = false
	return
}

func (cw *ClickhouseQueryTranslator) pancakeFindMetricAggregation(queryMap QueryMap, aggregationName string) model.Expr {
	notFoundValue := model.NewLiteral("")

	aggsRaw, exists := queryMap["aggs"]
	if !exists {
		logger.WarnWithCtx(cw.Ctx).Msgf("no aggs in queryMap, queryMap: %+v", queryMap)
		return notFoundValue
	}
	aggs, ok := aggsRaw.(QueryMap)
	if !ok {
		logger.WarnWithCtx(cw.Ctx).Msgf("aggs is not a map, but %T, value: %v. Skipping", aggsRaw, aggsRaw)
		return notFoundValue
	}

	var percentileNameWeLookFor string
	weTrySplitByDot := false

	// We try 2 things here:
	// First (always): maybe there exists an aggregation with exactly this name
	// Second (if aggregation_name == X.Y): maybe it's aggregationName.some_value, e.g. "2.75", when "2" aggregation is a percentile, and 75 is its value
	aggregationNamesToTry := []string{aggregationName}
	splitByDot := strings.Split(aggregationName, ".")
	if len(splitByDot) == 2 {
		weTrySplitByDot = true
		percentileNameWeLookFor = splitByDot[1]
		aggregationNamesToTry = append(aggregationNamesToTry, splitByDot[0])
	}

	for _, aggNameToTry := range aggregationNamesToTry {
		currentAggMapRaw, exists := aggs[aggNameToTry]
		if !exists {
			continue
		}

		currentAggMap, ok := currentAggMapRaw.(QueryMap)
		if !ok {
			logger.WarnWithCtx(cw.Ctx).Msgf("aggregation %s is not a map, but %T, value: %v. Skipping",
				aggregationName, currentAggMapRaw, currentAggMapRaw)
			continue
		}

		agg, success := cw.tryMetricsAggregation(currentAggMap)
		if !success {
			logger.WarnWithCtx(cw.Ctx).Msgf("failed to parse metric aggregation: %v", agg)
			continue
		}

		// we build a temporary query only to extract the name of the metric
		columns, err := generateMetricSelectedColumns(cw.Ctx, agg)
		if err != nil {
			continue
		}

		if aggNameToTry == aggregationName {
			if len(columns) != 1 {
				continue
			}
			return columns[0]
		} else if weTrySplitByDot {
			userPercents := util.MapKeysSortedByValue(agg.Percentiles)
			for i, percentileName := range userPercents {
				if percentileName == percentileNameWeLookFor {
					return columns[i]
				}
			}
		}
	}

	logger.ErrorWithCtx(cw.Ctx).Msgf("no given metric aggregation found (name: %v, queryMap: %+v)", aggregationName, queryMap)
	return notFoundValue
}

// samplerRaw - in a proper request should be of QueryMap type.
func (cw *ClickhouseQueryTranslator) parseSampler(samplerRaw any) bucket_aggregations.Sampler {
	const defaultSize = 100
	sampler, ok := samplerRaw.(QueryMap)
	if !ok {
		logger.WarnWithCtx(cw.Ctx).Msgf("sampler is not a map, but %T, value: %v", samplerRaw, samplerRaw)
		return bucket_aggregations.NewSampler(cw.Ctx, defaultSize)
	}
	return bucket_aggregations.NewSampler(cw.Ctx, cw.parseIntField(sampler, "shard_size", defaultSize))
}

// randomSamplerRaw - in a proper request should be of QueryMap type.
func (cw *ClickhouseQueryTranslator) parseRandomSampler(randomSamplerRaw any) bucket_aggregations.RandomSampler {
	const defaultProbability = 0.0 // theoretically it's required
	const defaultSeed = 0
	randomSampler, ok := randomSamplerRaw.(QueryMap)
	if !ok {
		logger.WarnWithCtx(cw.Ctx).Msgf("sampler is not a map, but %T, value: %v", randomSamplerRaw, randomSamplerRaw)
		return bucket_aggregations.NewRandomSampler(cw.Ctx, defaultProbability, defaultSeed)
	}
	return bucket_aggregations.NewRandomSampler(
		cw.Ctx,
		cw.parseFloatField(randomSampler, "probability", defaultProbability),
		cw.parseIntField(randomSampler, "seed", defaultSeed),
	)
}

func (cw *ClickhouseQueryTranslator) parseOrder(terms, queryMap QueryMap, fieldExpressions []model.Expr) []model.OrderByExpr {
	defaultMainOrderBy := model.NewCountFunc()
	defaultDirection := model.DescOrder

	fieldOrderBys := make([]model.OrderByExpr, 0, len(fieldExpressions))
	for _, fieldExpression := range fieldExpressions {
		fieldOrderBys = append(fieldOrderBys, model.OrderByExpr{Expr: fieldExpression})
	}

	var mainOrderBy model.Expr = defaultMainOrderBy
	fullOrderBy := []model.OrderByExpr{ // default
		{Expr: mainOrderBy, Direction: defaultDirection},
	}
	fullOrderBy = append(fullOrderBy, fieldOrderBys...)
	direction := defaultDirection

	orderRaw, exists := terms["order"]
	if !exists {
		return fullOrderBy
	}

	order, ok := orderRaw.(QueryMap) // TODO it can be array too, don't handle it yet
	if !ok {
		logger.WarnWithCtx(cw.Ctx).Msgf("order is not a map, but %T, value: %v. Using default order", orderRaw, orderRaw)
		return fullOrderBy
	}
	if len(order) != 1 {
		logger.WarnWithCtx(cw.Ctx).Msgf("order should have 1 key, but has %d. Order: %+v. Using default", len(order), order)
		return fullOrderBy
	}

	for key, valueRaw := range order { // value == "asc" or "desc"
		value, ok := valueRaw.(string)
		if !ok {
			logger.WarnWithCtx(cw.Ctx).Msgf("order value is not a string, but %T, value: %v. Using default (desc)", valueRaw, valueRaw)
			value = "desc"
		}
		if strings.ToLower(value) == "asc" {
			direction = model.AscOrder
		}

		if key == "_key" {
			fullOrderBy = fieldOrderBys
			for i := range fullOrderBy {
				fullOrderBy[i].Direction = direction
			}
			break // mainOrderBy remains default
		} else if key != "_count" {
			mainOrderBy = cw.pancakeFindMetricAggregation(queryMap, key)
		}

		fullOrderBy = []model.OrderByExpr{
			{Expr: mainOrderBy, Direction: direction},
		}
		fullOrderBy = append(fullOrderBy, fieldOrderBys...)
	}

	return fullOrderBy
}

// addMissingParameterIfPresent parses 'missing' parameter. It can be any type.
func (cw *ClickhouseQueryTranslator) addMissingParameterIfPresent(field model.Expr,
	aggrQueryMap QueryMap) (updatedField model.Expr, didWeAddMissing bool) {

	if aggrQueryMap["missing"] == nil {
		return field, false
	}

	// Maybe we should check the input type against the schema?
	// Right now we quote if it's a string.
	var value model.LiteralExpr
	switch val := aggrQueryMap["missing"].(type) {
	case string:
		value = model.NewLiteral("'" + val + "'")
	default:
		value = model.NewLiteral(val)
	}

	return model.NewFunction("COALESCE", field, value), true
}
