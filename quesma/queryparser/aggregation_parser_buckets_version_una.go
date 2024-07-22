// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package queryparser

import (
	"quesma/clickhouse"
	"quesma/logger"
	"quesma/model"
	"quesma/model/bucket_aggregations"
	"quesma/model/metrics_aggregations"
	"strconv"
	"strings"
)

func (cw *ClickhouseQueryTranslator) tryBucketAggregationVersionUna(aggregation *aggregationLevelVersionUna, queryMap QueryMap) (success bool, err error) {

	success = true // returned in most cases
	if histogramRaw, ok := queryMap["histogram"]; ok {
		histogram, ok := histogramRaw.(QueryMap)
		if !ok {
			logger.WarnWithCtx(cw.Ctx).Msgf("date_histogram is not a map, but %T, value: %v", histogramRaw, histogramRaw)
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
				logger.ErrorWithCtx(cw.Ctx).Err(err).Msgf("failed to parse interval: %v", intervalRaw)
			}
		case int:
			interval = float64(intervalTyped)
		case float64:
			interval = intervalTyped
		default:
			interval = 1.0
			logger.ErrorWithCtx(cw.Ctx).Msgf("unexpected type of interval: %T, value: %v", intervalTyped, intervalTyped)
		}
		minDocCount := cw.parseMinDocCount(histogram)
		aggregation.Type = bucket_aggregations.NewHistogram(cw.Ctx, interval, minDocCount)

		field, _ := cw.parseFieldFieldMaybeScript(histogram, "histogram")
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

		aggregation.SelectedColumns = append(aggregation.SelectedColumns, col)

		delete(queryMap, "histogram")
		return success, nil
	}
	if dateHistogramRaw, ok := queryMap["date_histogram"]; ok {
		dateHistogram, ok := dateHistogramRaw.(QueryMap)
		if !ok {
			logger.WarnWithCtx(cw.Ctx).Msgf("date_histogram is not a map, but %T, value: %v", dateHistogramRaw, dateHistogramRaw)
		}
		field := cw.parseFieldField(dateHistogram, "date_histogram")
		minDocCount := cw.parseMinDocCount(dateHistogram)
		interval, intervalType := cw.extractInterval(dateHistogram)
		dateTimeType := cw.Table.GetDateTimeTypeFromExpr(cw.Ctx, field)

		if dateTimeType == clickhouse.Invalid {
			logger.WarnWithCtx(cw.Ctx).Msgf("invalid date time type for field %s", field)
		}

		dateHistogramAggr := bucket_aggregations.NewDateHistogram(cw.Ctx, field, interval, minDocCount, intervalType, dateTimeType)
		aggregation.Type = dateHistogramAggr

		sqlQuery := dateHistogramAggr.GenerateSQL()
		aggregation.SelectedColumns = append(aggregation.SelectedColumns, sqlQuery)

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
			logger.WarnWithCtx(cw.Ctx).Msgf("%s is not a map, but %T, value: %v", termsType, termsRaw, termsRaw)
			continue
		}

		// Parse 'missing' parameter. It can be any type.
		var missingPlaceholder any
		if terms["missing"] != nil {
			missingPlaceholder = terms["missing"]
		}

		fieldExpression := cw.parseFieldField(terms, termsType)

		// apply missing placeholder if it is set
		if missingPlaceholder != nil {
			var value model.LiteralExpr

			// Maybe we should check the input type against the schema?
			// Right now we quote if it's a string.
			switch val := missingPlaceholder.(type) {
			case string:
				value = model.NewLiteral("'" + val + "'")
			default:
				value = model.NewLiteral(missingPlaceholder)
			}

			fieldExpression = model.NewFunction("COALESCE", fieldExpression, value)
		}

		size := 10
		if sizeRaw, ok := terms["size"]; ok {
			if sizeParsed, ok := sizeRaw.(float64); ok {
				size = int(sizeParsed)
			} else {
				logger.WarnWithCtx(cw.Ctx).Msgf("size is not an float64, but %T, value: %v. Using default", sizeRaw, sizeRaw)
			}
		}

		defaultMainOrderBy := model.NewCountFunc()
		defaultDirection := model.DescOrder

		var mainOrderBy model.Expr = defaultMainOrderBy
		fullOrderBy := []model.OrderByExpr{ // default
			{Exprs: []model.Expr{mainOrderBy}, Direction: defaultDirection, ExchangeToAliasInCTE: true},
			{Exprs: []model.Expr{fieldExpression}},
		}
		direction := defaultDirection
		if orderRaw, exists := terms["order"]; exists {
			if order, ok := orderRaw.(QueryMap); ok { // TODO it can be array too, don't handle it yet
				if len(order) == 1 {
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
							fullOrderBy = []model.OrderByExpr{{Exprs: []model.Expr{fieldExpression}, Direction: direction}}
							break // mainOrderBy remains default
						} else if key != "_count" {
							mainOrderBy = cw.findMetricAggregationVersionUna(queryMap, key)
						}

						fullOrderBy = []model.OrderByExpr{
							{Exprs: []model.Expr{mainOrderBy}, Direction: direction, ExchangeToAliasInCTE: true},
							{Exprs: []model.Expr{fieldExpression}},
						}
					}
				} else {
					logger.ErrorWithCtx(cw.Ctx).Msgf("order has more than 1 key, but %d. Order: %+v. Using default", len(order), order)
				}
			} else {
				logger.ErrorWithCtx(cw.Ctx).Msgf("order is not a map, but %T, value: %v. Using default order", orderRaw, orderRaw)
			}
		}

		aggregation.Type = bucket_aggregations.NewTerms(cw.Ctx, termsType == "significant_terms", mainOrderBy)
		aggregation.SelectedColumns = append(aggregation.SelectedColumns, fieldExpression)
		aggregation.Limit = size
		aggregation.OrderBy = &fullOrderBy
		if missingPlaceholder == nil { // TODO replace with schema
			aggregation.whereBuilder = model.NewSimpleQuery(
				model.NewInfixExpr(fieldExpression, "IS", model.NewLiteral("NOT NULL")), true)
		}

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

		aggregation.OrderBy = &[]model.OrderByExpr{model.NewSortByCountColumn(model.DescOrder)}
		aggregation.Limit = size

		var fieldsNr int
		if termsRaw, exists := multiTerms["terms"]; exists {
			terms, ok := termsRaw.([]any)
			if !ok {
				logger.WarnWithCtx(cw.Ctx).Msgf("terms is not an array, but %T, value: %v. Using empty array", termsRaw, termsRaw)
			}
			fieldsNr = len(terms)
			for _, term := range terms {
				column := cw.parseFieldField(term, "multi_terms")
				aggregation.SelectedColumns = append(aggregation.SelectedColumns, column)
			}
		} else {
			logger.WarnWithCtx(cw.Ctx).Msg("no terms in multi_terms")
		}

		aggregation.Type = bucket_aggregations.NewMultiTerms(cw.Ctx, fieldsNr)
		aggregation.Aggregator.SplitOverHowManyFields = fieldsNr

		delete(queryMap, "multi_terms")
		return success, nil
	}
	if rangeRaw, ok := queryMap["range"]; ok {
		rangeMap, ok := rangeRaw.(QueryMap)
		if !ok {
			logger.WarnWithCtx(cw.Ctx).Msgf("range is not a map, but %T, value: %v. Using empty map", rangeRaw, rangeRaw)
		}
		Range := cw.parseRangeAggregation(rangeMap)
		aggregation.Type = Range
		if Range.Keyed {
			aggregation.Aggregator.Keyed = true
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
		aggregation.Type = dateRangeParsed
		for _, interval := range dateRangeParsed.Intervals {

			aggregation.SelectedColumns = append(aggregation.SelectedColumns, interval.ToSQLSelectQuery(dateRangeParsed.FieldName))

			if sqlSelect, selectNeeded := interval.BeginTimestampToSQL(); selectNeeded {
				aggregation.SelectedColumns = append(aggregation.SelectedColumns, sqlSelect)
			}
			if sqlSelect, selectNeeded := interval.EndTimestampToSQL(); selectNeeded {
				aggregation.SelectedColumns = append(aggregation.SelectedColumns, sqlSelect)
			}
		}

		// TODO after https://github.com/QuesmaOrg/quesma/pull/99 it should be only in 1 of 2 cases (keyed or not), just like in range aggregation
		aggregation.Aggregator.SplitOverHowManyFields = 1

		delete(queryMap, "date_range")
		return success, nil
	}
	if geoTileGridRaw, ok := queryMap["geotile_grid"]; ok {
		geoTileGrid, ok := geoTileGridRaw.(QueryMap)
		if !ok {
			logger.WarnWithCtx(cw.Ctx).Msgf("geotile_grid is not a map, but %T, value: %v", geoTileGridRaw, geoTileGridRaw)
		}
		var precision float64
		precisionRaw, ok := geoTileGrid["precision"]
		if ok {
			switch cutValueTyped := precisionRaw.(type) {
			case float64:
				precision = cutValueTyped
			}
		}
		field := cw.parseFieldField(geoTileGrid, "geotile_grid")
		aggregation.Type = bucket_aggregations.NewGeoTileGrid(cw.Ctx)

		// That's bucket (group by) formula for geotile_grid
		// zoom/x/y
		//	SELECT precision as zoom,
		//	    FLOOR(((toFloat64("Location::lon") + 180.0) / 360.0) * POWER(2, zoom)) AS x_tile,
		//	    FLOOR(
		//	        (
		//	            1 - LOG(TAN(RADIANS(toFloat64("Location::lat"))) + (1 / COS(RADIANS(toFloat64("Location::lat"))))) / PI()
		//	        ) / 2.0 * POWER(2, zoom)
		//	    ) AS y_tile, count()
		//	FROM
		//	     kibana_sample_data_flights Group by zoom, x_tile, y_tile

		// TODO columns names should be created according to the schema
		var lon = model.AsString(field)
		lon = strings.Trim(lon, "\"")
		lon = lon + "::lon"
		var lat = model.AsString(field)
		lat = strings.Trim(lat, "\"")
		lat = lat + "::lat"
		toFloatFunLon := model.NewFunction("toFloat64", model.NewColumnRef(lon))
		var infixX model.Expr
		infixX = model.NewParenExpr(model.NewInfixExpr(toFloatFunLon, "+", model.NewLiteral(180.0)))
		infixX = model.NewParenExpr(model.NewInfixExpr(infixX, "/", model.NewLiteral(360.0)))
		infixX = model.NewInfixExpr(infixX, "*",
			model.NewFunction("POWER", model.NewLiteral(2), model.NewLiteral("zoom")))
		xTile := model.NewAliasedExpr(model.NewFunction("FLOOR", infixX), "x_tile")
		toFloatFunLat := model.NewFunction("toFloat64", model.NewColumnRef(lat))
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
			model.NewFunction("POWER", model.NewLiteral(2), model.NewLiteral("zoom")))
		yTile := model.NewAliasedExpr(
			model.NewFunction("FLOOR", FloorContent), "y_tile")

		aggregation.SelectedColumns = append(aggregation.SelectedColumns, model.NewAliasedExpr(model.NewLiteral(precision), "zoom"))
		aggregation.SelectedColumns = append(aggregation.SelectedColumns, xTile)
		aggregation.SelectedColumns = append(aggregation.SelectedColumns, yTile)

		delete(queryMap, "geotile_grid")
		return success, err
	}
	if _, ok := queryMap["sampler"]; ok {
		aggregation.Type = metrics_aggregations.NewCount(cw.Ctx)
		delete(queryMap, "sampler")
		return
	}
	// Let's treat random_sampler just like sampler for now, until we add `LIMIT` logic to sampler.
	// Random sampler doesn't have `size` field, but `probability`, so logic in the final version should be different.
	// So far I've only observed its "probability" field to be 1.0, so it's not really important.
	if _, ok := queryMap["random_sampler"]; ok {
		aggregation.Type = metrics_aggregations.NewCount(cw.Ctx)
		delete(queryMap, "random_sampler")
		return
	}
	if boolRaw, ok := queryMap["bool"]; ok {
		if Bool, ok := boolRaw.(QueryMap); ok {
			aggregation.whereBuilder = cw.parseBool(Bool)
		} else {
			logger.WarnWithCtx(cw.Ctx).Msgf("bool is not a map, but %T, value: %v. Skipping", boolRaw, boolRaw)
		}
		delete(queryMap, "bool")
		return
	}
	if isFilters, filterAggregation := cw.parseFilters(queryMap); isFilters {
		aggregation.Type = filterAggregation
		return
	}
	success = false
	return
}

func (cw *ClickhouseQueryTranslator) findMetricAggregationVersionUna(queryMap QueryMap, aggregationName string) model.Expr {
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
	if aggMapRaw, exists := aggs[aggregationName]; exists {
		aggMap, ok := aggMapRaw.(QueryMap)
		if !ok {
			logger.WarnWithCtx(cw.Ctx).Msgf("aggregation %s is not a map, but %T, value: %v. Skipping", aggregationName, aggMapRaw, aggMapRaw)
			return notFoundValue
		}

		agg, success := cw.tryMetricsAggregation(aggMap)
		if !success {
			logger.WarnWithCtx(cw.Ctx).Msgf("failed to parse metric aggregation: %v", agg)
			return notFoundValue
		}

		// we build a temporary query only to extract the name of the metric
		columns, err := generateMetricSelectedColumns(cw.Ctx, agg)
		if err != nil {
			logger.ErrorWithCtx(cw.Ctx).Err(err).Msg("failed to generate metric selected columns")
			return notFoundValue
		}
		if len(columns) != 1 {
			logger.ErrorWithCtx(cw.Ctx).Msgf("invalid number of columns, expected: 1, got: %d", len(columns))
			return notFoundValue
		}
		return columns[0]
	}
	return notFoundValue
}
