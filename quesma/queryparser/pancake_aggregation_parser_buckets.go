// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package queryparser

import (
	"fmt"
	"quesma/clickhouse"
	"quesma/kibana"
	"quesma/logger"
	"quesma/model"
	"quesma/model/bucket_aggregations"
	"sort"
	"strconv"
	"strings"
)

func (cw *ClickhouseQueryTranslator) pancakeTryBucketAggregation(aggregation *pancakeAggregationTreeNode, queryMap QueryMap) error {
	if histogramRaw, ok := queryMap["histogram"]; ok {
		histogram, ok := histogramRaw.(QueryMap)
		if !ok {
			return fmt.Errorf("histogram is not a map, but %T, value: %v", histogramRaw, histogramRaw)
		}

		var interval float64
		intervalRaw, ok := histogram["interval"]
		if !ok {
			return fmt.Errorf("interval not found in histogram: %v", histogram)
		}
		switch intervalTyped := intervalRaw.(type) {
		case string:
			var err error
			interval, err = strconv.ParseFloat(intervalTyped, 64)
			if err != nil {
				return fmt.Errorf("failed to parse interval: %v", intervalRaw)
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
		return nil
	}
	if dateHistogramRaw, ok := queryMap["date_histogram"]; ok {
		dateHistogram, ok := dateHistogramRaw.(QueryMap)
		if !ok {
			return fmt.Errorf("date_histogram is not a map, but %T, value: %v", dateHistogramRaw, dateHistogramRaw)
		}
		field := cw.parseFieldField(dateHistogram, "date_histogram")
		dateTimeType := cw.Table.GetDateTimeTypeFromExpr(cw.Ctx, field)

		weAddedMissing := false
		if missingRaw, exists := dateHistogram["missing"]; exists {
			if missing, ok := missingRaw.(string); ok {
				dateManager := kibana.NewDateManager(cw.Ctx)
				if missingExpr, parsingOk := dateManager.ParseDateUsualFormat(missing, dateTimeType); parsingOk {
					field = model.NewFunction("COALESCE", field, missingExpr)
					weAddedMissing = true
				} else {
					logger.ErrorWithCtx(cw.Ctx).Msgf("unknown format of missing in date_histogram: %v. Skipping it.", missing)
				}
			} else {
				logger.ErrorWithCtx(cw.Ctx).Msgf("missing %v is not a string, but: %T. Skipping it.", missingRaw, missingRaw)
			}
		}
		if !weAddedMissing {
			// if we don't add missing, we need to filter out nulls later
			aggregation.filterOutEmptyKeyBucket = true
		}

		ebMin, ebMax := bucket_aggregations.NoExtendedBound, bucket_aggregations.NoExtendedBound
		if extendedBounds, exists := dateHistogram["extended_bounds"].(QueryMap); exists {
			ebMin = cw.parseInt64Field(extendedBounds, "min", bucket_aggregations.NoExtendedBound)
			ebMax = cw.parseInt64Field(extendedBounds, "max", bucket_aggregations.NoExtendedBound)
		}

		minDocCount := cw.parseMinDocCount(dateHistogram)
		timezone := cw.parseStringField(dateHistogram, "time_zone", "")
		interval, intervalType := cw.extractInterval(dateHistogram)
		// TODO  GetDateTimeTypeFromExpr can be moved and it should take cw.Schema as an argument

		if dateTimeType == clickhouse.Invalid {
			logger.WarnWithCtx(cw.Ctx).Msgf("invalid date time type for field %s", field)
		}

		dateHistogramAggr := bucket_aggregations.NewDateHistogram(
			cw.Ctx, field, interval, timezone, minDocCount, ebMin, ebMax, intervalType, dateTimeType)
		aggregation.queryType = dateHistogramAggr

		sqlQuery := dateHistogramAggr.GenerateSQL()
		aggregation.selectedColumns = append(aggregation.selectedColumns, sqlQuery)
		aggregation.orderBy = append(aggregation.orderBy, model.NewOrderByExprWithoutOrder(sqlQuery))

		delete(queryMap, "date_histogram")
		return nil
	}
	if autoDateHistogram := cw.parseAutoDateHistogram(queryMap["auto_date_histogram"]); autoDateHistogram != nil {
		aggregation.queryType = autoDateHistogram
		delete(queryMap, "auto_date_histogram")
		return nil
	}
	for _, termsType := range []string{"terms", "significant_terms"} {
		termsRaw, ok := queryMap[termsType]
		if !ok {
			continue
		}
		terms, ok := termsRaw.(QueryMap)
		if !ok {
			return fmt.Errorf("%s is not a map, but %T, value: %v", termsType, termsRaw, termsRaw)
		}

		fieldExpression := cw.parseFieldField(terms, termsType)
		fieldExpression, didWeAddMissing := cw.addMissingParameterIfPresent(fieldExpression, terms)
		if !didWeAddMissing {
			aggregation.filterOutEmptyKeyBucket = true
		}

		const defaultSize = 10
		size := cw.parseSize(terms, defaultSize)
		orderBy := cw.parseOrder(terms, queryMap, []model.Expr{fieldExpression})
		aggregation.queryType = bucket_aggregations.NewTerms(cw.Ctx, termsType == "significant_terms", orderBy[0]) // TODO probably full, not [0]
		aggregation.selectedColumns = append(aggregation.selectedColumns, fieldExpression)
		aggregation.limit = size
		aggregation.orderBy = orderBy

		delete(queryMap, termsType)
		return nil
	}
	if multiTermsRaw, exists := queryMap["multi_terms"]; exists {
		multiTerms, ok := multiTermsRaw.(QueryMap)
		if !ok {
			return fmt.Errorf("multi_terms is not a map, but %T, value: %v", multiTermsRaw, multiTermsRaw)
		}

		const defaultSize = 10
		size := cw.parseIntField(multiTerms, "size", defaultSize)

		aggregation.limit = size

		var fieldsNr int
		if termsRaw, exists := multiTerms["terms"]; exists {
			terms, ok := termsRaw.([]any)
			if !ok {
				return fmt.Errorf("terms is not an array, but %T, value: %v. Using empty array", termsRaw, termsRaw)
			}
			fieldsNr = len(terms)
			columns := make([]model.Expr, 0, fieldsNr)
			for _, term := range terms {
				columns = append(columns, cw.parseFieldField(term, "multi_terms"))
			}
			aggregation.selectedColumns = append(aggregation.selectedColumns, columns...)
			aggregation.orderBy = append(aggregation.orderBy, cw.parseOrder(multiTerms, queryMap, columns)...)
		} else {
			return fmt.Errorf("no terms in multi_terms")
		}

		aggregation.queryType = bucket_aggregations.NewMultiTerms(cw.Ctx, fieldsNr)
		aggregation.limit = size

		delete(queryMap, "multi_terms")
		return nil
	}
	if rangeRaw, ok := queryMap["range"]; ok {
		rangeMap, ok := rangeRaw.(QueryMap)
		if !ok {
			return fmt.Errorf("range is not a map, but %T, value: %v. Using empty map", rangeRaw, rangeRaw)
		}

		Range := cw.parseRangeAggregation(rangeMap)
		aggregation.queryType = Range
		if Range.Keyed {
			aggregation.isKeyed = true
		}
		delete(queryMap, "range")
		return nil
	}
	if dateRangeRaw, ok := queryMap["date_range"]; ok {
		dateRange, ok := dateRangeRaw.(QueryMap)
		if !ok {
			return fmt.Errorf("date_range is not a map, but %T, value: %v. Using empty map", dateRangeRaw, dateRangeRaw)
		}
		if dateRangeParsed, err := cw.parseDateRangeAggregation(dateRange); err == nil {
			aggregation.queryType = dateRangeParsed
		} else {
			return err
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

		delete(queryMap, "date_range")
		return nil
	}
	if geoTileGridRaw, ok := queryMap["geotile_grid"]; ok {
		geoTileGrid, ok := geoTileGridRaw.(QueryMap)
		if !ok {
			return fmt.Errorf("geotile_grid is not a map, but %T, value: %v", geoTileGridRaw, geoTileGridRaw)
		}
		var precisionZoom float64
		precisionRaw, ok := geoTileGrid["precision"]
		if ok {
			if cutValueTyped, ok := precisionRaw.(float64); ok {
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
			return err
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
		return nil
	}
	if sampler, ok := queryMap["sampler"]; ok {
		aggregation.queryType = cw.parseSampler(sampler)
		delete(queryMap, "sampler")
		return nil
	}
	if randomSampler, ok := queryMap["random_sampler"]; ok {
		aggregation.queryType = cw.parseRandomSampler(randomSampler)
		delete(queryMap, "random_sampler")
		return
	}
	if filterAggregation, ok := cw.parseFilters(queryMap); ok {
		sort.Slice(filterAggregation.Filters, func(i, j int) bool { // stable order is required for tests and caching
			return filterAggregation.Filters[i].Name < filterAggregation.Filters[j].Name
		})
		aggregation.isKeyed = true
		aggregation.queryType = filterAggregation
		delete(queryMap, "filters")
		return
	}
	if composite, ok := queryMap["composite"]; ok {
		aggregation.queryType = cw.parseComposite(composite)
		delete(queryMap, "composite")
		return
	}
	success = false
	return
}

// samplerRaw - in a proper request should be of QueryMap type.
func (cw *ClickhouseQueryTranslator) parseSampler(samplerRaw any) (bucket_aggregations.Sampler, error) {
	const defaultSize = 100
	if sampler, ok := samplerRaw.(QueryMap); ok {
		return bucket_aggregations.NewSampler(cw.Ctx, cw.parseIntField(sampler, "shard_size", defaultSize)), nil
	}

	return bucket_aggregations.NewSampler(cw.Ctx, defaultSize), fmt.Errorf("sampler is not a map, but %T, value: %v", samplerRaw, samplerRaw)
}

// randomSamplerRaw - in a proper request should be of QueryMap type.
func (cw *ClickhouseQueryTranslator) parseRandomSampler(randomSamplerRaw any) (bucket_aggregations.RandomSampler, error) {
	const defaultProbability = 0.0 // theoretically it's required
	const defaultSeed = 0
	if randomSampler, ok := randomSamplerRaw.(QueryMap); ok {
		return bucket_aggregations.NewRandomSampler(cw.Ctx,
			cw.parseFloatField(randomSampler, "probability", defaultProbability),
			cw.parseIntField(randomSampler, "seed", defaultSeed),
		), nil
	}

	return bucket_aggregations.NewRandomSampler(cw.Ctx, defaultProbability, defaultSeed), fmt.Errorf("sampler is not a map, but %T, value: %v", randomSamplerRaw, randomSamplerRaw)
}

func (cw *ClickhouseQueryTranslator) parseAutoDateHistogram(paramsRaw any) (*bucket_aggregations.AutoDateHistogram, error) {
	params, ok := paramsRaw.(QueryMap)
	if !ok {
		return nil, fmt.Errorf("auto_date_histogram is not a map, but %T, value: %v", paramsRaw, paramsRaw)
	}

	fieldRaw := cw.parseFieldField(params, "auto_date_histogram")
	if field, ok := fieldRaw.(model.ColumnRef); ok {
		bucketsNr := cw.parseIntField(params, "buckets", 10)
		return bucket_aggregations.NewAutoDateHistogram(cw.Ctx, field, bucketsNr), nil
	}

	return nil, fmt.Errorf("field is not a string, but %T, value: %v", fieldRaw, fieldRaw)
}

func (cw *ClickhouseQueryTranslator) parseOrder(terms, queryMap QueryMap, fieldExpressions []model.Expr) []model.OrderByExpr {
	defaultDirection := model.DescOrder
	defaultOrderBy := model.NewOrderByExpr(model.NewCountFunc(), defaultDirection)

	ordersRaw, exists := terms["order"]
	if !exists {
		return []model.OrderByExpr{defaultOrderBy}
	}

	// order can be either a single order {}, or a list of such single orders [{}(,{}...)]
	orders := make([]QueryMap, 0)
	switch ordersTyped := ordersRaw.(type) {
	case QueryMap:
		orders = append(orders, ordersTyped)
	case []any:
		for _, order := range ordersTyped {
			if orderTyped, ok := order.(QueryMap); ok {
				orders = append(orders, orderTyped)
			} else {
				logger.WarnWithCtx(cw.Ctx).Msgf("invalid order: %v", order)
			}
		}
	default:
		logger.WarnWithCtx(cw.Ctx).Msgf("order is not a map/list of maps, but %T, value: %v. Using default order", ordersRaw, ordersRaw)
		return []model.OrderByExpr{defaultOrderBy}
	}

	fullOrderBy := make([]model.OrderByExpr, 0)

	for _, order := range orders {
		if len(order) != 1 {
			logger.WarnWithCtx(cw.Ctx).Msgf("invalid order length, should be 1: %v", order)
		}
		for key, valueRaw := range order { // value == "asc" or "desc"
			value, ok := valueRaw.(string)
			if !ok {
				logger.WarnWithCtx(cw.Ctx).Msgf("order value is not a string, but %T, value: %v. Using default (desc)", valueRaw, valueRaw)
				value = "desc"
			}

			direction := defaultDirection
			if strings.ToLower(value) == "asc" {
				direction = model.AscOrder
			}

			if key == "_key" {
				for _, fieldExpression := range fieldExpressions {
					fullOrderBy = append(fullOrderBy, model.OrderByExpr{Expr: fieldExpression, Direction: direction})
				}
			} else if key == "_count" {
				fullOrderBy = append(fullOrderBy, model.NewOrderByExpr(model.NewCountFunc(), direction))
			} else {
				fullOrderBy = append(fullOrderBy, model.OrderByExpr{Expr: model.NewLiteral(key), Direction: direction})
			}
		}
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
