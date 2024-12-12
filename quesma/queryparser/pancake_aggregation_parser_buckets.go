// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package queryparser

import (
	"fmt"
	"quesma/clickhouse"
	"quesma/logger"
	"quesma/model"
	"quesma/model/bucket_aggregations"
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
		dateTimeType := cw.Table.GetDateTimeTypeFromExpr(cw.Ctx, field)

		weAddedMissing := false
		if missingRaw, exists := dateHistogram["missing"]; exists {
			if missing, ok := missingRaw.(string); ok {
				dateManager := NewDateManager(cw.Ctx)
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
		return success, nil
	}
	if autoDateHistogram := cw.parseAutoDateHistogram(queryMap["auto_date_histogram"]); autoDateHistogram != nil {
		aggregation.queryType = autoDateHistogram
		delete(queryMap, "auto_date_histogram")
		return
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

		const defaultSize = 10
		size := cw.parseSize(terms, defaultSize)
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
		if err = bucket_aggregations.CheckParamsGeotileGrid(cw.Ctx, geoTileGridRaw); err != nil {
			logger.ErrorWithCtx(cw.Ctx).Err(err).Msg("failed to check geotile_grid params")
			return false, err
		}

		const (
			defaultPrecision = 7
			defaultSize      = 10000
		)
		geoTileGrid := geoTileGridRaw.(QueryMap)
		precisionZoom := int(cw.parseFloatField(geoTileGrid, "precision", defaultPrecision))
		field := cw.parseFieldField(geoTileGrid, "geotile_grid")
		size := cw.parseIntField(geoTileGrid, "size", defaultSize)
		aggregation.queryType = bucket_aggregations.NewGeoTileGrid(cw.Ctx, precisionZoom)

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

		aggregation.selectedColumns = append(aggregation.selectedColumns, xTile)
		aggregation.selectedColumns = append(aggregation.selectedColumns, yTile)
		// It's not explicitly stated in the Elastic documentation, but Geotile Grid is always ordered by count desc
		aggregation.orderBy = append(aggregation.orderBy, model.NewOrderByExpr(model.NewCountFunc(), model.DescOrder))
		fmt.Println("AGGREGATION ORDER BY", aggregation.orderBy)
		aggregation.limit = size

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
	if composite, ok := queryMap["composite"]; ok {
		aggregation.queryType, err = cw.parseComposite(aggregation, composite)
		delete(queryMap, "composite")
		return err == nil, err
	}
	success = false
	return
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

func (cw *ClickhouseQueryTranslator) parseAutoDateHistogram(paramsRaw any) *bucket_aggregations.AutoDateHistogram {
	params, ok := paramsRaw.(QueryMap)
	if !ok {
		return nil
	}

	fieldRaw := cw.parseFieldField(params, "auto_date_histogram")
	var field model.ColumnRef
	if field, ok = fieldRaw.(model.ColumnRef); !ok {
		logger.WarnWithCtx(cw.Ctx).Msgf("field is not a string, but %T, value: %v. Skipping auto_date_histogram", fieldRaw, fieldRaw)
		return nil
	}
	bucketsNr := cw.parseIntField(params, "buckets", 10)
	return bucket_aggregations.NewAutoDateHistogram(cw.Ctx, field, bucketsNr)
}

// compositeRaw - in a proper request should be of QueryMap type.
// TODO: In geotile_grid, without order specidfied, Elastic returns sort by key (a/b/c earlier than x/y/z if a<x or (a=x && b<y), etc.)
// Maybe add some ordering, but doesn't seem to be very important.
func (cw *ClickhouseQueryTranslator) parseComposite(currentAggrNode *pancakeAggregationTreeNode, compositeRaw any) (*bucket_aggregations.Composite, error) {
	const defaultSize = 10
	composite, ok := compositeRaw.(QueryMap)
	if !ok {
		return nil, fmt.Errorf("composite is not a map, but %T, value: %v", compositeRaw, compositeRaw)
	}

	// The sources parameter can be any of the following types:
	// 1) Terms (but NOT Significant Terms) 2) Histogram 3) Date histogram 4) GeoTile grid
	// https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-composite-aggregation.html
	isValidSourceType := func(queryType model.QueryType) bool {
		switch typed := queryType.(type) {
		case *bucket_aggregations.Histogram, *bucket_aggregations.DateHistogram, bucket_aggregations.GeoTileGrid:
			return true
		case bucket_aggregations.Terms:
			return !typed.IsSignificant()
		default:
			return false
		}
	}

	var baseAggrs []*bucket_aggregations.BaseAggregation
	sourcesRaw, exists := composite["sources"]
	if !exists {
		return nil, fmt.Errorf("composite has no sources")
	}
	sources, ok := sourcesRaw.([]any)
	if !ok {
		return nil, fmt.Errorf("sources is not an array, but %T, value: %v", sourcesRaw, sourcesRaw)
	}
	for _, sourceRaw := range sources {
		source, ok := sourceRaw.(QueryMap)
		if !ok {
			return nil, fmt.Errorf("source is not a map, but %T, value: %v", sourceRaw, sourceRaw)
		}
		if len(source) != 1 {
			return nil, fmt.Errorf("source has unexpected length: %v", source)
		}
		for aggrName, aggrRaw := range source {
			aggr, ok := aggrRaw.(QueryMap)
			if !ok {
				return nil, fmt.Errorf("source value is not a map, but %T, value: %v", aggrRaw, aggrRaw)

			}
			if success, err := cw.pancakeTryBucketAggregation(currentAggrNode, aggr); success {
				if !isValidSourceType(currentAggrNode.queryType) {
					return nil, fmt.Errorf("unsupported base aggregation type: %v", currentAggrNode.queryType)
				}
				baseAggrs = append(baseAggrs, bucket_aggregations.NewBaseAggregation(aggrName, currentAggrNode.queryType))
			} else {
				return nil, err
			}
		}
	}

	size := cw.parseIntField(composite, "size", defaultSize)
	currentAggrNode.limit = size
	return bucket_aggregations.NewComposite(cw.Ctx, size, baseAggrs), nil
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
