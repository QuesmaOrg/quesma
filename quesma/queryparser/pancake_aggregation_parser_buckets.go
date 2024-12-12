// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package queryparser

import (
	"fmt"
	"github.com/pkg/errors"
	"quesma/clickhouse"
	"quesma/logger"
	"quesma/model"
	"quesma/model/bucket_aggregations"
	"strconv"
	"strings"
)

func (cw *ClickhouseQueryTranslator) pancakeTryBucketAggregation(aggregation *pancakeAggregationTreeNode, queryMap QueryMap) error {
	aggregationHandlers := []struct {
		name    string
		handler func(*pancakeAggregationTreeNode, QueryMap) error
	}{
		{"histogram", cw.parseHistogram},
		{"date_histogram", cw.parseDateHistogram},
		{"terms", func(node *pancakeAggregationTreeNode, params QueryMap) error {
			return cw.parseTermsAggregation(node, params, "terms")
		}},
		{"filters", cw.parseFilters},
		{"sampler", cw.parseSampler},
		{"random_sampler", cw.parseRandomSampler},
		{"date_range", cw.parseDateRangeAggregation},
		{"range", cw.parseRangeAggregation},
		{"auto_date_histogram", cw.parseAutoDateHistogram},
		{"geotile_grid", cw.parseGeotileGrid},
		{"significant_terms", func(node *pancakeAggregationTreeNode, params QueryMap) error {
			return cw.parseTermsAggregation(node, params, "significant_terms")
		}},
		{"multi_terms", cw.parseMultiTerms},
		{"composite", cw.parseComposite},
	}

	for _, aggr := range aggregationHandlers {
		if paramsRaw, ok := queryMap[aggr.name]; ok {
			if params, ok := paramsRaw.(QueryMap); ok {
				delete(queryMap, aggr.name)
				return aggr.handler(aggregation, params)
			}
			return fmt.Errorf("%s is not a map, but %T, value: %v", aggr.name, paramsRaw, paramsRaw)
		}
	}

<<<<<<< Updated upstream
	return nil
}

// paramsRaw - in a proper request should be of QueryMap type.
func (cw *ClickhouseQueryTranslator) parseHistogram(aggregation *pancakeAggregationTreeNode, params QueryMap) (err error) {
	const defaultInterval = 1.0
	var interval float64
	intervalRaw, ok := params["interval"]
=======
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
	if composite, ok := queryMap["composite"]; ok {
		aggregation.queryType, err = cw.parseComposite(aggregation, composite)
		delete(queryMap, "composite")
		return err == nil, err
	}
	if ipPrefix, ok := queryMap["ip_prefix"]; ok {
		delete(queryMap, "ip_prefix")
		if err = cw.parseIpPrefix(aggregation, ipPrefix); err == nil {
			return
		}
	}
	success = false
	return
}

// paramsRaw - in a proper request should be of QueryMap type.
func (cw *ClickhouseQueryTranslator) parseIpPrefix(aggregation *pancakeAggregationTreeNode, paramsRaw any) error {
	const (
		defaultIsIpv6             = false
		defaultAppendPrefixLength = false
		defaultKeyed              = false
		defaultMinDocCount        = 1
	)
	params, ok := paramsRaw.(QueryMap)
	if !ok {
		return fmt.Errorf("ip_prefix is not a map, but %T, value: %v", paramsRaw, paramsRaw)
	}

	if err := bucket_aggregations.CheckParamsIpPrefix(cw.Ctx, params); err != nil {
		return err
	}

	aggr := bucket_aggregations.NewIpPrefix(
		cw.Ctx,
		cw.parseFieldField(params, "ip_prefix"),
		cw.parseIntField(params, "prefix_length", 0), // default doesn't matter, it's required
		cw.parseBoolField(params, "is_ipv6", defaultIsIpv6),
		cw.parseBoolField(params, "append_prefix_length", defaultAppendPrefixLength),
		cw.parseBoolField(params, "keyed", defaultKeyed),
		cw.parseIntField(params, "min_doc_count", defaultMinDocCount),
	)
	if sql := aggr.SqlSelectQuery(); sql != nil {
		aggregation.selectedColumns = append(aggregation.selectedColumns, sql)
		aggregation.orderBy = append(aggregation.orderBy, model.NewOrderByExprWithoutOrder(sql))
	}
	aggregation.queryType = aggr
	fmt.Println(1, 1>>5, 1<<5)
	return nil
}

// samplerRaw - in a proper request should be of QueryMap type.
func (cw *ClickhouseQueryTranslator) parseSampler(samplerRaw any) bucket_aggregations.Sampler {
	const defaultSize = 100
	sampler, ok := samplerRaw.(QueryMap)
>>>>>>> Stashed changes
	if !ok {
		return fmt.Errorf("interval not found in histogram: %v", params)
	}
	switch intervalTyped := intervalRaw.(type) {
	case string:
		interval, err = strconv.ParseFloat(intervalTyped, 64)
		if err != nil {
			return errors.Wrap(err, fmt.Sprintf("failed to parse interval: %v", intervalRaw))
		}
	case int:
		interval = float64(intervalTyped)
	case float64:
		interval = intervalTyped
	default:
		interval = 1.0
		logger.WarnWithCtx(cw.Ctx).Msgf("unexpected type of interval: %T, value: %v. Will use default (%v)", intervalTyped, intervalTyped, defaultInterval)
	}

	minDocCount := cw.parseMinDocCount(params)
	field, _ := cw.parseFieldFieldMaybeScript(params, "histogram")
	field, didWeAddMissing := cw.addMissingParameterIfPresent(field, params)
	if !didWeAddMissing {
		aggregation.filterOutEmptyKeyBucket = true
	}

	if interval != 1.0 {
		// column as string is: fmt.Sprintf("floor(%s / %f) * %f", fieldNameProperlyQuoted, interval, interval)
		field = model.NewInfixExpr(
			model.NewFunction("floor", model.NewInfixExpr(field, "/", model.NewLiteral(interval))),
			"*",
			model.NewLiteral(interval),
		)
	}

	aggregation.queryType = bucket_aggregations.NewHistogram(cw.Ctx, interval, minDocCount)
	aggregation.selectedColumns = append(aggregation.selectedColumns, field)
	aggregation.orderBy = append(aggregation.orderBy, model.NewOrderByExprWithoutOrder(field))
	return nil
}

// paramsRaw - in a proper request should be of QueryMap type.
func (cw *ClickhouseQueryTranslator) parseDateHistogram(aggregation *pancakeAggregationTreeNode, params QueryMap) (err error) {
	field := cw.parseFieldField(params, "date_histogram")
	dateTimeType := cw.Table.GetDateTimeTypeFromExpr(cw.Ctx, field)

	weAddedMissing := false
	if missingRaw, exists := params["missing"]; exists {
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
	if extendedBounds, exists := params["extended_bounds"].(QueryMap); exists {
		ebMin = cw.parseInt64Field(extendedBounds, "min", bucket_aggregations.NoExtendedBound)
		ebMax = cw.parseInt64Field(extendedBounds, "max", bucket_aggregations.NoExtendedBound)
	}

	minDocCount := cw.parseMinDocCount(params)
	timezone := cw.parseStringField(params, "time_zone", "")
	interval, intervalType := cw.extractInterval(params)
	// TODO  GetDateTimeTypeFromExpr can be moved and it should take cw.Schema as an argument

	if dateTimeType == clickhouse.Invalid {
		logger.WarnWithCtx(cw.Ctx).Msgf("invalid date time type for field %s", field)
	}

	dateHistogram := bucket_aggregations.NewDateHistogram(cw.Ctx,
		field, interval, timezone, minDocCount, ebMin, ebMax, intervalType, dateTimeType)
	aggregation.queryType = dateHistogram

	columnSql := dateHistogram.GenerateSQL()
	aggregation.selectedColumns = append(aggregation.selectedColumns, columnSql)
	aggregation.orderBy = append(aggregation.orderBy, model.NewOrderByExprWithoutOrder(columnSql))
	return nil
}

// paramsRaw - in a proper request should be of QueryMap type.
// aggrName - "terms" or "significant_terms"
func (cw *ClickhouseQueryTranslator) parseTermsAggregation(aggregation *pancakeAggregationTreeNode, params QueryMap, aggrName string) error {
	field := cw.parseFieldField(params, aggrName)
	field, didWeAddMissing := cw.addMissingParameterIfPresent(field, params)
	if !didWeAddMissing {
		aggregation.filterOutEmptyKeyBucket = true
	}

	const defaultSize = 10
	size := cw.parseSize(params, defaultSize)
	orderBy, err := cw.parseOrder(params, []model.Expr{field})
	if err != nil {
		return err
	}

	aggregation.queryType = bucket_aggregations.NewTerms(cw.Ctx, aggrName == "significant_terms", orderBy[0]) // TODO probably full, not [0]
	aggregation.selectedColumns = append(aggregation.selectedColumns, field)
	aggregation.limit = size
	aggregation.orderBy = orderBy
	return nil
}

func (cw *ClickhouseQueryTranslator) parseSampler(aggregation *pancakeAggregationTreeNode, params QueryMap) error {
	const defaultSize = 100
	aggregation.queryType = bucket_aggregations.NewSampler(cw.Ctx, cw.parseIntField(params, "shard_size", defaultSize))
	return nil
}

func (cw *ClickhouseQueryTranslator) parseRandomSampler(aggregation *pancakeAggregationTreeNode, params QueryMap) error {
	const defaultProbability = 0.0 // theoretically it's required
	const defaultSeed = 0
	aggregation.queryType = bucket_aggregations.NewRandomSampler(cw.Ctx,
		cw.parseFloatField(params, "probability", defaultProbability),
		cw.parseIntField(params, "seed", defaultSeed),
	)
	return nil
}

func (cw *ClickhouseQueryTranslator) parseRangeAggregation(aggregation *pancakeAggregationTreeNode, params QueryMap) error {
	ranges, err := cw.parseArrayField(params, "ranges")
	if err != nil {
		return err
	}
	intervals := make([]bucket_aggregations.Interval, 0, len(ranges))
	for _, Range := range ranges {
		rangePartMap := Range.(QueryMap)
		from := cw.parseFloatField(rangePartMap, "from", bucket_aggregations.IntervalInfiniteRange)
		to := cw.parseFloatField(rangePartMap, "to", bucket_aggregations.IntervalInfiniteRange)
		intervals = append(intervals, bucket_aggregations.NewInterval(from, to))
	}

	const keyedDefault = false
	keyed := keyedDefault
	if keyedRaw, exists := params["keyed"]; exists {
		var ok bool
		if keyed, ok = keyedRaw.(bool); !ok {
			logger.WarnWithCtx(cw.Ctx).Msgf("keyed is not a bool, but %T, value: %v", keyedRaw, keyedRaw)
		}
	}

	field := cw.parseFieldField(params, "range")
	aggregation.queryType = bucket_aggregations.NewRange(cw.Ctx, field, intervals, keyed)
	aggregation.isKeyed = keyed
	return nil
}

func (cw *ClickhouseQueryTranslator) parseAutoDateHistogram(aggregation *pancakeAggregationTreeNode, params QueryMap) error {
	fieldRaw := cw.parseFieldField(params, "auto_date_histogram")
	if field, ok := fieldRaw.(model.ColumnRef); ok {
		bucketsNr := cw.parseIntField(params, "buckets", 10)
		aggregation.queryType = bucket_aggregations.NewAutoDateHistogram(cw.Ctx, field, bucketsNr)
		return nil
	}

	return fmt.Errorf("error parsing 'field' in auto_date_histogram; field type: %T, value: %v", fieldRaw, fieldRaw)
}

func (cw *ClickhouseQueryTranslator) parseMultiTerms(aggregation *pancakeAggregationTreeNode, params QueryMap) error {
	terms, err := cw.parseArrayField(params, "terms")
	if err != nil {
		return err
	}

	fieldsNr := len(terms)
	columns := make([]model.Expr, 0, fieldsNr)
	for _, term := range terms {
		columns = append(columns, cw.parseFieldField(term, "multi_terms"))
	}

	orderBy, err := cw.parseOrder(params, columns)
	if err != nil {
		return err
	}
	aggregation.orderBy = append(aggregation.orderBy, orderBy...)
	aggregation.selectedColumns = append(aggregation.selectedColumns, columns...)

	const defaultSize = 10
	aggregation.limit = cw.parseSize(params, defaultSize)
	aggregation.queryType = bucket_aggregations.NewMultiTerms(cw.Ctx, fieldsNr)
	return nil
}

func (cw *ClickhouseQueryTranslator) parseGeotileGrid(aggregation *pancakeAggregationTreeNode, params QueryMap) error {
	const defaultPrecisionZoom = 7.0
	precisionZoom := cw.parseFloatField(params, "precision", defaultPrecisionZoom)
	field := cw.parseFieldField(params, "geotile_grid")

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

	aggregation.queryType = bucket_aggregations.NewGeoTileGrid(cw.Ctx)
	aggregation.selectedColumns = append(aggregation.selectedColumns, model.NewLiteral(fmt.Sprintf("CAST(%f AS Float32)", precisionZoom)))
	aggregation.selectedColumns = append(aggregation.selectedColumns, xTile)
	aggregation.selectedColumns = append(aggregation.selectedColumns, yTile)
	return nil
}

// compositeRaw - in a proper request should be of QueryMap type.
// TODO: In geotile_grid, without order specidfied, Elastic returns sort by key (a/b/c earlier than x/y/z if a<x or (a=x && b<y), etc.)
// Maybe add some ordering, but doesn't seem to be very important.
func (cw *ClickhouseQueryTranslator) parseComposite(aggregation *pancakeAggregationTreeNode, params QueryMap) error {
	const defaultSize = 10

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
	sources, err := cw.parseArrayField(params, "sources")
	if err != nil {
		return err
	}
	for _, sourceRaw := range sources {
		source, ok := sourceRaw.(QueryMap)
		if !ok {
			return fmt.Errorf("source is not a map, but %T, value: %v", sourceRaw, sourceRaw)
		}
		if len(source) != 1 {
			return fmt.Errorf("source has unexpected length: %v", source)
		}
		for aggrName, aggrRaw := range source {
			aggr, ok := aggrRaw.(QueryMap)
			if !ok {
				return fmt.Errorf("source value is not a map, but %T, value: %v", aggrRaw, aggrRaw)
			}
			if err = cw.pancakeTryBucketAggregation(aggregation, aggr); err == nil {
				if !isValidSourceType(aggregation.queryType) {
					return fmt.Errorf("unsupported base aggregation type: %v", aggregation.queryType)
				}
				baseAggrs = append(baseAggrs, bucket_aggregations.NewBaseAggregation(aggrName, aggregation.queryType))
			} else {
				return err
			}
		}
	}

	size := cw.parseIntField(params, "size", defaultSize)
	aggregation.limit = size
	aggregation.queryType = bucket_aggregations.NewComposite(cw.Ctx, size, baseAggrs)
	return nil
}

func (cw *ClickhouseQueryTranslator) parseOrder(params QueryMap, fieldExpressions []model.Expr) ([]model.OrderByExpr, error) {
	defaultDirection := model.DescOrder
	defaultOrderBy := model.NewOrderByExpr(model.NewCountFunc(), defaultDirection)

	ordersRaw, exists := params["order"]
	if !exists {
		return []model.OrderByExpr{defaultOrderBy}, nil
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
				return nil, fmt.Errorf("invalid order: %v", order)
			}
		}
	default:
		return nil, fmt.Errorf("order is not a map/list of maps, but %T, value: %v. Using default order", ordersRaw, ordersRaw)
	}

	fullOrderBy := make([]model.OrderByExpr, 0)

	for _, order := range orders {
		if len(order) != 1 {
			logger.WarnWithCtx(cw.Ctx).Msgf("unexpected order length, should be 1: %v", order)
		}
		for key, valueRaw := range order { // value == "asc" or "desc"
			value, ok := valueRaw.(string)
			if !ok {
				return nil, fmt.Errorf("order value is not a string, but %T, value: %v", valueRaw, valueRaw)
			}

			direction := defaultDirection
			if strings.ToLower(value) == "asc" {
				direction = model.AscOrder
			}

			switch key {
			case "_key":
				for _, fieldExpression := range fieldExpressions {
					fullOrderBy = append(fullOrderBy, model.OrderByExpr{Expr: fieldExpression, Direction: direction})
				}
			case "_count":
				fullOrderBy = append(fullOrderBy, model.NewOrderByExpr(model.NewCountFunc(), direction))
			default:
				fullOrderBy = append(fullOrderBy, model.OrderByExpr{Expr: model.NewLiteral(key), Direction: direction})
			}
		}
	}

	return fullOrderBy, nil
}

// addMissingParameterIfPresent parses 'missing' parameter from 'params'.
func (cw *ClickhouseQueryTranslator) addMissingParameterIfPresent(field model.Expr, params QueryMap) (updatedField model.Expr, didWeAddMissing bool) {
	if params["missing"] == nil {
		return field, false
	}

	// Maybe we should check the input type against the schema?
	// Right now we quote if it's a string.
	var value model.LiteralExpr
	switch val := params["missing"].(type) {
	case string:
		value = model.NewLiteral("'" + val + "'")
	default:
		value = model.NewLiteral(val)
	}

	return model.NewFunction("COALESCE", field, value), true
}
