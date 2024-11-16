// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package queryparser

import (
	"fmt"
	"github.com/pkg/errors"
	"quesma/clickhouse"
	"quesma/kibana"
	"quesma/logger"
	"quesma/model"
	"quesma/model/bucket_aggregations"
	"strconv"
	"strings"
)

func (cw *ClickhouseQueryTranslator) pancakeTryBucketAggregation(aggregation *pancakeAggregationTreeNode, queryMap QueryMap) error {
	aggregationHandlers := []struct {
		name    string
		handler func(*pancakeAggregationTreeNode, any) error
	}{
		{"histogram", cw.parseHistogram},
		{"date_histogram", cw.parseDateHistogram},
		{"terms", func(node *pancakeAggregationTreeNode, params any) error {
			return cw.parseTermsAggregation(node, params, "terms", queryMap)
		}},
		{"filters", cw.parseFilters},
		{"sampler", cw.parseSampler},
		{"random_sampler", cw.parseRandomSampler},
		{"date_range", cw.parseDateRangeAggregation},
		{"range", cw.parseRangeAggregation},
		{"auto_date_histogram", cw.parseAutoDateHistogram},
		{"geotile_grid", cw.parseGeotileGrid},
		{"significant_terms", func(node *pancakeAggregationTreeNode, params any) error {
			return cw.parseTermsAggregation(node, params, "significant_terms", queryMap)
		}},
		{"multi_terms", func(node *pancakeAggregationTreeNode, params any) error {
			return cw.parseMultiTerms(node, params, queryMap)
		}},
	}

	for _, aggr := range aggregationHandlers {
		if params, ok := queryMap[aggr.name]; ok {
			delete(queryMap, aggr.name)
			return aggr.handler(aggregation, params)
		}
	}

	return nil
}

// paramsRaw - in a proper request should be of QueryMap type.
func (cw *ClickhouseQueryTranslator) parseHistogram(aggregation *pancakeAggregationTreeNode, paramsRaw any) (err error) {
	params, ok := paramsRaw.(QueryMap)
	if !ok {
		return fmt.Errorf("histogram is not a map, but %T, value: %v", paramsRaw, paramsRaw)
	}

	var interval float64
	intervalRaw, ok := params["interval"]
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
		logger.WarnWithCtx(cw.Ctx).Msgf("unexpected type of interval: %T, value: %v. Will use 1.0.", intervalTyped, intervalTyped)
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
func (cw *ClickhouseQueryTranslator) parseDateHistogram(aggregation *pancakeAggregationTreeNode, paramsRaw any) (err error) {
	params, ok := paramsRaw.(QueryMap)
	if !ok {
		return fmt.Errorf("date_histogram is not a map, but %T, value: %v", paramsRaw, paramsRaw)
	}

	field := cw.parseFieldField(params, "date_histogram")
	dateTimeType := cw.Table.GetDateTimeTypeFromExpr(cw.Ctx, field)

	weAddedMissing := false
	if missingRaw, exists := params["missing"]; exists {
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
func (cw *ClickhouseQueryTranslator) parseTermsAggregation(aggregation *pancakeAggregationTreeNode, paramsRaw any, aggrName string, queryMap QueryMap) error {
	params, ok := paramsRaw.(QueryMap)
	if !ok {
		return fmt.Errorf("%s is not a map, but %T, value: %v", aggrName, paramsRaw, paramsRaw)
	}

	fieldExpression := cw.parseFieldField(params, aggrName)
	fieldExpression, didWeAddMissing := cw.addMissingParameterIfPresent(fieldExpression, params)
	if !didWeAddMissing {
		aggregation.filterOutEmptyKeyBucket = true
	}

	const defaultSize = 10
	size := cw.parseSize(params, defaultSize)
	orderBy := cw.parseOrder(params, queryMap, []model.Expr{fieldExpression})

	aggregation.queryType = bucket_aggregations.NewTerms(cw.Ctx, aggrName == "significant_terms", orderBy[0]) // TODO probably full, not [0]
	aggregation.selectedColumns = append(aggregation.selectedColumns, fieldExpression)
	aggregation.limit = size
	aggregation.orderBy = orderBy
	return nil
}

// paramsRaw - in a proper request should be of QueryMap type.
func (cw *ClickhouseQueryTranslator) parseSampler(aggregation *pancakeAggregationTreeNode, paramsRaw any) error {
	const defaultSize = 100
	if params, ok := paramsRaw.(QueryMap); ok {
		aggregation.queryType = bucket_aggregations.NewSampler(cw.Ctx, cw.parseIntField(params, "shard_size", defaultSize))
		return nil
	}
	return fmt.Errorf("sampler is not a map, but %T, value: %v", paramsRaw, paramsRaw)
}

// paramsRaw - in a proper request should be of QueryMap type.
func (cw *ClickhouseQueryTranslator) parseRandomSampler(aggregation *pancakeAggregationTreeNode, paramsRaw any) error {
	const defaultProbability = 0.0 // theoretically it's required
	const defaultSeed = 0
	if params, ok := paramsRaw.(QueryMap); ok {
		aggregation.queryType = bucket_aggregations.NewRandomSampler(cw.Ctx,
			cw.parseFloatField(params, "probability", defaultProbability),
			cw.parseIntField(params, "seed", defaultSeed),
		)
		return nil
	}

	return fmt.Errorf("random_sampler is not a map, but %T, value: %v", paramsRaw, paramsRaw)
}

// paramsRaw - in a proper request should be of QueryMap type.
func (cw *ClickhouseQueryTranslator) parseRangeAggregation(aggregation *pancakeAggregationTreeNode, paramsRaw any) error {
	const keyedDefault = false
	params, ok := paramsRaw.(QueryMap)
	if !ok {
		return fmt.Errorf("range is not a map, but %T, value: %v", paramsRaw, paramsRaw)
	}

	field := cw.parseFieldField(params, "range")
	var ranges []any
	if rangesRaw, ok := params["ranges"]; ok {
		ranges, ok = rangesRaw.([]any)
		if !ok {
			return fmt.Errorf("ranges is not an array, but %T, value: %v", rangesRaw, rangesRaw)
		}
	} else {
		return fmt.Errorf("ranges is not found in range aggregation: %v", params)
	}

	intervals := make([]bucket_aggregations.Interval, 0, len(ranges))
	for _, Range := range ranges {
		rangePartMap := Range.(QueryMap)
		var from, to float64
		if fromRaw, ok := rangePartMap["from"]; ok {
			from, ok = fromRaw.(float64)
			if !ok {
				logger.WarnWithCtx(cw.Ctx).Msgf("from is not a float64: %v, type: %T", fromRaw, fromRaw)
				from = bucket_aggregations.IntervalInfiniteRange
			}
		} else {
			from = bucket_aggregations.IntervalInfiniteRange
		}
		if toRaw, ok := rangePartMap["to"]; ok {
			to, ok = toRaw.(float64)
			if !ok {
				logger.WarnWithCtx(cw.Ctx).Msgf("to is not a float64: %v, type: %T", toRaw, toRaw)
				to = bucket_aggregations.IntervalInfiniteRange
			}
		} else {
			to = bucket_aggregations.IntervalInfiniteRange
		}
		intervals = append(intervals, bucket_aggregations.NewInterval(from, to))
	}

	keyed := keyedDefault
	if keyedRaw, exists := params["keyed"]; exists {
		if keyed, ok = keyedRaw.(bool); !ok {
			logger.WarnWithCtx(cw.Ctx).Msgf("keyed is not a bool, but %T, value: %v", keyedRaw, keyedRaw)
		}
	}

	aggregation.queryType = bucket_aggregations.NewRange(cw.Ctx, field, intervals, keyed)
	aggregation.isKeyed = keyed
	return nil
}

// paramsRaw - in a proper request should be of QueryMap type.
func (cw *ClickhouseQueryTranslator) parseAutoDateHistogram(aggregation *pancakeAggregationTreeNode, paramsRaw any) error {
	params, ok := paramsRaw.(QueryMap)
	if !ok {
		return fmt.Errorf("auto_date_histogram is not a map, but %T, value: %v", paramsRaw, paramsRaw)
	}

	fieldRaw := cw.parseFieldField(params, "auto_date_histogram")
	if field, ok := fieldRaw.(model.ColumnRef); ok {
		bucketsNr := cw.parseIntField(params, "buckets", 10)
		aggregation.queryType = bucket_aggregations.NewAutoDateHistogram(cw.Ctx, field, bucketsNr)
		return nil
	}

	return fmt.Errorf("field is not a string, but %T, value: %v", fieldRaw, fieldRaw)
}

// paramsRaw - in a proper request should be of QueryMap type.
func (cw *ClickhouseQueryTranslator) parseMultiTerms(aggregation *pancakeAggregationTreeNode, paramsRaw any, queryMap QueryMap) error {
	params, ok := paramsRaw.(QueryMap)
	if !ok {
		return fmt.Errorf("multi_terms is not a map, but %T, value: %v", paramsRaw, paramsRaw)
	}

	var fieldsNr int
	if termsRaw, exists := params["terms"]; exists {
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
		aggregation.orderBy = append(aggregation.orderBy, cw.parseOrder(params, queryMap, columns)...)
	} else {
		return fmt.Errorf("no terms in multi_terms")
	}

	const defaultSize = 10
	aggregation.limit = cw.parseSize(params, defaultSize)
	aggregation.queryType = bucket_aggregations.NewMultiTerms(cw.Ctx, fieldsNr)
	return nil
}

// paramsRaw - in a proper request should be of QueryMap type.
func (cw *ClickhouseQueryTranslator) parseGeotileGrid(aggregation *pancakeAggregationTreeNode, paramsRaw any) error {
	params, ok := paramsRaw.(QueryMap)
	if !ok {
		return fmt.Errorf("geotile_grid is not a map, but %T, value: %v", paramsRaw, paramsRaw)
	}
	var precisionZoom float64
	precisionRaw, ok := params["precision"]
	if ok {
		if cutValueTyped, ok := precisionRaw.(float64); ok {
			precisionZoom = cutValueTyped
		}
	}
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
