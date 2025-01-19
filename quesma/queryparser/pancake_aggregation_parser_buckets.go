// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package queryparser

import (
	"fmt"
	"github.com/H0llyW00dzZ/cidr"
	"github.com/QuesmaOrg/quesma/quesma/clickhouse"
	"github.com/QuesmaOrg/quesma/quesma/logger"
	"github.com/QuesmaOrg/quesma/quesma/model"
	"github.com/QuesmaOrg/quesma/quesma/model/bucket_aggregations"
	"github.com/QuesmaOrg/quesma/quesma/util"
	cidr2 "github.com/apparentlymart/go-cidr/cidr"
	"github.com/pkg/errors"
	"math"
	"net"
	"net/netip"
	"sort"
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
		{"ip_range", cw.parseIpRange},
		{"ip_prefix", cw.parseIpPrefix},
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

	return nil
}

func (cw *ClickhouseQueryTranslator) parseHistogram(aggregation *pancakeAggregationTreeNode, params QueryMap) (err error) {
	const defaultInterval = 1.0
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

// aggrName - "terms" or "significant_terms"
func (cw *ClickhouseQueryTranslator) parseTermsAggregation(aggregation *pancakeAggregationTreeNode, params QueryMap, aggrName string) error {
	if err := bucket_aggregations.CheckParamsTerms(cw.Ctx, params); err != nil {
		return err
	}

	terms := bucket_aggregations.NewTerms(
		cw.Ctx, aggrName == "significant_terms", params["include"], params["exclude"],
	)

	var didWeAddMissing, didWeUpdateFieldHere bool
	field, isFromScript := cw.parseFieldFieldMaybeScript(params, aggrName)
	fmt.Println(params, isFromScript)
	if !isFromScript {
		fmt.Println(field)
		field, didWeAddMissing = cw.addMissingParameterIfPresent(field, params)
		fmt.Println(field)
		field, didWeUpdateFieldHere = terms.UpdateFieldForIncludeAndExclude(field)
		fmt.Println(field)
	}

	// If we updated above, we change our select to if(condition, field, NULL), so we also need to filter out those NULLs later
	if !didWeAddMissing || didWeUpdateFieldHere {
		aggregation.filterOutEmptyKeyBucket = true
	}

	const defaultSize = 10
	size := cw.parseSize(params, defaultSize)

	orderBy, err := cw.parseOrder(params, []model.Expr{field})
	if err != nil {
		return err
	}

	aggregation.queryType = terms
	aggregation.selectedColumns = append(aggregation.selectedColumns, field)
	aggregation.limit = size
	aggregation.orderBy = orderBy
	return nil
}

func (cw *ClickhouseQueryTranslator) parseFilters(aggregation *pancakeAggregationTreeNode, params QueryMap) error {
	filtersParamRaw, exists := params["filters"]
	if !exists {
		return fmt.Errorf("filters is not a map, but %T, value: %v", params, params)
	}
	filtersParam, ok := filtersParamRaw.(QueryMap)
	if !ok {
		return fmt.Errorf("filters is not a map, but %T, value: %v", filtersParamRaw, filtersParamRaw)
	}

	filters := make([]bucket_aggregations.Filter, 0, len(filtersParam))
	for name, filterRaw := range filtersParam {
		filterMap, ok := filterRaw.(QueryMap)
		if !ok {
			return fmt.Errorf("filter is not a map, but %T, value: %v", filterRaw, filterRaw)
		}
		filter := cw.parseQueryMap(filterMap)
		if filter.WhereClause == nil {
			filter.WhereClause = model.TrueExpr
			filter.CanParse = true
		}
		filters = append(filters, bucket_aggregations.NewFilter(name, filter))
	}

	sort.Slice(filters, func(i, j int) bool {
		return filters[i].Name < filters[j].Name
	})
	aggregation.queryType = bucket_aggregations.NewFilters(cw.Ctx, filters)
	aggregation.isKeyed = true
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
	keyed := cw.parseBoolField(params, "keyed", keyedDefault)
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

func (cw *ClickhouseQueryTranslator) parseIpRange(aggregation *pancakeAggregationTreeNode, params QueryMap) error {
	const defaultKeyed = false

	if err := bucket_aggregations.CheckParamsIpRange(cw.Ctx, params); err != nil {
		return err
	}

	rangesRaw := params["ranges"].([]any)
	ranges := make([]bucket_aggregations.IpInterval, 0, len(rangesRaw))
	for _, rangeRaw := range rangesRaw {
		var begin, end string
		var key *string
		if keyIfPresent, exists := cw.parseStringFieldExistCheck(rangeRaw.(QueryMap), "key"); exists {
			key = &keyIfPresent
		}
		if maskIfExists, exists := cw.parseStringFieldExistCheck(rangeRaw.(QueryMap), "mask"); exists {
			_, ipNet, err := net.ParseCIDR(maskIfExists)
			if err != nil {
				return err
			}
			if ipNet.IP.To4() != nil {
				// it's ipv4
				beginAsInt, endAsInt := cidr.IPv4ToRange(ipNet)
				begin = util.IntToIpv4(beginAsInt)
				// endAsInt is inclusive, we do +1, because we need it exclusive
				if endAsInt != math.MaxUint32 {
					end = util.IntToIpv4(endAsInt + 1)
				} else {
					end = bucket_aggregations.BiggestIpv4 // "255.255.255.255 + 1", so to say (value in compliance with Elastic)
				}
			} else if ipNet.IP.To16() != nil {
				// it's ipv6
				beginInclusive, endInclusive := cidr2.AddressRange(ipNet)
				begin = beginInclusive.String()
				// we do +1 (.Next()), because we need end to be exclusive
				endExclusive := netip.MustParseAddr(endInclusive.String()).Next()
				if endExclusive.IsValid() {
					end = endExclusive.String()
				} else { // invalid means endInclusive was already the biggest possible value (ff...ff)
					end = bucket_aggregations.UnboundedInterval
				}
			} else {
				return fmt.Errorf("invalid mask: %s", maskIfExists)
			}
			if key == nil {
				key = &maskIfExists
			}
		} else {
			begin = cw.parseStringField(rangeRaw.(QueryMap), "from", bucket_aggregations.UnboundedInterval)
			end = cw.parseStringField(rangeRaw.(QueryMap), "to", bucket_aggregations.UnboundedInterval)
		}
		ranges = append(ranges, bucket_aggregations.NewIpInterval(begin, end, key))
	}
	aggregation.isKeyed = cw.parseBoolField(params, "keyed", defaultKeyed)
	aggregation.queryType = bucket_aggregations.NewIpRange(cw.Ctx, ranges, cw.parseFieldField(params, "ip_range"), aggregation.isKeyed)
	return nil
}

func (cw *ClickhouseQueryTranslator) parseIpPrefix(aggregation *pancakeAggregationTreeNode, params QueryMap) error {
	const (
		defaultIsIpv6             = false
		defaultAppendPrefixLength = false
		defaultKeyed              = false
		defaultMinDocCount        = 1
	)

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

func (cw *ClickhouseQueryTranslator) parseMinDocCount(queryMap QueryMap) int {
	const defaultMinDocCount = 0
	if minDocCountRaw, exists := queryMap["min_doc_count"]; exists {
		if minDocCount, ok := minDocCountRaw.(float64); ok {
			asInt := int(minDocCount)
			if asInt != 0 && asInt != 1 {
				logger.WarnWithCtx(cw.Ctx).Msgf("min_doc_count is not 0 or 1, but %d. Not really supported", asInt)
			}
			return asInt
		} else {
			logger.WarnWithCtx(cw.Ctx).Msgf("min_doc_count is not a number, but %T, value: %v. Using default value: %d",
				minDocCountRaw, minDocCountRaw, defaultMinDocCount)
		}
	}
	return defaultMinDocCount
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
