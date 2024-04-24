package queryparser

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/barkimedes/go-deepcopy"
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/model"
	"mitmproxy/quesma/model/bucket_aggregations"
	"mitmproxy/quesma/model/metrics_aggregations"
	"slices"
	"strconv"
	"strings"
)

type filter struct {
	name string
	sql  SimpleQuery
}

func newFilter(name string, sql SimpleQuery) filter {
	return filter{name: name, sql: sql}
}

type aggrQueryBuilder struct {
	model.QueryWithAggregation
	whereBuilder SimpleQuery // during building this is used for where clause, not `aggr.Where`
	ctx          context.Context
}

type metricsAggregation struct {
	AggrType    string
	FieldNames  []string      // on these fields we're doing aggregation. Array, because e.g. 'top_hits' can have multiple fields
	Percentiles model.JsonMap // Only for percentiles aggregation
	Keyed       bool          // Only for percentiles aggregation
	SortBy      string        // Only for top_metrics
	Size        int           // Only for top_metrics
	Order       string        // Only for top_metrics
}

func (b *aggrQueryBuilder) buildAggregationCommon(metadata model.JsonMap) model.QueryWithAggregation {
	query := b.QueryWithAggregation
	query.WhereClause = b.whereBuilder.Sql.Stmt

	// Need to copy, as we might be proceeding to modify 'b' pointer
	query.CopyAggregationFields(b.QueryWithAggregation)
	if len(query.Fields) > 0 && query.Fields[len(query.Fields)-1] == model.EmptyFieldSelection { // TODO 99% sure it's removed in next PR, let's leave for now
		query.Fields = query.Fields[:len(query.Fields)-1]
	}
	query.RemoveEmptyGroupBy()
	query.TrimKeywordFromFields(b.ctx)
	query.Metadata = metadata
	return query
}

func (b *aggrQueryBuilder) buildCountAggregation(metadata model.JsonMap) model.QueryWithAggregation {
	query := b.buildAggregationCommon(metadata)
	query.Type = metrics_aggregations.NewCount(b.ctx)
	query.NonSchemaFields = append(query.NonSchemaFields, "count()")
	return query
}

func (b *aggrQueryBuilder) buildBucketAggregation(metadata model.JsonMap) model.QueryWithAggregation {
	query := b.buildAggregationCommon(metadata)
	query.NonSchemaFields = append(query.NonSchemaFields, "count()")
	return query
}
func (b *aggrQueryBuilder) buildMetricsAggregation(metricsAggr metricsAggregation, metadata model.JsonMap) model.QueryWithAggregation {
	getFirstFieldName := func() string {
		if len(metricsAggr.FieldNames) > 0 {
			return metricsAggr.FieldNames[0]
		}
		logger.ErrorWithCtx(b.ctx).Msg("No field names in metrics aggregation. Using empty.")
		return ""
	}
	query := b.buildAggregationCommon(metadata)
	switch metricsAggr.AggrType {
	case "sum", "min", "max", "avg":
		query.NonSchemaFields = append(query.NonSchemaFields, metricsAggr.AggrType+`("`+getFirstFieldName()+`")`)
	case "quantile":
		for usersPercent, percentAsFloat := range metricsAggr.Percentiles {
			query.NonSchemaFields = append(query.NonSchemaFields, fmt.Sprintf(
				"quantiles(%6f)(`%s`) AS `quantile_%s`", percentAsFloat, getFirstFieldName(), usersPercent))
		}
	case "cardinality":
		query.NonSchemaFields = append(query.NonSchemaFields, `COUNT(DISTINCT "`+getFirstFieldName()+`")`)
	case "value_count":
		query.NonSchemaFields = append(query.NonSchemaFields, "count()")
	case "stats":
		fieldName := getFirstFieldName()
		query.NonSchemaFields = append(
			query.NonSchemaFields,
			"count(`"+fieldName+"`)",
			"min(`"+fieldName+"`)",
			"max(`"+fieldName+"`)",
			"avg(`"+fieldName+"`)",
			"sum(`"+fieldName+"`)",
		)
	case "top_hits":
		query.Fields = append(query.Fields, metricsAggr.FieldNames...)
		fieldsAsString := strings.Join(metricsAggr.FieldNames, ", ")
		query.FromClause = fmt.Sprintf(
			"(SELECT %s, ROW_NUMBER() OVER (PARTITION BY %s) AS %s FROM %s)",
			fieldsAsString, fieldsAsString, model.RowNumberColumnName, query.FromClause,
		)
	case "top_metrics":
		// This appending of `metricsAggr.SortBy` and having it duplicated in SELECT block
		// is a way to pass value we're sorting by to the query result. In the future we might add SQL aliasing support, e.g. SELECT x AS 'sort_by' FROM ...
		if len(b.QueryWithAggregation.Query.GroupByFields) > 0 {
			var ordFunc string
			switch metricsAggr.Order {
			case "asc":
				ordFunc = `MAX`
			case "desc":
				ordFunc = `MIN`
			}
			var topSelectFields []string
			innerFields := append(metricsAggr.FieldNames, metricsAggr.SortBy)
			for _, field := range innerFields {
				topSelectFields = append(topSelectFields, fmt.Sprintf(`%s("%s") AS "windowed_%s"`, ordFunc, field, field))
			}
			query.NonSchemaFields = append(query.NonSchemaFields, topSelectFields...)
			partitionBy := strings.Join(b.QueryWithAggregation.Query.GroupByFields, "")
			fieldsAsString := strings.Join(quoteArray(innerFields), ", ") // need those fields in the inner clause
			query.FromClause = fmt.Sprintf(
				"(SELECT %s, ROW_NUMBER() OVER (PARTITION BY %s ORDER BY %s %s) AS %s FROM %s WHERE %s)",
				fieldsAsString, partitionBy,
				strconv.Quote(metricsAggr.SortBy), metricsAggr.Order,
				model.RowNumberColumnName, query.FromClause, b.whereBuilder.Sql.Stmt,
			)
			query.WhereClause = query.WhereClause + fmt.Sprintf(" AND %s <= %d", model.RowNumberColumnName, metricsAggr.Size)
		} else {
			query.Fields = append(metricsAggr.FieldNames, metricsAggr.SortBy)
			query.SuffixClauses = append(query.SuffixClauses,
				fmt.Sprintf(`ORDER BY %s %s LIMIT %d`, metricsAggr.SortBy, metricsAggr.Order, metricsAggr.Size))
		}
	case "percentile_ranks":
		for _, cutValueAsString := range metricsAggr.FieldNames[1:] {
			cutValue, _ := strconv.ParseFloat(cutValueAsString, 64)
			Select := fmt.Sprintf("count(if(%s<=%f, 1, NULL))/count(*)*100", strconv.Quote(getFirstFieldName()), cutValue)
			query.NonSchemaFields = append(query.NonSchemaFields, Select)
		}
	default:
		logger.WarnWithCtx(b.ctx).Msgf("unknown metrics aggregation: %s", metricsAggr.AggrType)
		query.CanParse = false
	}
	switch metricsAggr.AggrType {
	case "sum":
		query.Type = metrics_aggregations.NewSum(b.ctx)
	case "min":
		query.Type = metrics_aggregations.NewMin(b.ctx)
	case "max":
		query.Type = metrics_aggregations.NewMax(b.ctx)
	case "avg":
		query.Type = metrics_aggregations.NewAvg(b.ctx)
	case "stats":
		query.Type = metrics_aggregations.NewStats(b.ctx)
	case "cardinality":
		query.Type = metrics_aggregations.NewCardinality(b.ctx)
	case "quantile":
		query.Type = metrics_aggregations.NewQuantile(b.ctx, metricsAggr.Keyed)
	case "top_hits":
		query.Type = metrics_aggregations.NewTopHits(b.ctx)
	case "top_metrics":
		query.Type = metrics_aggregations.NewTopMetrics(b.ctx)
	case "value_count":
		query.Type = metrics_aggregations.NewValueCount(b.ctx)
	case "percentile_ranks":
		query.Type = metrics_aggregations.NewPercentileRanks(b.ctx)
	}
	return query
}

// ParseAggregationJson parses JSON with aggregation query and returns array of queries with aggregations.
// If there are no aggregations, returns nil.
func (cw *ClickhouseQueryTranslator) ParseAggregationJson(queryAsJson string) ([]model.QueryWithAggregation, error) {
	queryAsMap := make(QueryMap)
	err := json.Unmarshal([]byte(queryAsJson), &queryAsMap)
	if err != nil {
		return nil, fmt.Errorf("unmarshal error: %v", err)
	}
	currentAggr := aggrQueryBuilder{}
	currentAggr.FromClause = cw.Table.FullTableName()
	currentAggr.ctx = cw.Ctx
	if queryPartRaw, ok := queryAsMap["query"]; ok {
		if queryPart, ok := queryPartRaw.(QueryMap); ok {
			currentAggr.whereBuilder = cw.parseQueryMap(queryPart)
		} else {
			logger.WarnWithCtx(cw.Ctx).Msgf("query is not a map, but %T, query: %v. Skipping", queryPartRaw, queryPartRaw)
		}
	}

	// COUNT(*) is needed for every request. We should change it and don't duplicate it, as some
	// requests also ask for that themselves, but let's leave it for later.
	aggregations := []model.QueryWithAggregation{currentAggr.buildCountAggregation(model.NoMetadataField)}

	if aggsRaw, ok := queryAsMap["aggs"]; ok {
		aggs, ok := aggsRaw.(QueryMap)
		if !ok {
			logger.WarnWithCtx(cw.Ctx).Msgf("aggs is not a map, but %T, aggs: %v", aggsRaw, aggsRaw)
			return aggregations, nil
		}
		// The 'for' below duplicates the logic of parseAggregation a little bit, but let's refactor that later.
		// Duplication is needed, because one request's most outer aggregator's name is "sampler", which
		// is the same as the name of one bucket aggregation, and parsing algorithm mishandles the aggregator name
		// for bucket aggregation name...
		for aggrName, aggrRaw := range aggs {
			aggr, ok := aggrRaw.(QueryMap)
			if !ok {
				logger.WarnWithCtx(cw.Ctx).Msgf("aggr is not a map, but %T, aggr: %v. Skipping", aggrRaw, aggrRaw)
				continue
			}
			currentAggr.Aggregators = append(currentAggr.Aggregators, model.NewAggregatorEmpty(aggrName))
			cw.parseAggregation(&currentAggr, aggr, &aggregations)
			currentAggr.Aggregators = currentAggr.Aggregators[:len(currentAggr.Aggregators)-1]
		}
	} else {
		return nil, fmt.Errorf("no aggs -> request is not an aggregation query")
	}

	return aggregations, nil
}

// 'resultAccumulator' - array when we store results
// Builds aggregations recursively. Seems to be working on all examples so far,
// even though it's a pretty simple algorithm.
// When making changes, look at the order in which we parse fields, it is very important for correctness.
func (cw *ClickhouseQueryTranslator) parseAggregation(currentAggr *aggrQueryBuilder, queryMap QueryMap, resultAccumulator *[]model.QueryWithAggregation) {
	if len(queryMap) == 0 {
		return
	}

	filterOnThisLevel := false
	whereBeforeNesting := currentAggr.whereBuilder // to restore it after processing this level
	queryTypeBeforeNesting := currentAggr.Type

	// check if metadata's present
	var metadata model.JsonMap
	if metaRaw, exists := queryMap["meta"]; exists {
		metadata = metaRaw.(model.JsonMap)
		delete(queryMap, "meta")
	} else {
		metadata = model.NoMetadataField
	}

	// 1. Metrics aggregation => always leaf
	metricsAggrResult, ok := cw.tryMetricsAggregation(queryMap)
	if ok {
		*resultAccumulator = append(*resultAccumulator, currentAggr.buildMetricsAggregation(metricsAggrResult, metadata))
		return
	}

	// 2. Now process filter(s) first, because they apply to everything else on the same level or below.
	// Also filter introduces count to current level.
	if filterRaw, ok := queryMap["filter"]; ok {
		if filter, ok := filterRaw.(QueryMap); ok {
			filterOnThisLevel = true
			currentAggr.Type = metrics_aggregations.NewCount(cw.Ctx)
			currentAggr.whereBuilder = cw.combineWheres(
				currentAggr.whereBuilder,
				cw.parseQueryMap(filter),
			)
			*resultAccumulator = append(*resultAccumulator, currentAggr.buildCountAggregation(metadata))
		} else {
			logger.WarnWithCtx(cw.Ctx).Msgf("filter is not a map, but %T, value: %v. Skipping", filterRaw, filterRaw)
		}
		delete(queryMap, "filter")
	}

	// 3. Bucket aggregations. They introduce new subaggregations, even if no explicit subaggregation defined on this level.
	bucketAggrPresent, nonSchemaFieldsAddedCount, groupByFieldsAddecCount := cw.tryBucketAggregation(currentAggr, queryMap)
	if nonSchemaFieldsAddedCount > 0 {
		if len(currentAggr.Aggregators) > 0 {
			currentAggr.Aggregators[len(currentAggr.Aggregators)-1].Empty = false
		} else {
			logger.ErrorWithCtx(cw.Ctx).Msgf("nonSchemaFieldsAddedCount > 0, but no aggregators present")
		}
	}

	if filtersRaw, ok := queryMap["filters"]; ok {
		currentAggr.Aggregators[len(currentAggr.Aggregators)-1].Filters = true
		var filters []filter
		if filtersAsMap, ok := filtersRaw.(QueryMap); ok {
			filters = cw.parseFilters(filtersAsMap)
		} else {
			logger.WarnWithCtx(cw.Ctx).Msgf("filters is not a map, but %T, value: %v. Using empty", filtersRaw, filtersRaw)
		}
		for _, filter := range filters {
			currentAggr.Type = bucket_aggregations.NewFilters(cw.Ctx)
			currentAggr.whereBuilder = cw.combineWheres(currentAggr.whereBuilder, filter.sql)
			currentAggr.Aggregators = append(currentAggr.Aggregators, model.NewAggregatorEmpty(filter.name))
			*resultAccumulator = append(*resultAccumulator, currentAggr.buildBucketAggregation(metadata))
			if aggs, ok := queryMap["aggs"].(QueryMap); ok {
				aggsCopy, err := deepcopy.Anything(aggs)
				if err == nil {
					cw.parseAggregation(currentAggr, aggsCopy.(QueryMap), resultAccumulator)
				} else {
					logger.ErrorWithCtx(cw.Ctx).Msgf("deepcopy 'aggs' map error: %v. Skipping current filter: %v, aggs: %v", err, filter, aggs)
				}
			}
			currentAggr.Aggregators = currentAggr.Aggregators[:len(currentAggr.Aggregators)-1]
			currentAggr.whereBuilder = whereBeforeNesting
		}
		delete(queryMap, "filters")
	} else if aggs, ok := queryMap["aggs"]; ok {
		cw.parseAggregation(currentAggr, aggs.(QueryMap), resultAccumulator)
	}
	delete(queryMap, "aggs") // no-op if no "aggs"

	if bucketAggrPresent {
		*resultAccumulator = append(*resultAccumulator, currentAggr.buildBucketAggregation(metadata))
	}

	// 5. At the end, we process subaggregations, introduced via (k, v), meaning 'subaggregation_name': { dict }
	for k, v := range queryMap {
		// I assume it's new aggregator name
		logger.DebugWithCtx(cw.Ctx).Msgf("names += %s", k)
		currentAggr.Aggregators = append(currentAggr.Aggregators, model.NewAggregatorEmpty(k))
		if subAggregation, ok := v.(QueryMap); ok {
			cw.parseAggregation(currentAggr, subAggregation, resultAccumulator)
		} else {
			logger.ErrorWithCtx(cw.Ctx).Msgf("unexpected type of subaggregation: (%v: %v), value type: %T. Skipping", k, v, v)
		}
		logger.DebugWithCtx(cw.Ctx).Msgf("names -= %s", k)
		currentAggr.Aggregators = currentAggr.Aggregators[:len(currentAggr.Aggregators)-1]
	}

	// restore current state, removing subaggregation state
	if filterOnThisLevel {
		currentAggr.whereBuilder = whereBeforeNesting
	}
	if nonSchemaFieldsAddedCount > 0 {
		if len(currentAggr.NonSchemaFields) >= nonSchemaFieldsAddedCount {
			currentAggr.NonSchemaFields = currentAggr.NonSchemaFields[:len(currentAggr.NonSchemaFields)-nonSchemaFieldsAddedCount]
		} else {
			logger.ErrorWithCtx(cw.Ctx).Msgf("nonSchemaFieldsAddedCount > currentAggr.NonSchemaFields length -> should be impossible")
		}
	}
	if groupByFieldsAddecCount > 0 {
		if len(currentAggr.GroupByFields) >= groupByFieldsAddecCount {
			currentAggr.GroupByFields = currentAggr.GroupByFields[:len(currentAggr.GroupByFields)-groupByFieldsAddecCount]
		} else {
			logger.ErrorWithCtx(cw.Ctx).Msgf("groupByFieldsAddecCount > currentAggr.GroupByFields length -> should be impossible")
		}
	}
	currentAggr.Type = queryTypeBeforeNesting
}

// Tries to parse metrics aggregation from queryMap. If it's not a metrics aggregation, returns false.
func (cw *ClickhouseQueryTranslator) tryMetricsAggregation(queryMap QueryMap) (metricAggregation metricsAggregation, success bool) {
	if len(queryMap) != 1 {
		return metricsAggregation{}, false
	}

	// full list: https://www.elastic.co/guide/en/elasticsearch/reference/current/search-Aggregations-metrics.html
	// shouldn't be hard to handle others, if necessary

	metricsAggregations := []string{"sum", "avg", "min", "max", "cardinality", "value_count", "stats"}
	for k, v := range queryMap {
		if slices.Contains(metricsAggregations, k) {
			return metricsAggregation{
				AggrType:   k,
				FieldNames: []string{cw.parseFieldField(v, k)},
			}, true
		}
	}

	if percentile, ok := queryMap["percentiles"]; ok {
		percentileMap, ok := percentile.(QueryMap)
		if !ok {
			logger.WarnWithCtx(cw.Ctx).Msgf("percentiles is not a map, but %T, value: %v. Using empty map.", percentile, percentile)
		}
		fieldName, keyed, percentiles := cw.parsePercentilesAggregation(percentileMap)
		return metricsAggregation{
			AggrType:    "quantile",
			FieldNames:  []string{fieldName},
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
	if topHits, ok := queryMap["top_hits"]; ok {
		var fields []any
		fields, ok = topHits.(QueryMap)["_source"].(QueryMap)["includes"].([]any)
		if !ok {
			logger.WarnWithCtx(cw.Ctx).Msgf("can't parse top_hits' fields. top_hits type: %T, value: %v. Using empty fields.", topHits, topHits)
		}
		fieldsAsStrings := make([]string, 0, len(fields))
		for i, v := range fields {
			if vAsString, ok := v.(string); ok {
				fieldsAsStrings = append(fieldsAsStrings, vAsString)
			} else {
				logger.WarnWithCtx(cw.Ctx).Msgf("field %d in top_hits is not a string. Field's type: %T, value: %v. Skipping.", i, v, v)
			}
		}
		return metricsAggregation{
			AggrType:   "top_hits",
			FieldNames: fieldsAsStrings,
		}, true
	}

	// Shortcut here. Percentile_ranks has "field" and a list of "values"
	// I'm keeping all of them in `fieldNames' array for "simplicity".
	if percentileRanks, ok := queryMap["percentile_ranks"]; ok {
		fieldNames := []string{cw.parseFieldField(percentileRanks, "percentile_ranks")}
		var cutValues []any
		if values, exists := percentileRanks.(QueryMap)["values"]; exists {
			cutValues, ok = values.([]any)
			if !ok {
				logger.WarnWithCtx(cw.Ctx).Msgf("values in percentile_ranks is not an array, but %T, value: %v. Using empty array.", values, values)
			}
		} else {
			logger.WarnWithCtx(cw.Ctx).Msg("no values in percentile_ranks. Using empty array.")
		}
		for _, cutValue := range cutValues {
			switch cutValueTyped := cutValue.(type) {
			case float64:
				fieldNames = append(fieldNames, strconv.FormatFloat(cutValueTyped, 'f', -1, 64))
			case int64:
				fieldNames = append(fieldNames, strconv.FormatInt(cutValueTyped, 10))
			default:
				logger.WarnWithCtx(cw.Ctx).Msgf("cutValue in percentile_ranks is not a number, but %T, value: %v. Skipping.", cutValue, cutValue)
			}
		}
		return metricsAggregation{
			AggrType:   "percentile_ranks",
			FieldNames: fieldNames,
		}, true
	}

	return metricsAggregation{}, false
}

// tryBucketAggregation checks if 'queryMap' represents a bucket aggregation on current level and if it does, updates 'currentAggr'.
// Returns:
// * 'success': was it bucket aggreggation?
// * 'nonSchemaFieldAdded': did we add a non-schema field to 'currentAggr', if it turned out to be bucket aggregation? If we did, we need to know, to remove it later.
func (cw *ClickhouseQueryTranslator) tryBucketAggregation(currentAggr *aggrQueryBuilder, queryMap QueryMap) (
	success bool, nonSchemaFieldsAddedCount, groupByFieldsAddedCount int) {

	success = true // returned in most cases
	if histogram, ok := queryMap["histogram"]; ok {
		currentAggr.Type = bucket_aggregations.NewHistogram(cw.Ctx)
		fieldName := strconv.Quote(cw.parseFieldField(histogram, "histogram"))
		var interval int
		intervalQueryMap, ok := histogram.(QueryMap)["interval"]
		if !ok {
			logger.WarnWithCtx(cw.Ctx).Msgf("interval not found in histogram: %v", histogram)
		}
		switch intervalRaw := intervalQueryMap.(type) {
		case string:
			var err error
			interval, err = strconv.Atoi(intervalRaw)
			if err != nil {
				logger.ErrorWithCtx(cw.Ctx).Err(err).Msgf("failed to parse interval: %v", intervalRaw)
			}
		case int:
			interval = intervalRaw
		case float64:
			interval = int(intervalRaw)
		default:
			logger.ErrorWithCtx(cw.Ctx).Msgf("unexpected type of interval: %T, value: %v", intervalRaw, intervalRaw)
		}
		groupByStr := fieldName
		if interval != 1 {
			groupByStr = fmt.Sprintf("floor(%s / %d) * %d AS %s", fieldName, interval, interval, fieldName)
		}
		currentAggr.GroupByFields = append(currentAggr.GroupByFields, groupByStr)
		currentAggr.NonSchemaFields = append(currentAggr.NonSchemaFields, fieldName)
		delete(queryMap, "histogram")
		return success, 1, 1
	}
	if dateHistogramRaw, ok := queryMap["date_histogram"]; ok {
		dateHistogram, ok := dateHistogramRaw.(QueryMap)
		if !ok {
			logger.WarnWithCtx(cw.Ctx).Msgf("date_histogram is not a map, but %T, value: %v", dateHistogramRaw, dateHistogramRaw)
		}
		currentAggr.Type = bucket_aggregations.NewDateHistogram(cw.Ctx, cw.extractInterval(dateHistogram))
		histogramPartOfQuery := cw.createHistogramPartOfQuery(dateHistogram)
		currentAggr.GroupByFields = append(currentAggr.GroupByFields, histogramPartOfQuery)
		currentAggr.NonSchemaFields = append(currentAggr.NonSchemaFields, histogramPartOfQuery)
		delete(queryMap, "date_histogram")
		return success, 1, 1
	}
	for _, termsType := range []string{"terms", "significant_terms"} {
		if terms, ok := queryMap[termsType]; ok {
			currentAggr.Type = bucket_aggregations.NewTerms(cw.Ctx, termsType == "significant_terms")
			fieldName := strconv.Quote(cw.parseFieldField(terms, termsType))
			currentAggr.GroupByFields = append(currentAggr.GroupByFields, fieldName)
			currentAggr.NonSchemaFields = append(currentAggr.NonSchemaFields, fieldName)
			delete(queryMap, termsType)
			return success, 1, 1
		}
	}
	if rangeRaw, ok := queryMap["range"]; ok {
		Range, ok := rangeRaw.(QueryMap)
		if !ok {
			logger.WarnWithCtx(cw.Ctx).Msgf("range is not a map, but %T, value: %v. Using empty map", rangeRaw, rangeRaw)
		}
		rangeParsed := cw.parseRangeAggregation(Range)
		currentAggr.Type = rangeParsed
		if rangeParsed.Keyed {
			currentAggr.Aggregators[len(currentAggr.Aggregators)-1].Keyed = true
		}
		for _, interval := range rangeParsed.Intervals {
			currentAggr.NonSchemaFields = append(
				currentAggr.NonSchemaFields,
				interval.ToSQLSelectQuery(rangeParsed.QuotedFieldName),
			)
		}
		delete(queryMap, "range")
		return success, len(rangeParsed.Intervals), 0
	}
	if dateRangeRaw, ok := queryMap["date_range"]; ok {
		dateRange, ok := dateRangeRaw.(QueryMap)
		if !ok {
			logger.WarnWithCtx(cw.Ctx).Msgf("date_range is not a map, but %T, value: %v. Using empty map", dateRangeRaw, dateRangeRaw)
		}
		dateRangeParsed := cw.parseDateRangeAggregation(dateRange)
		currentAggr.Type = dateRangeParsed
		for _, interval := range dateRangeParsed.Intervals {
			currentAggr.NonSchemaFields = append(currentAggr.NonSchemaFields, interval.ToSQLSelectQuery(dateRangeParsed.FieldName))
			if sqlSelect, selectNeeded := interval.BeginTimestampToSQL(); selectNeeded {
				currentAggr.NonSchemaFields = append(currentAggr.NonSchemaFields, sqlSelect)
			}
			if sqlSelect, selectNeeded := interval.EndTimestampToSQL(); selectNeeded {
				currentAggr.NonSchemaFields = append(currentAggr.NonSchemaFields, sqlSelect)
			}
		}
		delete(queryMap, "date_range")
		return success, dateRangeParsed.SelectColumnsNr, 0
	}
	if _, ok := queryMap["sampler"]; ok {
		currentAggr.Type = metrics_aggregations.NewCount(cw.Ctx)
		delete(queryMap, "sampler")
		return
	}
	// Let's treat random_sampler just like sampler for now, until we add `LIMIT` logic to sampler.
	// Random sampler doesn't have `size` field, but `probability`, so logic in the final version should be different.
	// So far I've only observed its "probability" field to be 1.0, so it's not really important.
	if _, ok := queryMap["random_sampler"]; ok {
		currentAggr.Type = metrics_aggregations.NewCount(cw.Ctx)
		delete(queryMap, "random_sampler")
		return
	}
	if boolRaw, ok := queryMap["bool"]; ok {
		if Bool, ok := boolRaw.(QueryMap); ok {
			currentAggr.whereBuilder = cw.combineWheres(currentAggr.whereBuilder, cw.parseBool(Bool))
		} else {
			logger.WarnWithCtx(cw.Ctx).Msgf("bool is not a map, but %T, value: %v. Skipping", boolRaw, boolRaw)
		}
		delete(queryMap, "bool")
		return
	}
	success = false
	return
}

func (cw *ClickhouseQueryTranslator) parseRangeAggregation(rangePart QueryMap) bucket_aggregations.Range {
	fieldName := cw.parseFieldField(rangePart, "range")
	var ranges []any
	if rangesRaw, ok := rangePart["ranges"]; ok {
		ranges, ok = rangesRaw.([]any)
		if !ok {
			logger.WarnWithCtx(cw.Ctx).Msgf("ranges is not an array, but %T, value: %v. Using empty array", rangesRaw, rangesRaw)
		}
	} else {
		logger.WarnWithCtx(cw.Ctx).Msg("no ranges in range aggregation. Using empty array.")
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
	if keyedRaw, exists := rangePart["keyed"]; exists {
		if keyed, ok := keyedRaw.(bool); ok {
			return bucket_aggregations.NewRange(cw.Ctx, strconv.Quote(fieldName), intervals, keyed)
		} else {
			logger.WarnWithCtx(cw.Ctx).Msgf("keyed is not a bool, but %T, value: %v", keyedRaw, keyedRaw)
		}
	}
	return bucket_aggregations.NewRangeWithDefaultKeyed(cw.Ctx, strconv.Quote(fieldName), intervals)
}

// parseFieldField returns field 'field' from shouldBeMap, which should be a string. Logs some warnings in case of errors, and returns "" then
func (cw *ClickhouseQueryTranslator) parseFieldField(shouldBeMap any, aggregationType string) string {
	Map, ok := shouldBeMap.(QueryMap)
	if !ok {
		logger.WarnWithCtx(cw.Ctx).Msgf("%s aggregation is not a map, but %T, value: %v", aggregationType, shouldBeMap, shouldBeMap)
		return ""
	}
	if fieldRaw, ok := Map["field"]; ok {
		if field, ok := fieldRaw.(string); ok {
			return cw.Table.ResolveField(cw.Ctx, field)
		} else {
			logger.WarnWithCtx(cw.Ctx).Msgf("field is not a string, but %T, value: %v", fieldRaw, fieldRaw)
		}
	} else {
		logger.WarnWithCtx(cw.Ctx).Msgf("field not found in %s aggregation: %v", aggregationType, Map)
	}
	return ""
}

func (cw *ClickhouseQueryTranslator) parseFilters(filtersMap QueryMap) []filter {
	var filters []filter
	filtersMap = filtersMap["filters"].(QueryMap)
	for name, filter := range filtersMap {
		filters = append(filters, newFilter(name, cw.parseQueryMap(filter.(QueryMap))))
	}
	return filters
}

func (cw *ClickhouseQueryTranslator) combineWheres(where1, where2 SimpleQuery) SimpleQuery {
	combined := SimpleQuery{
		Sql:      and([]Statement{where1.Sql, where2.Sql}),
		CanParse: where1.CanParse && where2.CanParse,
	}
	if len(where1.FieldName) > 0 && len(where2.FieldName) > 0 && where1.FieldName != where2.FieldName {
		logger.WarnWithCtx(cw.Ctx).Msgf("combining 2 where clauses with different field names: %s, %s, where queries: %v %v", where1.FieldName, where2.FieldName, where1, where2)
	}
	if len(where1.FieldName) > 0 {
		combined.FieldName = where1.FieldName
	} else {
		combined.FieldName = where2.FieldName
	}
	return combined
}

// quoteArray returns a new array with the same elements, but quoted
func quoteArray(array []string) []string {
	quotedArray := make([]string, 0, len(array))
	for _, el := range array {
		quotedArray = append(quotedArray, strconv.Quote(el))
	}
	return quotedArray
}
