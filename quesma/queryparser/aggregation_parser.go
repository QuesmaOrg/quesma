package queryparser

import (
	"encoding/json"
	"fmt"
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/model"
	"mitmproxy/quesma/model/bucket_aggregations"
	"mitmproxy/quesma/model/metrics_aggregations"
	"slices"
	"strconv"
	"strings"
)

type aggrQueryBuilder struct {
	model.QueryWithAggregation
	whereBuilder SimpleQuery // during building this is used for where clause, not `aggr.Where`
}

type metricsAggregation struct {
	AggrType   string
	FieldNames []string // on these fields we're doing aggregation. Array, because e.g. 'top_hits' can have multiple fields
}

func (b *aggrQueryBuilder) buildAggregationCommon() model.QueryWithAggregation {
	query := b.QueryWithAggregation
	query.WhereClause = b.whereBuilder.Sql.Stmt

	// Need to copy, as we might be proceeding to modify 'b' pointer
	query.CopyAggregationFields(b.QueryWithAggregation)
	if len(query.Fields) > 0 && query.Fields[len(query.Fields)-1] == model.EmptyFieldSelection { // TODO 99% sure it's removed in next PR, let's leave for now
		query.Fields = query.Fields[:len(query.Fields)-1]
	}
	query.RemoveEmptyGroupBy()
	query.TrimKeywordFromFields()
	return query
}

func (b *aggrQueryBuilder) buildCountAggregation() model.QueryWithAggregation {
	query := b.buildAggregationCommon()
	query.NonSchemaFields = append(query.NonSchemaFields, "count()")
	return query
}

func (b *aggrQueryBuilder) buildMetricsAggregation(metricsAggr metricsAggregation) model.QueryWithAggregation {
	query := b.buildAggregationCommon()
	switch metricsAggr.AggrType {
	case "sum", "min", "max", "avg", "quantile":
		query.NonSchemaFields = append(query.NonSchemaFields, metricsAggr.AggrType+`("`+metricsAggr.FieldNames[0]+`")`)
	case "cardinality":
		query.NonSchemaFields = append(query.NonSchemaFields, `COUNT(DISTINCT "`+metricsAggr.FieldNames[0]+`")`)
	case "value_count":
		query.NonSchemaFields = append(query.NonSchemaFields, "count()")
	case "top_hits", "top_metrics":
		query.Fields = append(query.Fields, metricsAggr.FieldNames...)
		fieldsAsString := strings.Join(metricsAggr.FieldNames, ", ")
		query.FromClause = fmt.Sprintf(
			"(SELECT %s, ROW_NUMBER() OVER (PARTITION BY %s) AS %s FROM %s)",
			fieldsAsString, fieldsAsString, model.RowNumberColumnName, query.FromClause,
		)
	default:
		logger.Warn().Msgf("unknown metrics aggregation: %s", metricsAggr.AggrType)
		query.CanParse = false
	}
	switch metricsAggr.AggrType {
	case "sum":
		query.Type = metrics_aggregations.QueryTypeSum{}
	case "min":
		query.Type = metrics_aggregations.QueryTypeMin{}
	case "max":
		query.Type = metrics_aggregations.QueryTypeMax{}
	case "avg":
		query.Type = metrics_aggregations.QueryTypeAvg{}
	case "cardinality":
		query.Type = metrics_aggregations.QueryTypeCardinality{}
	case "quantile":
		query.Type = metrics_aggregations.QueryTypeQuantile{}
	case "top_hits":
		query.Type = metrics_aggregations.QueryTypeTopHits{}
	case "top_metrics":
		query.Type = metrics_aggregations.QueryTypeTopMetrics{}
	case "value_count":
		query.Type = metrics_aggregations.QueryTypeValueCount{}
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
	currentAggr.Type = metrics_aggregations.QueryTypeCount{}
	if queryPart, ok := queryAsMap["query"]; ok {
		currentAggr.whereBuilder = cw.parseQueryMap(queryPart.(QueryMap))
	}

	// COUNT(*) is needed for every request. We should change it and don't duplicate it, as some
	// requests also ask for that themselves, but let's leave it for later.
	aggregations := []model.QueryWithAggregation{currentAggr.buildCountAggregation()}
	if aggs, ok := queryAsMap["aggs"]; ok {
		cw.parseAggregation(&currentAggr, aggs.(QueryMap), &aggregations)
	} else {
		return nil, fmt.Errorf("no aggs")
	}

	if len(aggregations) == 1 { // there were no aggregations, only COUNT(*) we added a few lines above
		return nil, nil
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
	currentQueryType := currentAggr.Type

	// 1. Metrics aggregation => always leaf
	metricsAggrResult, ok := tryMetricsAggregation(queryMap)
	if ok {
		*resultAccumulator = append(*resultAccumulator, currentAggr.buildMetricsAggregation(metricsAggrResult))
		return
	}

	// 2. Now process filter(s) first, because they apply to everything else on the same level or below.
	if filter, ok := queryMap["filter"]; ok {
		filterOnThisLevel = true
		currentAggr.Type = metrics_aggregations.QueryTypeCount{}
		currentAggr.whereBuilder = cw.combineWheres(
			currentAggr.whereBuilder,
			cw.parseBool(filter.(QueryMap)["bool"].(QueryMap)),
		)
		delete(queryMap, "filter")
	}
	if filters, ok := queryMap["filters"]; ok {
		filterOnThisLevel = true
		cw.parseAggregation(currentAggr, filters.(QueryMap)["filters"].(QueryMap), resultAccumulator)
		delete(queryMap, "filters")
	}

	// 3. Bucket aggregations. They introduce new subaggregations, even if no explicit subaggregation defined on this level.
	weWantToAggregateHere := true
	bucketAggrPresent, addedNonSchemaField := cw.tryBucketAggregation(currentAggr, queryMap)

	// If 'aggs' is present, we only aggregate if also 'terms'/'sampler' is present. It then introduces a new GROUP BY,
	// and we need counts for that.
	if aggs, ok := queryMap["aggs"]; ok {
		if !bucketAggrPresent {
			weWantToAggregateHere = false
		}
		cw.parseAggregation(currentAggr, aggs.(QueryMap), resultAccumulator)
		delete(queryMap, "aggs")
	}

	if len(queryMap) == 0 && weWantToAggregateHere {
		*resultAccumulator = append(*resultAccumulator, currentAggr.buildCountAggregation())
	}

	// 5. At the end, we process subaggregations, introduced via (k, v), meaning 'subaggregation_name': { dict }
	for k, v := range queryMap {
		// I assume it's new aggregator name
		logger.Debug().Str(logger.RID, "TODO fill this out").Msgf("Names += %s", k)
		currentAggr.AggregatorsNames = append(currentAggr.AggregatorsNames, k)

		// we have no idea what it is yet, we'll only find out in the level below and set GROUP BY field accordingly
		currentAggr.GroupByFields = append(currentAggr.GroupByFields, model.EmptyFieldSelection)
		currentAggr.Fields = append(currentAggr.Fields, model.EmptyFieldSelection)
		cw.parseAggregation(currentAggr, v.(QueryMap), resultAccumulator)
		logger.Debug().Str(logger.RID, "TODO fill this out").Msgf("Names -= %s", k)
		currentAggr.AggregatorsNames = currentAggr.AggregatorsNames[:len(currentAggr.AggregatorsNames)-1]
		currentAggr.Fields = currentAggr.Fields[:len(currentAggr.Fields)-1]
		currentAggr.GroupByFields = currentAggr.GroupByFields[:len(currentAggr.GroupByFields)-1]
	}

	// restore current state, removing subaggregation state
	if filterOnThisLevel {
		currentAggr.whereBuilder = whereBeforeNesting
	}
	if addedNonSchemaField {
		currentAggr.NonSchemaFields = currentAggr.NonSchemaFields[:len(currentAggr.NonSchemaFields)-1]
	}
	currentAggr.Type = currentQueryType
}

// Tries to parse metrics aggregation from queryMap. If it's not a metrics aggregation, returns false.
func tryMetricsAggregation(queryMap QueryMap) (metricsAggregation, bool) {
	if len(queryMap) != 1 {
		return metricsAggregation{}, false
	}

	// full list: https://www.elastic.co/guide/en/elasticsearch/reference/current/search-Aggregations-metrics.html
	// shouldn't be hard to handle others, if necessary

	metricsAggregations := []string{"sum", "avg", "min", "max", "cardinality", "value_count"}
	for k, v := range queryMap {
		if slices.Contains(metricsAggregations, k) {
			return metricsAggregation{
				AggrType:   k,
				FieldNames: []string{v.(QueryMap)["field"].(string)},
			}, true
		}
	}

	if percentile, ok := queryMap["percentiles"]; ok {
		return metricsAggregation{
			AggrType:   "quantile",
			FieldNames: []string{percentile.(QueryMap)["field"].(string)},
		}, true
	}

	if topMetrics, ok := queryMap["top_metrics"]; ok {
		return metricsAggregation{
			AggrType:   "top_metrics",
			FieldNames: []string{topMetrics.(QueryMap)["metrics"].(QueryMap)["field"].(string)},
		}, true
	}
	if topHits, ok := queryMap["top_hits"]; ok {
		fields := topHits.(QueryMap)["_source"].(QueryMap)["includes"].([]interface{})
		fieldsAsStrings := make([]string, len(fields))
		for i, v := range fields {
			fieldsAsStrings[i] = v.(string)
		}
		return metricsAggregation{
			AggrType:   "top_hits",
			FieldNames: fieldsAsStrings,
		}, true
	}

	return metricsAggregation{}, false
}

// tryBucketAggregation checks if 'queryMap' represents a bucket aggregation on current level and if it does, updates 'currentAggr'.
// Returns:
// * 'success': was it bucket aggreggation?
// * 'nonSchemaFieldAdded': did we add a non-schema field to 'currentAggr', if it turned out to be bucket aggregation? If we did, we need to know, to remove it later.
func (cw *ClickhouseQueryTranslator) tryBucketAggregation(currentAggr *aggrQueryBuilder, queryMap QueryMap) (
	success bool, nonSchemaFieldAdded bool) {
	success, nonSchemaFieldAdded = true, false // returned in most cases
	if histogram, ok := queryMap["histogram"]; ok {
		currentAggr.GroupByFields[len(currentAggr.GroupByFields)-1] = strconv.Quote(histogram.(QueryMap)["field"].(string))
		currentAggr.Fields[len(currentAggr.Fields)-1] = histogram.(QueryMap)["field"].(string)
		currentAggr.Type = bucket_aggregations.QueryTypeHistogram{}
		delete(queryMap, "histogram")
		return
	}
	if dateHistogram, ok := queryMap["date_histogram"]; ok {
		histogramPartOfQuery := cw.createHistogramPartOfQuery(dateHistogram.(QueryMap))
		currentAggr.GroupByFields[len(currentAggr.GroupByFields)-1] = histogramPartOfQuery
		currentAggr.NonSchemaFields = append(currentAggr.NonSchemaFields, histogramPartOfQuery)
		currentAggr.Type = bucket_aggregations.QueryTypeDateHistogram{Interval: cw.extractInterval(dateHistogram.(QueryMap))}
		delete(queryMap, "date_histogram")
		nonSchemaFieldAdded = true // only case for that
		return
	}
	if terms, ok := queryMap["terms"]; ok {
		currentAggr.Type = bucket_aggregations.QueryTypeTerms{}
		currentAggr.GroupByFields[len(currentAggr.GroupByFields)-1] = terms.(QueryMap)["field"].(string)
		currentAggr.Fields[len(currentAggr.Fields)-1] = terms.(QueryMap)["field"].(string)
		delete(queryMap, "terms")
		return
	}
	if _, ok := queryMap["sampler"]; ok {
		delete(queryMap, "sampler")
		return
	}
	if Range, ok := queryMap["range"]; ok {
		currentAggr.whereBuilder = cw.combineWheres(
			currentAggr.whereBuilder,
			cw.parseRange(Range.(QueryMap)),
		)
		delete(queryMap, "range")
		return
	}
	if Bool, ok := queryMap["bool"]; ok {
		currentAggr.whereBuilder = cw.combineWheres(
			currentAggr.whereBuilder,
			cw.parseBool(Bool.(QueryMap)),
		)
		delete(queryMap, "bool")
		return
	}
	success = false
	return
}

func (cw *ClickhouseQueryTranslator) combineWheres(where1, where2 SimpleQuery) SimpleQuery {
	combined := SimpleQuery{
		Sql:      and([]Statement{where1.Sql, where2.Sql}),
		CanParse: where1.CanParse && where2.CanParse,
	}
	if len(where1.FieldName) > 0 && len(where2.FieldName) > 0 {
		logger.Warn().Msgf("combining 2 where clauses with non-empty field names: %s, %s, where queries: %v %v", where1.FieldName, where2.FieldName, where1, where2)
	}
	if len(where1.FieldName) > 0 {
		combined.FieldName = where1.FieldName
	} else {
		combined.FieldName = where2.FieldName
	}
	return combined
}
