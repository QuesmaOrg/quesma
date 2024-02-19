package queryparser

import (
	"encoding/json"
	"fmt"
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/model"
	"mitmproxy/quesma/model/bucket_aggregations"
	"mitmproxy/quesma/model/metrics_aggregations"
	"slices"
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
	query.FilterEmptyAggregationFields()
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
	case "sum", "min", "max", "avg", "quantile", "cardinality": // TODO fix cardinality's SQL
		query.NonSchemaFields = append(query.NonSchemaFields, metricsAggr.AggrType+`("`+metricsAggr.FieldNames[0]+`")`)
	case "top_hits", "top_metrics":
		query.Fields = append(query.Fields, metricsAggr.FieldNames...)
		fieldsAsString := strings.Join(metricsAggr.FieldNames, ", ")
		query.TableName = fmt.Sprintf(
			`(SELECT %s, ROW_NUMBER() OVER (PARTITION BY %s) AS %s FROM %s)`,
			fieldsAsString, fieldsAsString, model.RowNumberColumnName, query.TableName,
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
	}
	return query
}

func (cw *ClickhouseQueryTranslator) ParseAggregationJson(queryAsJson string) ([]model.QueryWithAggregation, error) {
	queryAsMap := make(QueryMap)
	err := json.Unmarshal([]byte(queryAsJson), &queryAsMap)
	if err != nil {
		return nil, fmt.Errorf("unmarshal error: %v", err)
	}
	aggregations := make([]model.QueryWithAggregation, 0)
	currentAggr := aggrQueryBuilder{}
	currentAggr.TableName = cw.TableName
	currentAggr.Type = metrics_aggregations.QueryTypeCount{}
	cw.parseAggregation(&currentAggr, queryAsMap["aggs"].(QueryMap), &aggregations)
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
	whereBeforeFilter := "" // to restore it after processing this level
	currentQueryType := currentAggr.Type

	// 1. Metrics aggregation => always leaf
	metricsAggrResult, ok := tryMetricsAggregation(queryMap)
	if ok {
		*resultAccumulator = append(*resultAccumulator, currentAggr.buildMetricsAggregation(metricsAggrResult))
		return
	}

	// 2. Now process filter(s) first, because they apply to everything else on the same level or below.
	if filter, ok := queryMap["filter"]; ok {
		filterOnThisLevel, whereBeforeFilter = true, currentAggr.whereBuilder.Sql.Stmt
		currentAggr.Type = metrics_aggregations.QueryTypeCount{}
		currentAggr.whereBuilder = cw.parseBool(filter.(QueryMap)["bool"].(QueryMap)) // todo change to filter handler
		delete(queryMap, "filter")
	}
	if filters, ok := queryMap["filters"]; ok {
		filterOnThisLevel, whereBeforeFilter = true, currentAggr.whereBuilder.Sql.Stmt
		cw.parseAggregation(currentAggr, filters.(QueryMap)["filters"].(QueryMap), resultAccumulator)
		delete(queryMap, "filters")
	}

	// 3. aggs, terms. They introduce new subaggregations.

	// If 'aggs' is present, we only aggregate if also 'terms' is present. It then introduces a new GROUP BY,
	// and we need counts for that.
	weWantToAggregateHere := true
	termsPresent := false
	if terms, ok := queryMap["terms"]; ok {
		termsPresent = true
		currentAggr.Type = bucket_aggregations.QueryTypeTerms{}
		currentAggr.GroupByFields[len(currentAggr.GroupByFields)-1] = terms.(QueryMap)["field"].(string)
		currentAggr.Fields[len(currentAggr.Fields)-1] = terms.(QueryMap)["field"].(string)
		delete(queryMap, "terms")
	}

	if aggs, ok := queryMap["aggs"]; ok {
		if !termsPresent {
			weWantToAggregateHere = false
		}
		cw.parseAggregation(currentAggr, aggs.(QueryMap), resultAccumulator)
		delete(queryMap, "aggs")
	}

	// 4. Bucket aggregations (https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket.html)
	cw.tryBucketAggregation(currentAggr, queryMap)

	if len(queryMap) == 0 && weWantToAggregateHere {
		*resultAccumulator = append(*resultAccumulator, currentAggr.buildCountAggregation())
	}

	// 5. At the end, we process subaggregations, introduced via (k, v), meaning 'subaggregation_name': { dict }
	for k, v := range queryMap {
		// I assume it's new aggregator name
		logger.Debug().Str(logger.RID, "TODO fill this out").Msgf("Names += %s", k)
		currentAggr.AggregatorsNames = append(currentAggr.AggregatorsNames, k)

		// we have no idea what it is yet, we'll only find out in the level below and set GROUP BY field accordingly
		currentAggr.GroupByFields = append(currentAggr.GroupByFields, "")
		currentAggr.Fields = append(currentAggr.Fields, "")
		cw.parseAggregation(currentAggr, v.(QueryMap), resultAccumulator)
		logger.Debug().Str(logger.RID, "TODO fill this out").Msgf("Names -= %s", k)
		currentAggr.AggregatorsNames = currentAggr.AggregatorsNames[:len(currentAggr.AggregatorsNames)-1]
		currentAggr.Fields = currentAggr.Fields[:len(currentAggr.Fields)-1]
		currentAggr.GroupByFields = currentAggr.GroupByFields[:len(currentAggr.GroupByFields)-1]
	}

	// restore current state, removing subaggregation state
	if filterOnThisLevel {
		currentAggr.whereBuilder.Sql.Stmt = whereBeforeFilter
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

	metricsAggregations := []string{"sum", "avg", "min", "max", "cardinality"}
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

// TODO where clauses should be combined, not just replaced like now
// I treat all clauses as ending (leaf) subAggregation.
// It can be changed, and we'll probably need it, as I see 'date_histogram' case with its subaggregations.
func (cw *ClickhouseQueryTranslator) tryBucketAggregation(currentAggr *aggrQueryBuilder, queryMap QueryMap) {
	if histogram, ok := queryMap["histogram"]; ok {
		currentAggr.GroupByFields[len(currentAggr.GroupByFields)-1] = histogram.(QueryMap)["field"].(string)
		currentAggr.Fields[len(currentAggr.Fields)-1] = histogram.(QueryMap)["field"].(string)
		currentAggr.Type = bucket_aggregations.QueryTypeHistogram{}
		delete(queryMap, "histogram")
	}
	if dateHistogram, ok := queryMap["date_histogram"]; ok {
		currentAggr.GroupByFields[len(currentAggr.GroupByFields)-1] = dateHistogram.(QueryMap)["field"].(string)
		currentAggr.Fields[len(currentAggr.Fields)-1] = dateHistogram.(QueryMap)["field"].(string)
		currentAggr.Type = bucket_aggregations.QueryTypeDateHistogram{}
		delete(queryMap, "date_histogram")
	}
	if Range, ok := queryMap["range"]; ok {
		currentAggr.whereBuilder = cw.parseRange(Range.(QueryMap))
		delete(queryMap, "range")
	}
	if Bool, ok := queryMap["bool"]; ok {
		currentAggr.whereBuilder = cw.parseBool(Bool.(QueryMap))
		delete(queryMap, "bool")
	}
}
