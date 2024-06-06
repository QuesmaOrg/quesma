package queryparser

import (
	"context"
	"fmt"
	"mitmproxy/quesma/clickhouse"
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/model"
	"mitmproxy/quesma/model/bucket_aggregations"
	"mitmproxy/quesma/model/metrics_aggregations"
	"mitmproxy/quesma/queryparser/aexp"
	"mitmproxy/quesma/queryparser/where_clause"
	"mitmproxy/quesma/quesma/types"
	"mitmproxy/quesma/util"
	"regexp"
	"slices"
	"strconv"
	"strings"
)

const keyedDefaultValuePercentileRanks = true

type aggrQueryBuilder struct {
	model.Query
	whereBuilder model.SimpleQuery // during building this is used for where clause, not `aggr.Where`
	ctx          context.Context
}

type metricsAggregation struct {
	AggrType            string
	FieldNames          []string                // on these fields we're doing aggregation. Array, because e.g. 'top_hits' can have multiple fields
	FieldType           clickhouse.DateTimeType // field type of FieldNames[0]. If it's a date field, a slightly different response is needed
	Percentiles         map[string]float64      // Only for percentiles aggregation
	Keyed               bool                    // Only for percentiles aggregation
	SortBy              string                  // Only for top_metrics
	Size                int                     // Only for top_metrics
	Order               string                  // Only for top_metrics
	IsFieldNameCompound bool                    // Only for a few aggregations, where we have only 1 field. It's a compound, so e.g. toHour(timestamp), not just "timestamp"
	sigma               float64                 // only for standard deviation
}

func (m metricsAggregation) sortByExists() bool {
	return len(m.SortBy) > 0
}

const metricsAggregationDefaultFieldType = clickhouse.Invalid

/* code from my previous approach to this issue. Let's keep for now, 95% it'll be not needed, I'll remove it then.
func (b *aggrQueryBuilder) applyTermsSubSelect(terms bucket_aggregations.Terms) {
	termsField := b.Query.GroupByFields[len(b.Query.GroupByFields)-1]
	pp.Println(b, terms, termsField, b.Query.String())
	whereLimitStmt := fmt.Sprintf("%s IN (%s)", termsField, b.String())
	fmt.Println("WHERE LIMIT STMT:", whereLimitStmt)
	fmt.Println("where before:", b.whereBuilder.Sql.Stmt)
	b.whereBuilder = combineWheres(b.whereBuilder, newSimpleQuery(NewSimpleStatement(whereLimitStmt), true))
	fmt.Println("where after:", b.whereBuilder.Sql.Stmt)
}
*/

func (b *aggrQueryBuilder) buildAggregationCommon(metadata model.JsonMap) model.Query {
	query := b.Query
	query.WhereClause = b.whereBuilder.WhereClause

	// Need to copy, as we might be proceeding to modify 'b' pointer
	query.CopyAggregationFields(b.Query)

	query.TrimKeywordFromFields()

	query.Metadata = metadata
	return query
}

func (b *aggrQueryBuilder) buildCountAggregation(metadata model.JsonMap) model.Query {
	query := b.buildAggregationCommon(metadata)
	query.Type = metrics_aggregations.NewCount(b.ctx)

	query.Columns = append(query.Columns, model.SelectColumn{Expression: aexp.Count()})
	return query
}

func (b *aggrQueryBuilder) buildBucketAggregation(metadata model.JsonMap) model.Query {
	query := b.buildAggregationCommon(metadata)

	query.Columns = append(query.Columns, model.SelectColumn{Expression: aexp.Count()})
	return query
}
func (b *aggrQueryBuilder) buildMetricsAggregation(metricsAggr metricsAggregation, metadata model.JsonMap) model.Query {
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

		// TODO firstFieldName can be an SQL expression or field name
		if strings.Contains(getFirstFieldName(), "(") {
			query.Columns = append(query.Columns, model.SelectColumn{Expression: aexp.Function(metricsAggr.AggrType+"OrNull", aexp.SQL{Query: getFirstFieldName()})})
		} else {
			query.Columns = append(query.Columns, model.SelectColumn{Expression: aexp.Function(metricsAggr.AggrType+"OrNull", aexp.TableColumn(getFirstFieldName()))})
		}

	case "quantile":
		// Sorting here useful mostly for determinism in tests.
		// It wasn't there before, and everything worked fine. We could safely remove it, if needed.
		usersPercents := util.MapKeysSortedByValue(metricsAggr.Percentiles)
		for _, usersPercent := range usersPercents {
			percentAsFloat := metricsAggr.Percentiles[usersPercent]

			query.Columns = append(query.Columns, model.SelectColumn{
				Expression: aexp.MultiFunctionExp{
					Name: "quantiles",
					Args: []aexp.AExp{aexp.Literal(percentAsFloat), aexp.TableColumn(getFirstFieldName())}},
				Alias: fmt.Sprintf("quantile_%s", usersPercent),
			})
		}
	case "cardinality":
		query.Columns = append(query.Columns, model.SelectColumn{Expression: aexp.Count(aexp.NewComposite(aexp.Symbol("DISTINCT"), aexp.TableColumn(getFirstFieldName())))})

	case "value_count":
		query.Columns = append(query.Columns, model.SelectColumn{Expression: aexp.Count()})

	case "stats":
		fieldName := getFirstFieldName()

		query.Columns = append(query.Columns, model.SelectColumn{Expression: aexp.Count(aexp.TableColumn(fieldName))},
			model.SelectColumn{Expression: aexp.Function("minOrNull", aexp.TableColumn(fieldName))},
			model.SelectColumn{Expression: aexp.Function("maxOrNull", aexp.TableColumn(fieldName))},
			model.SelectColumn{Expression: aexp.Function("avgOrNull", aexp.TableColumn(fieldName))},
			model.SelectColumn{Expression: aexp.Function("sumOrNull", aexp.TableColumn(fieldName))})

	case "top_hits":
		fieldsAsString := strings.Join(metricsAggr.FieldNames, ", ")

		// TODO add/restore tests for top_hits. E.g. we missed WHERE in FROM below, so the SQL might not be correct
		query.FromClause = fmt.Sprintf(
			"(SELECT %s, ROW_NUMBER() OVER (PARTITION BY %s) AS %s FROM %s WHERE %s)",
			fieldsAsString, fieldsAsString, model.RowNumberColumnName, query.FromClause, b.whereBuilder.WhereClauseAsString(),
		)

	case "top_metrics":
		// This appending of `metricsAggr.SortBy` and having it duplicated in SELECT block
		// is a way to pass value we're sorting by to the query result. In the future we might add SQL aliasing support, e.g. SELECT x AS 'sort_by' FROM ...
		if len(b.Query.GroupBy) > 0 {
			var ordFunc string
			switch metricsAggr.Order {
			case "asc":
				ordFunc = `maxOrNull`
			case "desc":
				ordFunc = `minOrNull`
			}
			var topSelectFields []string
			innerFields := append(metricsAggr.FieldNames, metricsAggr.SortBy)
			for _, field := range innerFields {
				topSelectFields = append(topSelectFields, fmt.Sprintf(`%s("%s") AS "windowed_%s"`, ordFunc, field, field))
			}

			for _, field := range topSelectFields {
				query.Columns = append(query.Columns, model.SelectColumn{Expression: aexp.SQL{Query: field}})
			}

			partitionByArr := make([]string, 0, len(b.Query.GroupBy))
			for _, groupByField := range b.Query.GroupBy {
				partitionByArr = append(partitionByArr, groupByField.SQL())
			}
			partitionBy := strings.Join(partitionByArr, ", ")
			fieldsAsString := strings.Join(quoteArray(innerFields), ", ") // need those fields in the inner clause
			query.FromClause = fmt.Sprintf(
				"(SELECT %s, ROW_NUMBER() OVER (PARTITION BY %s ORDER BY %s %s) AS %s FROM %s WHERE %s)",
				fieldsAsString, partitionBy,
				strconv.Quote(metricsAggr.SortBy), metricsAggr.Order,
				model.RowNumberColumnName, query.FromClause, b.whereBuilder.WhereClauseAsString(),
			)
			query.WhereClause = model.And([]where_clause.Statement{query.WhereClause, where_clause.NewInfixOp(where_clause.NewColumnRef(model.RowNumberColumnName), "<=", where_clause.NewLiteral(strconv.Itoa(metricsAggr.Size)))})
		} else {
			query.Limit = metricsAggr.Size
			for _, f := range metricsAggr.FieldNames {
				query.Columns = append(query.Columns, model.SelectColumn{Expression: aexp.TableColumn(f)})
			}
			if metricsAggr.sortByExists() {
				query.Columns = append(query.Columns, model.SelectColumn{Expression: aexp.TableColumn(metricsAggr.SortBy)})
				query.OrderBy = append(query.OrderBy, model.NewSortColumn(metricsAggr.SortBy, strings.ToLower(metricsAggr.Order) == "desc"))
			}
		}
	case "percentile_ranks":
		for _, cutValueAsString := range metricsAggr.FieldNames[1:] {
			cutValue, _ := strconv.ParseFloat(cutValueAsString, 64)
			Select := fmt.Sprintf("count(if(%s<=%f, 1, NULL))/count(*)*100", strconv.Quote(getFirstFieldName()), cutValue)

			query.Columns = append(query.Columns, model.SelectColumn{Expression: aexp.SQL{Query: Select}})
		}
	case "extended_stats":

		fieldNameBare := getFirstFieldName()

		// add column with fn applied to field
		addColumn := func(funcName string) {
			query.Columns = append(query.Columns, model.SelectColumn{Expression: aexp.Function(funcName, aexp.TableColumn(fieldNameBare))})
		}

		addColumn("count")
		addColumn("minOrNull")
		addColumn("maxOrNull")
		addColumn("avgOrNull")
		addColumn("sumOrNull")

		query.Columns = append(query.Columns, model.SelectColumn{Expression: aexp.Function("sumOrNull", aexp.Infix(aexp.TableColumn(fieldNameBare), "*", aexp.TableColumn(fieldNameBare)))})

		addColumn("varPop")
		addColumn("varSamp")
		addColumn("stddevPop")
		addColumn("stddevSamp")

	default:
		logger.WarnWithCtx(b.ctx).Msgf("unknown metrics aggregation: %s", metricsAggr.AggrType)
		query.CanParse = false
	}
	switch metricsAggr.AggrType {
	case "sum":
		query.Type = metrics_aggregations.NewSum(b.ctx, metricsAggr.FieldType)
	case "min":
		query.Type = metrics_aggregations.NewMin(b.ctx, metricsAggr.FieldType)
	case "max":
		query.Type = metrics_aggregations.NewMax(b.ctx, metricsAggr.FieldType)
	case "avg":
		query.Type = metrics_aggregations.NewAvg(b.ctx, metricsAggr.FieldType)
	case "stats":
		query.Type = metrics_aggregations.NewStats(b.ctx)
	case "extended_stats":
		query.Type = metrics_aggregations.NewExtendedStats(b.ctx, metricsAggr.sigma)
	case "cardinality":
		query.Type = metrics_aggregations.NewCardinality(b.ctx)
	case "quantile":
		query.Type = metrics_aggregations.NewQuantile(b.ctx, metricsAggr.Keyed, metricsAggr.FieldType)
	case "top_hits":
		query.Type = metrics_aggregations.NewTopHits(b.ctx)
	case "top_metrics":
		query.Type = metrics_aggregations.NewTopMetrics(b.ctx, metricsAggr.sortByExists())
	case "value_count":
		query.Type = metrics_aggregations.NewValueCount(b.ctx)
	case "percentile_ranks":
		query.Type = metrics_aggregations.NewPercentileRanks(b.ctx, metricsAggr.Keyed)
	}
	return query
}

// ParseAggregationJson parses JSON with aggregation query and returns array of queries with aggregations.
// If there are no aggregations, returns nil.
func (cw *ClickhouseQueryTranslator) ParseAggregationJson(body types.JSON) ([]model.Query, error) {
	queryAsMap := body.Clone()
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

	// count(*) is needed for every request. We should change it and don't duplicate it, as some
	// requests also ask for that themselves, but let's leave it for later.
	aggregations := []model.Query{currentAggr.buildCountAggregation(model.NoMetadataField)}

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
			err := cw.parseAggregation(&currentAggr, aggr, &aggregations)
			if err != nil {
				return nil, err
			}
			currentAggr.Aggregators = currentAggr.Aggregators[:len(currentAggr.Aggregators)-1]
		}
	} else {
		return nil, fmt.Errorf("no aggs -> request is not an aggregation query")
	}

	const defaultSearchSize = 10
	size := cw.parseSize(queryAsMap, defaultSearchSize)
	if size > 0 {
		simpleQuery := currentAggr.whereBuilder
		if sort, ok := queryAsMap["sort"]; ok {
			simpleQuery.OrderBy = cw.parseSortFields(sort)
		}
		hitQuery := cw.BuildNRowsQuery("*", simpleQuery, size)
		aggregations = append(aggregations, *hitQuery)
	}
	return aggregations, nil
}

// 'resultAccumulator' - array when we store results
// 'queryMap' always looks like this:
//
//	"aggs": {
//	  "arbitrary_aggregation_name": {
//	     ["some aggregation":
//	        { "arbitrary_aggregation_name_2": { ... },]
//	     ["some other aggregation": { ... },]
//	     ["aggs": { ... }]
//	  }
//	}
//
// Notice that on 0, 2, ..., level of nesting we have "aggs" key or aggregation type.
// On 1, 3, ... level of nesting we have names of aggregations, which can be any arbitrary strings.
// This function is called on those 1, 3, ... levels, and parses and saves those aggregation names.

func (cw *ClickhouseQueryTranslator) parseAggregationNames(currentAggr *aggrQueryBuilder, queryMap QueryMap, resultAccumulator *[]model.Query) (err error) {
	// We process subaggregations, introduced via (k, v), meaning 'aggregation_name': { dict }
	for k, v := range queryMap {
		// I assume it's new aggregator name
		logger.DebugWithCtx(cw.Ctx).Msgf("names += %s", k)
		currentAggr.Aggregators = append(currentAggr.Aggregators, model.NewAggregatorEmpty(k))
		if subAggregation, ok := v.(QueryMap); ok {
			err = cw.parseAggregation(currentAggr, subAggregation, resultAccumulator)
			if err != nil {
				return err
			}
		} else {
			logger.ErrorWithCtxAndReason(cw.Ctx, logger.ReasonUnsupportedQuery("unexpected_type")).
				Msgf("unexpected type of subaggregation: (%v: %v), value type: %T. Skipping", k, v, v)
		}
		logger.DebugWithCtx(cw.Ctx).Msgf("names -= %s", k)
		currentAggr.Aggregators = currentAggr.Aggregators[:len(currentAggr.Aggregators)-1]
	}
	return nil
}

// Builds aggregations recursively. Seems to be working on all examples so far,
// even though it's a pretty simple algorithm.
// When making changes, look at the order in which we parse fields, it is very important for correctness.
//
// 'resultAccumulator' - array when we store results
// 'queryMap' always looks like this:
//
//	"aggs": {
//	  "arbitrary_aggregation_name": {
//	     ["some aggregation":
//	        { "arbitrary_aggregation_name_2": { ... },]
//	     ["some other aggregation": { ... },]
//	     ["aggs": { ... }]
//	  }
//	}
//
// Notice that on 0, 2, ..., level of nesting we have "aggs" key or aggregation type.
// On 1, 3, ... level of nesting we have names of aggregations, which can be any arbitrary strings.
// This function is called on those 0, 2, ... levels, and parses the actual aggregations.
func (cw *ClickhouseQueryTranslator) parseAggregation(currentAggr *aggrQueryBuilder, queryMap QueryMap, resultAccumulator *[]model.Query) error {
	if len(queryMap) == 0 {
		return nil
	}

	filterOnThisLevel := false
	whereBeforeNesting := currentAggr.whereBuilder // to restore it after processing this level
	queryTypeBeforeNesting := currentAggr.Type
	limitBeforeNesting := currentAggr.Limit

	// check if metadata's present
	var metadata model.JsonMap
	if metaRaw, exists := queryMap["meta"]; exists {
		metadata = metaRaw.(model.JsonMap)
		delete(queryMap, "meta")
	} else {
		metadata = model.NoMetadataField
	}

	// 1. Metrics aggregation => always leaf
	if metricsAggrResult, isMetrics := cw.tryMetricsAggregation(queryMap); isMetrics {
		*resultAccumulator = append(*resultAccumulator, currentAggr.buildMetricsAggregation(metricsAggrResult, metadata))
		return nil
	}

	// 2. Pipeline aggregation => always leaf (for now)
	pipelineAggregationType, isPipelineAggregation := cw.parsePipelineAggregations(queryMap)
	if isPipelineAggregation {
		*resultAccumulator = append(*resultAccumulator, currentAggr.finishBuildingAggregationPipeline(pipelineAggregationType, metadata))
	}

	// 3. Now process filter(s) first, because they apply to everything else on the same level or below.
	// Also filter introduces count to current level.
	if filterRaw, ok := queryMap["filter"]; ok {
		if filter, ok := filterRaw.(QueryMap); ok {
			filterOnThisLevel = true
			currentAggr.Type = metrics_aggregations.NewCount(cw.Ctx)
			currentAggr.whereBuilder = model.CombineWheres(cw.Ctx, currentAggr.whereBuilder, cw.parseQueryMap(filter))
			*resultAccumulator = append(*resultAccumulator, currentAggr.buildCountAggregation(metadata))
		} else {
			logger.WarnWithCtx(cw.Ctx).Msgf("filter is not a map, but %T, value: %v. Skipping", filterRaw, filterRaw)
		}
		delete(queryMap, "filter")
	}

	// 4. Bucket aggregations. They introduce new subaggregations, even if no explicit subaggregation defined on this level.
	bucketAggrPresent, nonSchemaFieldsAddedCount, groupByFieldsAddedCount, err := cw.tryBucketAggregation(currentAggr, queryMap)
	if err != nil {
		return err
	}
	if nonSchemaFieldsAddedCount > 0 {
		if len(currentAggr.Aggregators) > 0 {
			currentAggr.Aggregators[len(currentAggr.Aggregators)-1].Empty = false
		} else {
			logger.ErrorWithCtx(cw.Ctx).Msgf("nonSchemaFieldsAddedCount > 0, but no aggregators present")
		}
	}

	// process "range" with subaggregations
	Range, isRange := currentAggr.Type.(bucket_aggregations.Range)
	if isRange {
		cw.processRangeAggregation(currentAggr, Range, queryMap, resultAccumulator, metadata)
	}

	// TODO what happens if there's all: filters, range, and subaggregations at current level?
	// We probably need to do |ranges| * |filters| * |subaggregations| queries, but we don't do that yet.
	// Or probably a bit less, if optimized correctly.
	// Let's wait until we see such a query, maybe range and filters are mutually exclusive.

	filters, isFilters := currentAggr.Type.(bucket_aggregations.Filters)
	if isFilters {
		cw.processFiltersAggregation(currentAggr, filters, queryMap, resultAccumulator)
	}

	aggsHandledSeparately := isRange || isFilters
	if aggs, ok := queryMap["aggs"]; ok && !aggsHandledSeparately {
		err = cw.parseAggregationNames(currentAggr, aggs.(QueryMap), resultAccumulator)
		if err != nil {
			return err
		}
	}
	delete(queryMap, "aggs") // no-op if no "aggs"

	if bucketAggrPresent && !aggsHandledSeparately {
		// range aggregation has separate, optimized handling
		*resultAccumulator = append(*resultAccumulator, currentAggr.buildBucketAggregation(metadata))
	}

	for k, v := range queryMap {
		// should be empty by now. If it's not, it's an unsupported/unrecognized type of aggregation.
		logger.ErrorWithCtxAndReason(cw.Ctx, logger.ReasonUnsupportedQuery(k)).
			Msgf("unexpected type of subaggregation: (%v: %v), value type: %T. Skipping", k, v, v)
	}

	// restore current state, removing subaggregation state
	if filterOnThisLevel {
		currentAggr.whereBuilder = whereBeforeNesting
	}
	if nonSchemaFieldsAddedCount > 0 {

		if len(currentAggr.Columns) >= nonSchemaFieldsAddedCount {
			currentAggr.Columns = currentAggr.Columns[:len(currentAggr.Columns)-nonSchemaFieldsAddedCount]
		} else {
			logger.ErrorWithCtx(cw.Ctx).Msgf("nonSchemaFieldsAddedCount > currentAggr.Columns length -> should be impossible")
		}

	}
	if groupByFieldsAddedCount > 0 {
		if len(currentAggr.GroupBy) >= groupByFieldsAddedCount {
			currentAggr.GroupBy = currentAggr.GroupBy[:len(currentAggr.GroupBy)-groupByFieldsAddedCount]
		} else {
			logger.ErrorWithCtx(cw.Ctx).Msgf("groupByFieldsAddecCount > currentAggr.GroupByFields length -> should be impossible")
		}
		if len(currentAggr.OrderBy) >= groupByFieldsAddedCount {
			currentAggr.OrderBy = currentAggr.GroupBy[:len(currentAggr.OrderBy)-groupByFieldsAddedCount]
		} else {
			logger.ErrorWithCtx(cw.Ctx).Msgf("groupByFieldsAddecCount > currentAggr.OrderBy length -> should be impossible")
		}
	}
	currentAggr.Type = queryTypeBeforeNesting
	currentAggr.Limit = limitBeforeNesting
	return nil
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
			fieldName, isFieldNameFromScript := cw.parseFieldFieldMaybeScript(v, k)
			return metricsAggregation{
				AggrType:            k,
				FieldNames:          []string{fieldName},
				FieldType:           cw.Table.GetDateTimeType(cw.Ctx, fieldName),
				IsFieldNameCompound: isFieldNameFromScript,
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
			FieldType:   cw.Table.GetDateTimeType(cw.Ctx, fieldName),
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
			FieldType:  metricsAggregationDefaultFieldType, // don't need to check, it's unimportant for this aggregation
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
		var keyed bool
		if keyedRaw, ok := percentileRanks.(QueryMap)["keyed"]; ok {
			if keyed, ok = keyedRaw.(bool); !ok {
				logger.WarnWithCtx(cw.Ctx).Msgf("keyed specified for percentiles aggregation is not a boolean. Querymap: %v", queryMap)
				keyed = keyedDefaultValuePercentileRanks
			}
		} else {
			keyed = keyedDefaultValuePercentileRanks
		}
		return metricsAggregation{
			AggrType:   "percentile_ranks",
			FieldNames: fieldNames,
			FieldType:  metricsAggregationDefaultFieldType, // don't need to check, it's unimportant for this aggregation
			Keyed:      keyed,
		}, true
	}

	if extendedStatsRaw, exists := queryMap["extended_stats"]; exists {
		extendedStats, ok := extendedStatsRaw.(QueryMap)
		if !ok {
			logger.WarnWithCtx(cw.Ctx).Msgf("extended_stats is not a map, but %T, value: %v. Skipping.", extendedStatsRaw, extendedStatsRaw)
			return metricsAggregation{}, false
		}
		const defaultSigma = 2.0
		sigma := defaultSigma
		if sigmaRaw, exists := extendedStats["sigma"]; exists {
			sigma, ok = sigmaRaw.(float64)
			if !ok {
				logger.WarnWithCtx(cw.Ctx).Msgf("sigma in extended_stats is not a float64, but %T, value: %v. Using default.", sigmaRaw, sigmaRaw)
				sigma = defaultSigma
			}
		}
		return metricsAggregation{
			AggrType:   "extended_stats",
			FieldNames: []string{cw.parseFieldField(extendedStats, "extended_stats")},
			sigma:      sigma,
		}, true
	}

	return metricsAggregation{}, false
}

// tryBucketAggregation checks if 'queryMap' represents a bucket aggregation on current level and if it does, updates 'currentAggr'.
// Returns:
// * 'success': was it bucket aggreggation?
// * 'nonSchemaFieldAdded': did we add a non-schema field to 'currentAggr', if it turned out to be bucket aggregation? If we did, we need to know, to remove it later.
func (cw *ClickhouseQueryTranslator) tryBucketAggregation(currentAggr *aggrQueryBuilder, queryMap QueryMap) (
	success bool, nonSchemaFieldsAddedCount, groupByFieldsAddedCount int, err error) {

	success = true // returned in most cases
	if histogramRaw, ok := queryMap["histogram"]; ok {
		histogram, ok := histogramRaw.(QueryMap)
		if !ok {
			logger.WarnWithCtx(cw.Ctx).Msgf("date_histogram is not a map, but %T, value: %v", histogramRaw, histogramRaw)
		}
		fieldName, isFieldNameFromScript := cw.parseFieldFieldMaybeScript(histogram, "histogram")
		var fieldNameProperlyQuoted string
		if isFieldNameFromScript {
			fieldNameProperlyQuoted = fieldName
		} else {
			fieldNameProperlyQuoted = strconv.Quote(fieldName)
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
		currentAggr.Type = bucket_aggregations.NewHistogram(cw.Ctx, interval, minDocCount)

		var groupByStr string
		if interval != 1.0 {
			groupByStr = fmt.Sprintf("floor(%s / %f) * %f", fieldNameProperlyQuoted, interval, interval)
		} else {
			groupByStr = fieldNameProperlyQuoted
		}

		currentAggr.GroupBy = append(currentAggr.GroupBy, model.SelectColumn{Expression: aexp.SQL{Query: groupByStr}})
		currentAggr.OrderBy = append(currentAggr.OrderBy, model.SelectColumn{Expression: aexp.SQL{Query: groupByStr}})

		currentAggr.Columns = append(currentAggr.Columns, model.SelectColumn{Expression: aexp.SQL{Query: groupByStr}})

		delete(queryMap, "histogram")
		return success, 1, 1, nil
	}
	if dateHistogramRaw, ok := queryMap["date_histogram"]; ok {
		dateHistogram, ok := dateHistogramRaw.(QueryMap)
		if !ok {
			logger.WarnWithCtx(cw.Ctx).Msgf("date_histogram is not a map, but %T, value: %v", dateHistogramRaw, dateHistogramRaw)
		}
		minDocCount := cw.parseMinDocCount(dateHistogram)
		currentAggr.Type = bucket_aggregations.NewDateHistogram(cw.Ctx, minDocCount, cw.extractInterval(dateHistogram))
		histogramPartOfQuery := cw.createHistogramPartOfQuery(dateHistogram)

		currentAggr.GroupBy = append(currentAggr.GroupBy, model.SelectColumn{Expression: aexp.SQL{Query: histogramPartOfQuery}})
		currentAggr.OrderBy = append(currentAggr.OrderBy, model.SelectColumn{Expression: aexp.SQL{Query: histogramPartOfQuery}})

		currentAggr.Columns = append(currentAggr.Columns, model.SelectColumn{Expression: aexp.SQL{Query: histogramPartOfQuery}})

		delete(queryMap, "date_histogram")
		return success, 1, 1, nil
	}
	for _, termsType := range []string{"terms", "significant_terms"} {
		if terms, ok := queryMap[termsType]; ok {
			currentAggr.Type = bucket_aggregations.NewTerms(cw.Ctx, termsType == "significant_terms")

			isEmptyGroupBy := len(currentAggr.GroupBy) == 0

			currentAggr.GroupBy = append(currentAggr.GroupBy, model.SelectColumn{Expression: aexp.TableColumn(cw.parseFieldField(terms, termsType))})
			currentAggr.Columns = append(currentAggr.Columns, model.SelectColumn{Expression: aexp.TableColumn(cw.parseFieldField(terms, termsType))})

			orderByAdded := false
			size := 10
			if _, ok := queryMap["aggs"]; isEmptyGroupBy && !ok { // we can do limit only it terms are not nested
				if jsonMap, ok := terms.(QueryMap); ok {
					if sizeRaw, ok := jsonMap["size"]; ok {
						if sizeParsed, ok := sizeRaw.(float64); ok {
							size = int(sizeParsed)
						} else {
							logger.WarnWithCtx(cw.Ctx).Msgf("size is not an float64, but %T, value: %v. Using default", sizeRaw, sizeRaw)
						}
					}
				}
				currentAggr.Limit = size
				currentAggr.OrderBy = append(currentAggr.OrderBy, model.NewSortByCountColumn(true))
				orderByAdded = true
			}
			delete(queryMap, termsType)
			if !orderByAdded {
				currentAggr.OrderBy = append(currentAggr.OrderBy, model.SelectColumn{Expression: aexp.TableColumn(cw.parseFieldField(terms, termsType))})
			}
			return success, 1, 1, nil
			/* will remove later
			var size int
			if sizeRaw, exists := terms.(QueryMap)["size"]; exists {
				size = (int)(sizeRaw.(float64))
			} else {
				size = bucket_aggregations.DefaultSize
			}
			currentAggr.Type = bucket_aggregations.NewTerms(cw.Ctx, size, termsType == "significant_terms")

			fieldName := strconv.Quote(cw.parseFieldField(terms, termsType))
			currentAggr.GroupByFields = append(currentAggr.GroupByFields, fieldName)
			currentAggr.NonSchemaFields = append(currentAggr.NonSchemaFields, fieldName)
			currentAggr.SuffixClauses = append(currentAggr.SuffixClauses, fmt.Sprintf("LIMIT %d", size))
			currentAggr.SubSelect = currentAggr.Query.String()
			fmt.Println("SUB:", currentAggr.SubSelect)
			delete(queryMap, termsType)
			return success, 1, 1, nil
			*/
		}
	}
	if rangeRaw, ok := queryMap["range"]; ok {
		rangeMap, ok := rangeRaw.(QueryMap)
		if !ok {
			logger.WarnWithCtx(cw.Ctx).Msgf("range is not a map, but %T, value: %v. Using empty map", rangeRaw, rangeRaw)
		}
		Range := cw.parseRangeAggregation(rangeMap)
		currentAggr.Type = Range
		if Range.Keyed {
			currentAggr.Aggregators[len(currentAggr.Aggregators)-1].Keyed = true
		}
		delete(queryMap, "range")
		return success, 0, 0, nil
	}
	if dateRangeRaw, ok := queryMap["date_range"]; ok {
		dateRange, ok := dateRangeRaw.(QueryMap)
		if !ok {
			logger.WarnWithCtx(cw.Ctx).Msgf("date_range is not a map, but %T, value: %v. Using empty map", dateRangeRaw, dateRangeRaw)
		}
		dateRangeParsed, err := cw.parseDateRangeAggregation(dateRange)
		if err != nil {
			logger.ErrorWithCtx(cw.Ctx).Err(err).Msg("failed to parse date_range aggregation")
			return false, 0, 0, err
		}
		currentAggr.Type = dateRangeParsed
		for _, interval := range dateRangeParsed.Intervals {

			currentAggr.Columns = append(currentAggr.Columns, model.SelectColumn{Expression: aexp.SQL{Query: interval.ToSQLSelectQuery(dateRangeParsed.FieldName)}})

			if sqlSelect, selectNeeded := interval.BeginTimestampToSQL(); selectNeeded {
				currentAggr.Columns = append(currentAggr.Columns, model.SelectColumn{Expression: aexp.SQL{Query: sqlSelect}})
			}
			if sqlSelect, selectNeeded := interval.EndTimestampToSQL(); selectNeeded {
				currentAggr.Columns = append(currentAggr.Columns, model.SelectColumn{Expression: aexp.SQL{Query: sqlSelect}})
			}
		}

		delete(queryMap, "date_range")
		return success, dateRangeParsed.SelectColumnsNr, 0, nil
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
			currentAggr.whereBuilder = model.CombineWheres(cw.Ctx, currentAggr.whereBuilder, cw.parseBool(Bool))
		} else {
			logger.WarnWithCtx(cw.Ctx).Msgf("bool is not a map, but %T, value: %v. Skipping", boolRaw, boolRaw)
		}
		delete(queryMap, "bool")
		return
	}
	if isFilters, aggregation := cw.parseFilters(queryMap); isFilters {
		currentAggr.Type = aggregation
		return
	}
	success = false
	return
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

// parseFieldFieldMaybeScript is basically almost a copy of parseFieldField above, but it also handles a basic script, if "field" is missing.
func (cw *ClickhouseQueryTranslator) parseFieldFieldMaybeScript(shouldBeMap any, aggregationType string) (field string, isFromScript bool) {
	isFromScript = false
	Map, ok := shouldBeMap.(QueryMap)
	if !ok {
		logger.WarnWithCtx(cw.Ctx).Msgf("%s aggregation is not a map, but %T, value: %v", aggregationType, shouldBeMap, shouldBeMap)
		return
	}
	// maybe "field" field
	if fieldRaw, ok := Map["field"]; ok {
		if field, ok = fieldRaw.(string); ok {
			return
		} else {
			logger.WarnWithCtx(cw.Ctx).Msgf("field is not a string, but %T, value: %v", fieldRaw, fieldRaw)
		}
	}

	// else: maybe script
	if fieldName, ok := cw.parseFieldFromScriptField(Map); ok {
		return fmt.Sprintf("toHour(`%s`)", fieldName), true
	}

	logger.WarnWithCtx(cw.Ctx).Msgf("field not found in %s aggregation: %v", aggregationType, Map)
	return
}

func (cw *ClickhouseQueryTranslator) parseFieldFromScriptField(queryMap QueryMap) (fieldName string, success bool) {
	scriptRaw, exists := queryMap["script"]
	if !exists {
		return
	}
	script, ok := scriptRaw.(QueryMap)
	if !ok {
		logger.WarnWithCtx(cw.Ctx).Msgf("script is not a JsonMap, but %T, value: %v", scriptRaw, scriptRaw)
		return
	}

	sourceRaw, exists := script["source"]
	if !exists {
		logger.WarnWithCtx(cw.Ctx).Msgf("source not found in script: %v", script)
		return
	}
	source, ok := sourceRaw.(string)
	if !ok {
		logger.WarnWithCtx(cw.Ctx).Msgf("source is not a string, but %T, value: %v", sourceRaw, sourceRaw)
	}

	// source must look like "doc['field_name'].value.getHour()" or "doc['field_name'].value.hourOfDay"
	wantedRegex := regexp.MustCompile(`^doc\['(\w+)']\.value\.(?:getHour\(\)|hourOfDay)$`)
	matches := wantedRegex.FindStringSubmatch(source)
	if len(matches) == 2 {
		return matches[1], true
	}
	return
}

func (cw *ClickhouseQueryTranslator) parseMinDocCount(queryMap QueryMap) int {
	if minDocCountRaw, exists := queryMap["min_doc_count"]; exists {
		if minDocCount, ok := minDocCountRaw.(float64); ok {
			asInt := int(minDocCount)
			if asInt != 0 && asInt != 1 {
				logger.WarnWithCtx(cw.Ctx).Msgf("min_doc_count is not 0 or 1, but %d. Not really supported", asInt)
			}
			return asInt
		} else {
			logger.WarnWithCtx(cw.Ctx).Msgf("min_doc_count is not a number, but %T, value: %v. Using default value: %d",
				minDocCountRaw, minDocCountRaw, bucket_aggregations.DefaultMinDocCount)
		}
	}
	return bucket_aggregations.DefaultMinDocCount
}

// quoteArray returns a new array with the same elements, but quoted
func quoteArray(array []string) []string {
	quotedArray := make([]string, 0, len(array))
	for _, el := range array {
		quotedArray = append(quotedArray, strconv.Quote(el))
	}
	return quotedArray
}
