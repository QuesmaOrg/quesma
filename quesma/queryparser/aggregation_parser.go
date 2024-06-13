package queryparser

import (
	"context"
	"fmt"
	"mitmproxy/quesma/clickhouse"
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/model"
	"mitmproxy/quesma/model/bucket_aggregations"
	"mitmproxy/quesma/model/metrics_aggregations"

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
	Fields              []model.Expr            // on these fields we're doing aggregation. Array, because e.g. 'top_hits' can have multiple fields
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

func (b *aggrQueryBuilder) buildAggregationCommon(metadata model.JsonMap) *model.Query {
	query := b.Query
	query.WhereClause = b.whereBuilder.WhereClause

	// Need to copy, as we might be proceeding to modify 'b' pointer
	query.CopyAggregationFields(b.Query)

	query.TrimKeywordFromFields()

	query.Metadata = metadata
	return &query
}

func (b *aggrQueryBuilder) buildCountAggregation(metadata model.JsonMap) *model.Query {
	query := b.buildAggregationCommon(metadata)
	query.Type = metrics_aggregations.NewCount(b.ctx)

	query.Columns = append(query.Columns, model.NewCountFunc())
	return query
}

func (b *aggrQueryBuilder) buildBucketAggregation(metadata model.JsonMap) *model.Query {
	query := b.buildAggregationCommon(metadata)

	query.Columns = append(query.Columns, model.NewCountFunc())
	return query
}

func (b *aggrQueryBuilder) buildMetricsAggregation(metricsAggr metricsAggregation, metadata model.JsonMap) *model.Query {
	getFirstExpression := func() model.Expr {
		if len(metricsAggr.Fields) > 0 {
			return metricsAggr.Fields[0]
		}
		logger.ErrorWithCtx(b.ctx).Msg("No field names in metrics aggregation. Using empty.")
		return nil
	}

	query := b.buildAggregationCommon(metadata)

	switch metricsAggr.AggrType {
	case "sum", "min", "max", "avg":
		query.Columns = append(query.Columns, model.NewFunction(metricsAggr.AggrType+"OrNull", getFirstExpression()))
	case "quantile":
		// Sorting here useful mostly for determinism in tests.
		// It wasn't there before, and everything worked fine. We could safely remove it, if needed.
		usersPercents := util.MapKeysSortedByValue(metricsAggr.Percentiles)
		for _, usersPercent := range usersPercents {
			percentAsFloat := metricsAggr.Percentiles[usersPercent]
			query.Columns = append(query.Columns, model.NewAliasedExpr(
				model.MultiFunctionExpr{
					Name: "quantiles",
					Args: []model.Expr{model.NewLiteral(percentAsFloat), getFirstExpression()}},
				fmt.Sprintf("quantile_%s", usersPercent),
			))

		}
	case "cardinality":
		query.Columns = append(query.Columns, model.NewCountFunc(model.NewDistinctExpr(getFirstExpression())))

	case "value_count":
		query.Columns = append(query.Columns, model.NewCountFunc())

	case "stats":
		expr := getFirstExpression()

		query.Columns = append(query.Columns, model.NewCountFunc(expr),
			model.NewFunction("minOrNull", expr),
			model.NewFunction("maxOrNull", expr),
			model.NewFunction("avgOrNull", expr),
			model.NewFunction("sumOrNull", expr))

	case "top_hits":
		// TODO add/restore tests for top_hits. E.g. we missed WHERE in FROM below, so the SQL might not be correct
		innerFieldsAsSelect := make([]model.Expr, len(metricsAggr.Fields))
		copy(innerFieldsAsSelect, metricsAggr.Fields)
		query.Columns = append(query.Columns, innerFieldsAsSelect...)
		/*
			query.FromClause = fmt.Sprintf(
				"(SELECT %s, ROW_NUMBER() OVER (PARTITION BY %s) AS %s FROM %s%s)",
				metricsAggr.Fields, partitionBy, model.RowNumberColumnName, query.FromClause, whereString,
			)
		*/
		query.FromClause = query.NewSelectExprWithRowNumber(
			innerFieldsAsSelect, b.GroupBy, b.whereBuilder.WhereClauseAsString(), "", true)
		query.WhereClause = model.And([]model.Expr{
			query.WhereClause,
			model.NewInfixExpr(
				model.NewColumnRef(model.RowNumberColumnName),
				"<=",
				model.NewLiteral(strconv.Itoa(metricsAggr.Size)),
			)},
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

			innerFields := append(metricsAggr.Fields, model.NewColumnRef(metricsAggr.SortBy))
			for _, field := range innerFields {
				fieldName, _ := strconv.Unquote(model.AsString(field))
				query.Columns = append(query.Columns,
					model.NewAliasedExpr(model.NewFunction(ordFunc, field), fmt.Sprintf("windowed_%s", fieldName)))
			}

			innerFieldsAsSelect := make([]model.Expr, len(innerFields))
			copy(innerFieldsAsSelect, innerFields)
			query.FromClause = query.NewSelectExprWithRowNumber(
				innerFieldsAsSelect, b.Query.GroupBy, b.whereBuilder.WhereClauseAsString(),
				metricsAggr.SortBy, strings.ToLower(metricsAggr.Order) == "desc",
			)
			query.WhereClause = model.And([]model.Expr{query.WhereClause,
				model.NewInfixExpr(model.NewColumnRef(model.RowNumberColumnName), "<=", model.NewLiteral(strconv.Itoa(metricsAggr.Size)))})
		} else {
			innerFieldsAsSelect := make([]model.Expr, len(metricsAggr.Fields))
			copy(innerFieldsAsSelect, metricsAggr.Fields)
			query.Limit = metricsAggr.Size
			query.Columns = append(query.Columns, innerFieldsAsSelect...)
			if metricsAggr.sortByExists() {
				query.Columns = append(query.Columns, model.NewColumnRef(metricsAggr.SortBy))
				if strings.ToLower(metricsAggr.Order) == "desc" {
					query.OrderBy = append(query.OrderBy, model.NewSortColumn(metricsAggr.SortBy, model.DescOrder))
				} else {
					query.OrderBy = append(query.OrderBy, model.NewSortColumn(metricsAggr.SortBy, model.AscOrder))
				}

			}
		}
	case "percentile_ranks":
		for _, cutValueAsString := range metricsAggr.Fields[1:] {
			unquoted, _ := strconv.Unquote(model.AsString(cutValueAsString))
			cutValue, _ := strconv.ParseFloat(unquoted, 64)

			// full exp we create below looks like this:
			// fmt.Sprintf("count(if(%s<=%f, 1, NULL))/count(*)*100", strconv.Quote(getFirstFieldName()), cutValue)

			ifExp := model.NewFunction(
				"if",
				model.NewInfixExpr(getFirstExpression(), "<=", model.NewLiteral(cutValue)),
				model.NewLiteral(1),
				model.NewStringExpr("NULL"),
			)
			firstCountExp := model.NewFunction("count", ifExp)
			twoCountsExp := model.NewInfixExpr(firstCountExp, "/", model.NewCountFunc(model.NewWildcardExpr))

			query.Columns = append(query.Columns, model.NewInfixExpr(twoCountsExp, "*", model.NewLiteral(100)))
		}
	case "extended_stats":

		expr := getFirstExpression()

		// add column with fn applied to field
		addColumn := func(funcName string) {
			query.Columns = append(query.Columns, model.NewFunction(funcName, expr))
		}

		addColumn("count")
		addColumn("minOrNull")
		addColumn("maxOrNull")
		addColumn("avgOrNull")
		addColumn("sumOrNull")

		query.Columns = append(query.Columns, model.NewFunction("sumOrNull", model.NewInfixExpr(expr, "*", expr)))

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
func (cw *ClickhouseQueryTranslator) ParseAggregationJson(body types.JSON) ([]*model.Query, error) {
	queryAsMap := body.Clone()
	currentAggr := aggrQueryBuilder{}
	currentAggr.FromClause = model.NewTableRef(cw.Table.FullTableName())
	currentAggr.TableName = cw.Table.FullTableName()
	currentAggr.ctx = cw.Ctx
	if queryPartRaw, ok := queryAsMap["query"]; ok {
		if queryPart, ok := queryPartRaw.(QueryMap); ok {
			currentAggr.whereBuilder = cw.parseQueryMap(queryPart)
		} else {
			logger.WarnWithCtx(cw.Ctx).Msgf("query is not a map, but %T, query: %v. Skipping", queryPartRaw, queryPartRaw)
		}
	}

	aggregations := make([]*model.Query, 0)

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
			currentAggr.Aggregators = append(currentAggr.Aggregators, model.NewAggregator(aggrName))
			err := cw.parseAggregation(&currentAggr, aggr, &aggregations)
			if err != nil {
				return nil, err
			}
			currentAggr.Aggregators = currentAggr.Aggregators[:len(currentAggr.Aggregators)-1]
		}
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

func (cw *ClickhouseQueryTranslator) parseAggregationNames(currentAggr *aggrQueryBuilder, queryMap QueryMap, resultAccumulator *[]*model.Query) (err error) {
	// We process subaggregations, introduced via (k, v), meaning 'aggregation_name': { dict }
	for k, v := range queryMap {
		// I assume it's new aggregator name
		logger.DebugWithCtx(cw.Ctx).Msgf("names += %s", k)
		currentAggr.Aggregators = append(currentAggr.Aggregators, model.NewAggregator(k))
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
func (cw *ClickhouseQueryTranslator) parseAggregation(currentAggr *aggrQueryBuilder, queryMap QueryMap, resultAccumulator *[]*model.Query) error {
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
	bucketAggrPresent, columnsAdded, groupByFieldsAdded, orderByFieldsAdded, err := cw.tryBucketAggregation(currentAggr, queryMap)
	if err != nil {
		return err
	}
	if groupByFieldsAdded > 0 {
		if len(currentAggr.Aggregators) > 0 {
			currentAggr.Aggregators[len(currentAggr.Aggregators)-1].SplitOverHowManyFields = groupByFieldsAdded
		} else {
			logger.ErrorWithCtx(cw.Ctx).Msgf("groupByFieldsAdded > 0, but no aggregators present")
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
	if columnsAdded > 0 {

		if len(currentAggr.Columns) >= columnsAdded {
			currentAggr.Columns = currentAggr.Columns[:len(currentAggr.Columns)-columnsAdded]
		} else {
			logger.ErrorWithCtx(cw.Ctx).Msgf("columnsAdded > currentAggr.Columns length -> should be impossible")
		}

	}
	if groupByFieldsAdded > 0 {
		if len(currentAggr.GroupBy) >= groupByFieldsAdded {
			currentAggr.GroupBy = currentAggr.GroupBy[:len(currentAggr.GroupBy)-groupByFieldsAdded]
		} else {
			logger.ErrorWithCtx(cw.Ctx).Msgf("groupByFieldsAdded > currentAggr.GroupBy length -> should be impossible")
		}
	}
	if orderByFieldsAdded > 0 {
		if len(currentAggr.OrderBy) >= orderByFieldsAdded {
			currentAggr.OrderBy = currentAggr.OrderBy[:len(currentAggr.OrderBy)-orderByFieldsAdded]
		} else {
			logger.ErrorWithCtx(cw.Ctx).Msgf("orderByFieldsAdded > currentAggr.OrderBy length -> should be impossible")
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
			field, isFromScript := cw.parseFieldFieldMaybeScript(v, k)
			return metricsAggregation{
				AggrType:            k,
				Fields:              []model.Expr{field},
				FieldType:           cw.Table.GetDateTimeTypeFromSelectClause(cw.Ctx, field),
				IsFieldNameCompound: isFromScript,
			}, true
		}
	}

	if percentile, ok := queryMap["percentiles"]; ok {
		percentileMap, ok := percentile.(QueryMap)
		if !ok {
			logger.WarnWithCtx(cw.Ctx).Msgf("percentiles is not a map, but %T, value: %v. Using empty map.", percentile, percentile)
		}
		field, keyed, percentiles := cw.parsePercentilesAggregation(percentileMap)
		return metricsAggregation{
			AggrType:    "quantile",
			Fields:      []model.Expr{field},
			FieldType:   cw.Table.GetDateTimeTypeFromSelectClause(cw.Ctx, field),
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
		exprs := make([]model.Expr, 0, len(fields))
		for i, fieldNameRaw := range fields {
			if fieldName, ok := fieldNameRaw.(string); ok {
				exprs = append(exprs, model.NewColumnRef(fieldName))
			} else {
				logger.WarnWithCtx(cw.Ctx).Msgf("field %d in top_hits is not a string. Field's type: %T, value: %v. Skipping.",
					i, fieldNameRaw, fieldNameRaw)
			}
		}

		const defaultSize = 1
		size := defaultSize
		if mapTyped, ok := topHits.(QueryMap); ok {
			size = cw.parseSize(mapTyped, defaultSize)
		} else {
			logger.WarnWithCtx(cw.Ctx).Msgf("top_hits is not a map, but %T, value: %v. Using default size.", topHits, topHits)
		}
		return metricsAggregation{
			AggrType:  "top_hits",
			Fields:    exprs,
			FieldType: metricsAggregationDefaultFieldType, // don't need to check, it's unimportant for this aggregation
			Size:      size,
		}, true
	}

	// Shortcut here. Percentile_ranks has "field" and a list of "values"
	// I'm keeping all of them in `fieldNames' array for "simplicity".
	if percentileRanks, ok := queryMap["percentile_ranks"]; ok {
		fields := []model.Expr{cw.parseFieldField(percentileRanks, "percentile_ranks")}
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
				fields = append(fields, model.NewColumnRef(strconv.FormatFloat(cutValueTyped, 'f', -1, 64)))
			case int64:
				fields = append(fields, model.NewColumnRef(strconv.FormatInt(cutValueTyped, 10)))
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
			AggrType:  "percentile_ranks",
			Fields:    fields,
			FieldType: metricsAggregationDefaultFieldType, // don't need to check, it's unimportant for this aggregation
			Keyed:     keyed,
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
			AggrType: "extended_stats",
			Fields:   []model.Expr{cw.parseFieldField(extendedStats, "extended_stats")},
			sigma:    sigma,
		}, true
	}

	return metricsAggregation{}, false
}

// tryBucketAggregation checks if 'queryMap' represents a bucket aggregation on current level and if it does, updates 'currentAggr'.
// Returns:
// * 'success': was it bucket aggreggation?
// * 'nonSchemaFieldAdded': did we add a non-schema field to 'currentAggr', if it turned out to be bucket aggregation? If we did, we need to know, to remove it later.
func (cw *ClickhouseQueryTranslator) tryBucketAggregation(currentAggr *aggrQueryBuilder, queryMap QueryMap) (
	success bool, columnsAdded, groupByFieldsAdded, orderByFieldsAdded int, err error) {

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
		currentAggr.Type = bucket_aggregations.NewHistogram(cw.Ctx, interval, minDocCount)

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

		currentAggr.Columns = append(currentAggr.Columns, col)
		currentAggr.GroupBy = append(currentAggr.GroupBy, col)
		currentAggr.OrderBy = append(currentAggr.OrderBy, model.NewOrderByExprWithoutOrder(col))

		delete(queryMap, "histogram")
		return success, 1, 1, 1, nil
	}
	if dateHistogramRaw, ok := queryMap["date_histogram"]; ok {
		dateHistogram, ok := dateHistogramRaw.(QueryMap)
		if !ok {
			logger.WarnWithCtx(cw.Ctx).Msgf("date_histogram is not a map, but %T, value: %v", dateHistogramRaw, dateHistogramRaw)
		}
		minDocCount := cw.parseMinDocCount(dateHistogram)
		currentAggr.Type = bucket_aggregations.NewDateHistogram(cw.Ctx, minDocCount, cw.extractInterval(dateHistogram))
		histogramPartOfQuery := cw.createHistogramPartOfQuery(dateHistogram)

		currentAggr.Columns = append(currentAggr.Columns, histogramPartOfQuery)
		currentAggr.GroupBy = append(currentAggr.GroupBy, histogramPartOfQuery)
		currentAggr.OrderBy = append(currentAggr.OrderBy, model.NewOrderByExprWithoutOrder(histogramPartOfQuery))

		delete(queryMap, "date_histogram")
		return success, 1, 1, 1, nil
	}
	for _, termsType := range []string{"terms", "significant_terms"} {
		if terms, ok := queryMap[termsType]; ok {
			currentAggr.Type = bucket_aggregations.NewTerms(cw.Ctx, termsType == "significant_terms")

			isEmptyGroupBy := len(currentAggr.GroupBy) == 0

			currentAggr.GroupBy = append(currentAggr.GroupBy, cw.parseFieldField(terms, termsType))
			currentAggr.Columns = append(currentAggr.Columns, cw.parseFieldField(terms, termsType))

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
				currentAggr.OrderBy = append(currentAggr.OrderBy, model.NewSortByCountColumn(model.DescOrder))
				orderByAdded = true
			}
			delete(queryMap, termsType)
			if !orderByAdded {
				currentAggr.OrderBy = append(currentAggr.OrderBy, model.NewOrderByExprWithoutOrder(cw.parseFieldField(terms, termsType)))
			}
			return success, 1, 1, 1, nil
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
	if multiTermsRaw, exists := queryMap["multi_terms"]; exists {
		multiTerms, ok := multiTermsRaw.(QueryMap)
		if !ok {
			logger.WarnWithCtx(cw.Ctx).Msgf("multi_terms is not a map, but %T, value: %v", multiTermsRaw, multiTermsRaw)
		}

		orderByAdded := false
		isEmptyGroupBy := len(currentAggr.GroupBy) == 0
		const defaultSize = 10
		size := cw.parseIntField(multiTerms, "size", defaultSize)
		if _, exists := queryMap["aggs"]; isEmptyGroupBy && !exists { // we can do limit only it terms are not nested
			currentAggr.OrderBy = append(currentAggr.OrderBy, model.NewSortByCountColumn(model.DescOrder))
			currentAggr.Limit = size
			orderByAdded = true
			orderByFieldsAdded = 1
		}

		var fieldsNr int
		if termsRaw, exists := multiTerms["terms"]; exists {
			terms, ok := termsRaw.([]any)
			if !ok {
				logger.WarnWithCtx(cw.Ctx).Msgf("terms is not an array, but %T, value: %v. Using empty array", termsRaw, termsRaw)
			}
			fieldsNr = len(terms)
			for _, term := range terms {
				column := cw.parseFieldField(term, "multi_terms")
				currentAggr.Columns = append(currentAggr.Columns, column)
				currentAggr.GroupBy = append(currentAggr.GroupBy, column)
				if !orderByAdded {
					currentAggr.OrderBy = append(currentAggr.OrderBy, model.NewOrderByExprWithoutOrder(column))
					orderByFieldsAdded++
				}
			}
		} else {
			logger.WarnWithCtx(cw.Ctx).Msg("no terms in multi_terms")
		}

		currentAggr.Type = bucket_aggregations.NewMultiTerms(cw.Ctx, fieldsNr)
		if len(currentAggr.Aggregators) > 0 {
			currentAggr.Aggregators[len(currentAggr.Aggregators)-1].SplitOverHowManyFields = fieldsNr
		} else {
			logger.WarnWithCtx(cw.Ctx).Msgf("empty aggregators, should be impossible. currentAggr: %+v", currentAggr)
		}

		delete(queryMap, "multi_terms")
		return success, fieldsNr, fieldsNr, orderByFieldsAdded, nil
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
		return success, 0, 0, 0, nil
	}
	if dateRangeRaw, ok := queryMap["date_range"]; ok {
		dateRange, ok := dateRangeRaw.(QueryMap)
		if !ok {
			logger.WarnWithCtx(cw.Ctx).Msgf("date_range is not a map, but %T, value: %v. Using empty map", dateRangeRaw, dateRangeRaw)
		}
		dateRangeParsed, err := cw.parseDateRangeAggregation(dateRange)
		if err != nil {
			logger.ErrorWithCtx(cw.Ctx).Err(err).Msg("failed to parse date_range aggregation")
			return false, 0, 0, 0, err
		}
		currentAggr.Type = dateRangeParsed
		for _, interval := range dateRangeParsed.Intervals {

			currentAggr.Columns = append(currentAggr.Columns, model.SQL{Query: interval.ToSQLSelectQuery(dateRangeParsed.FieldName)})

			if sqlSelect, selectNeeded := interval.BeginTimestampToSQL(); selectNeeded {
				currentAggr.Columns = append(currentAggr.Columns, model.SQL{Query: sqlSelect})
			}
			if sqlSelect, selectNeeded := interval.EndTimestampToSQL(); selectNeeded {
				currentAggr.Columns = append(currentAggr.Columns, model.SQL{Query: sqlSelect})
			}
		}

		// TODO after https://github.com/QuesmaOrg/quesma/pull/99 it should be only in 1 of 2 cases (keyed or not), just like in range aggregation
		if len(currentAggr.Aggregators) > 0 {
			currentAggr.Aggregators[len(currentAggr.Aggregators)-1].SplitOverHowManyFields = 1
		} else {
			logger.ErrorWithCtx(cw.Ctx).Msg("no aggregators in currentAggr")
		}

		delete(queryMap, "date_range")
		return success, dateRangeParsed.SelectColumnsNr, 0, 0, nil
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
func (cw *ClickhouseQueryTranslator) parseFieldField(shouldBeMap any, aggregationType string) model.Expr {
	Map, ok := shouldBeMap.(QueryMap)
	if !ok {
		logger.WarnWithCtx(cw.Ctx).Msgf("%s aggregation is not a map, but %T, value: %v", aggregationType, shouldBeMap, shouldBeMap)
		return nil
	}
	if fieldRaw, ok := Map["field"]; ok {
		if field, ok := fieldRaw.(string); ok {
			return model.NewColumnRef(cw.Table.ResolveField(cw.Ctx, field)) // model.NewSelectColumnTableField(cw.Table.ResolveField(cw.Ctx, field)) // remove this resolve? we do all transforms after parsing is done?
		} else {
			logger.WarnWithCtx(cw.Ctx).Msgf("field is not a string, but %T, value: %v", fieldRaw, fieldRaw)
		}
	} else {
		logger.WarnWithCtx(cw.Ctx).Msgf("field not found in %s aggregation: %v", aggregationType, Map)
	}
	return nil
}

func (cw *ClickhouseQueryTranslator) parseIntField(queryMap QueryMap, fieldName string, defaultValue int) int {
	if valueRaw, exists := queryMap[fieldName]; exists {
		if asFloat, ok := valueRaw.(float64); ok {
			return int(asFloat)
		}
		logger.WarnWithCtx(cw.Ctx).Msgf("%s is not an float64, but %T, value: %v. Using default", fieldName, valueRaw, valueRaw)
	}
	return defaultValue
}

// parseFieldFieldMaybeScript is basically almost a copy of parseFieldField above, but it also handles a basic script, if "field" is missing.
func (cw *ClickhouseQueryTranslator) parseFieldFieldMaybeScript(shouldBeMap any, aggregationType string) (field model.Expr, isFromScript bool) {
	Map, ok := shouldBeMap.(QueryMap)
	if !ok {
		logger.WarnWithCtx(cw.Ctx).Msgf("%s aggregation is not a map, but %T, value: %v", aggregationType, shouldBeMap, shouldBeMap)
		return
	}
	// maybe "field" field
	if fieldRaw, ok := Map["field"]; ok {
		if field, ok := fieldRaw.(string); ok {
			return model.NewColumnRef(cw.Table.ResolveField(cw.Ctx, field)), true // remove this resolve? we do all transforms after parsing is done?
		} else {
			logger.WarnWithCtx(cw.Ctx).Msgf("field is not a string, but %T, value: %v", fieldRaw, fieldRaw)
		}
	}

	// else: maybe script
	if field, isFromScript = cw.parseFieldFromScriptField(Map); !isFromScript {
		logger.WarnWithCtx(cw.Ctx).Msgf("field not found in %s aggregation: %v", aggregationType, Map)
	}
	return
}

// parseFieldFromScriptField returns (field, true) if parsing succeeded, (model.SelectColumn{}, false) otherwise.
func (cw *ClickhouseQueryTranslator) parseFieldFromScriptField(queryMap QueryMap) (field model.Expr, success bool) {
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
		return model.NewFunction("toHour", model.NewColumnRef(matches[1])), true
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
