// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package queryparser

import (
	"context"
	"fmt"
	"quesma/clickhouse"
	"quesma/logger"
	"quesma/model"
	"quesma/model/bucket_aggregations"
	"quesma/model/metrics_aggregations"

	"quesma/quesma/types"
	"quesma/util"
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

func isColumnExist(columns []model.Expr, columnName string) bool {
	for _, column := range columns {
		if model.AsString(column) == fmt.Sprintf("%q", columnName) {
			return true
		}
	}
	return false
}

// updateInnerQueryColumns adds columns that exists in where clause and are missing
// in select clause
func updateInnerQueryColumns(query model.SelectCommand, whereClause model.Expr) model.SelectCommand {

	columns := model.GetUsedColumns(whereClause)

	for _, column := range columns {
		if isColumnExist(query.Columns, column.ColumnName) {
			continue
		}
		query.Columns = append(query.Columns, column)
	}
	return query
}

func (b *aggrQueryBuilder) buildAggregationCommon(metadata model.JsonMap) *model.Query {
	query := b.Query
	query.SelectCommand.WhereClause = b.whereBuilder.WhereClause

	// Need to copy, as we might be proceeding to modify 'b' pointer
	query.CopyAggregationFields(b.Query)

	query.Metadata = metadata
	return &query
}

func (b *aggrQueryBuilder) buildCountAggregation(metadata model.JsonMap) *model.Query {
	query := b.buildAggregationCommon(metadata)
	query.Type = metrics_aggregations.NewCount(b.ctx)

	query.SelectCommand.Columns = append(query.SelectCommand.Columns, model.NewCountFunc())
	return query
}

func (b *aggrQueryBuilder) buildBucketAggregation(metadata model.JsonMap) *model.Query {
	query := b.buildAggregationCommon(metadata)

	query.SelectCommand.Columns = append(query.SelectCommand.Columns, model.NewCountFunc())
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
		query.SelectCommand.Columns = append(query.SelectCommand.Columns, model.NewFunction(metricsAggr.AggrType+"OrNull", getFirstExpression()))
	case "quantile":
		// Sorting here useful mostly for determinism in tests.
		// It wasn't there before, and everything worked fine. We could safely remove it, if needed.
		usersPercents := util.MapKeysSortedByValue(metricsAggr.Percentiles)
		for _, usersPercent := range usersPercents {
			percentAsFloat := metricsAggr.Percentiles[usersPercent]
			query.SelectCommand.Columns = append(query.SelectCommand.Columns, model.NewAliasedExpr(
				model.MultiFunctionExpr{
					Name: "quantiles",
					Args: []model.Expr{model.NewLiteral(percentAsFloat), getFirstExpression()}},
				fmt.Sprintf("quantile_%s", usersPercent),
			))

		}
	case "cardinality":
		query.SelectCommand.Columns = append(query.SelectCommand.Columns, model.NewCountFunc(model.NewDistinctExpr(getFirstExpression())))

	case "value_count":
		query.SelectCommand.Columns = append(query.SelectCommand.Columns, model.NewCountFunc())

	case "stats":
		expr := getFirstExpression()

		query.SelectCommand.Columns = append(query.SelectCommand.Columns, model.NewCountFunc(expr),
			model.NewFunction("minOrNull", expr),
			model.NewFunction("maxOrNull", expr),
			model.NewFunction("avgOrNull", expr),
			model.NewFunction("sumOrNull", expr))

	case "top_hits":
		// TODO add/restore tests for top_hits. E.g. we missed WHERE in FROM below, so the SQL might not be correct
		innerFieldsAsSelect := make([]model.Expr, len(metricsAggr.Fields))
		copy(innerFieldsAsSelect, metricsAggr.Fields)
		query.SelectCommand.Columns = append(query.SelectCommand.Columns, innerFieldsAsSelect...)
		/*
			query.SelectCommand.FromClause = fmt.Sprintf(
				"(SELECT %s, ROW_NUMBER() OVER (PARTITION BY %s) AS %s FROM %s%s)",
				metricsAggr.Fields, partitionBy, model.RowNumberColumnName, query.SelectCommand.FromClause, whereString,
			)
		*/
		query.SelectCommand.FromClause = query.NewSelectExprWithRowNumber(
			query.SelectCommand.Columns, b.SelectCommand.GroupBy, b.whereBuilder.WhereClause, "", true)
		query.SelectCommand.WhereClause = model.And([]model.Expr{
			query.SelectCommand.WhereClause,
			model.NewInfixExpr(
				model.NewColumnRef(model.RowNumberColumnName),
				"<=",
				model.NewLiteral(strconv.Itoa(metricsAggr.Size)),
			)},
		)
		query.SelectCommand.GroupBy = append(query.SelectCommand.GroupBy, innerFieldsAsSelect...)
	case "top_metrics":
		// This appending of `metricsAggr.SortBy` and having it duplicated in SELECT block
		// is a way to pass value we're sorting by to the query.SelectCommand.result. In the future we might add SQL aliasing support, e.g. SELECT x AS 'sort_by' FROM ...
		if len(b.Query.SelectCommand.GroupBy) > 0 {
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
				query.SelectCommand.Columns = append(query.SelectCommand.Columns,
					model.NewAliasedExpr(model.NewFunction(ordFunc, field), fmt.Sprintf("windowed_%s", fieldName)))
			}

			innerFieldsAsSelect := make([]model.Expr, len(innerFields))
			copy(innerFieldsAsSelect, innerFields)
			query.SelectCommand.FromClause = query.NewSelectExprWithRowNumber(
				innerFieldsAsSelect, b.Query.SelectCommand.GroupBy, b.whereBuilder.WhereClause,
				metricsAggr.SortBy, strings.ToLower(metricsAggr.Order) == "desc",
			)
			// where clause is built from filters aggregation,
			// and it can contain columns that are not in the select clause,
			// so we need to add them to the select clause
			// as they are used in outer query
			// For now that kind of local fix, but this can be done in a more general way
			// by step that will check semantic correctness of the query
			// and do necessary transformations
			query.SelectCommand.FromClause = updateInnerQueryColumns(query.SelectCommand.FromClause.(model.SelectCommand),
				query.SelectCommand.FromClause.(model.SelectCommand).WhereClause)

			query.SelectCommand.WhereClause = model.And([]model.Expr{query.SelectCommand.WhereClause,
				model.NewInfixExpr(model.NewColumnRef(model.RowNumberColumnName), "<=", model.NewLiteral(strconv.Itoa(metricsAggr.Size)))})
		} else {
			innerFieldsAsSelect := make([]model.Expr, len(metricsAggr.Fields))
			copy(innerFieldsAsSelect, metricsAggr.Fields)
			query.SelectCommand.Limit = metricsAggr.Size
			query.SelectCommand.Columns = append(query.SelectCommand.Columns, innerFieldsAsSelect...)
			if metricsAggr.sortByExists() {
				query.SelectCommand.Columns = append(query.SelectCommand.Columns, model.NewColumnRef(metricsAggr.SortBy))
				if strings.ToLower(metricsAggr.Order) == "desc" {
					query.SelectCommand.OrderBy = append(query.SelectCommand.OrderBy, model.NewSortColumn(metricsAggr.SortBy, model.DescOrder))
				} else {
					query.SelectCommand.OrderBy = append(query.SelectCommand.OrderBy, model.NewSortColumn(metricsAggr.SortBy, model.AscOrder))
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

			query.SelectCommand.Columns = append(query.SelectCommand.Columns, model.NewInfixExpr(twoCountsExp, "*", model.NewLiteral(100)))
		}
	case "extended_stats":

		expr := getFirstExpression()

		// add column with fn applied to field
		addColumn := func(funcName string) {
			query.SelectCommand.Columns = append(query.SelectCommand.Columns, model.NewFunction(funcName, expr))
		}

		addColumn("count")
		addColumn("minOrNull")
		addColumn("maxOrNull")
		addColumn("avgOrNull")
		addColumn("sumOrNull")

		query.SelectCommand.Columns = append(query.SelectCommand.Columns, model.NewFunction("sumOrNull", model.NewInfixExpr(expr, "*", expr)))

		addColumn("varPop")
		addColumn("varSamp")
		addColumn("stddevPop")
		addColumn("stddevSamp")
	case "geo_centroid":
		firstExpr := getFirstExpression()
		if col, ok := firstExpr.(model.ColumnRef); ok {
			colName := col.ColumnName
			// TODO we have create columns according to the schema
			latColumn := model.NewColumnRef(colName + "::lat")
			lonColumn := model.NewColumnRef(colName + "::lon")
			castLat := model.NewFunction("CAST", latColumn, model.NewLiteral(fmt.Sprintf("'%s'", "Float")))
			castLon := model.NewFunction("CAST", lonColumn, model.NewLiteral(fmt.Sprintf("'%s'", "Float")))
			query.SelectCommand.Columns = append(query.SelectCommand.Columns, model.NewFunction("avgOrNull", castLat))
			query.SelectCommand.Columns = append(query.SelectCommand.Columns, model.NewFunction("avgOrNull", castLon))
			query.SelectCommand.Columns = append(query.SelectCommand.Columns, model.NewFunction("count"))
		}
	default:
		logger.WarnWithCtx(b.ctx).Msgf("unknown metrics aggregation: %s", metricsAggr.AggrType)
		return nil
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
		query.Type = metrics_aggregations.NewQuantile(b.ctx, util.MapKeysSortedByValue(metricsAggr.Percentiles), metricsAggr.Keyed, metricsAggr.FieldType)
	case "top_hits":
		query.Type = metrics_aggregations.NewTopHits(b.ctx)
	case "top_metrics":
		query.Type = metrics_aggregations.NewTopMetrics(b.ctx, metricsAggr.sortByExists())
	case "value_count":
		query.Type = metrics_aggregations.NewValueCount(b.ctx)
	case "percentile_ranks":
		query.Type = metrics_aggregations.NewPercentileRanks(b.ctx, metricsAggr.Keyed)
	case "geo_centroid":
		query.Type = metrics_aggregations.NewGeoCentroid(b.ctx)
	}
	return query
}

// ParseAggregationJson parses JSON with aggregation query and returns array of queries with aggregations.
// If there are no aggregations, returns nil.
func (cw *ClickhouseQueryTranslator) ParseAggregationJson(body types.JSON) ([]*model.Query, error) {
	queryAsMap := body.Clone()
	currentAggr := aggrQueryBuilder{}
	currentAggr.SelectCommand.FromClause = model.NewTableRef(cw.Table.FullTableName())
	currentAggr.TableName = cw.Table.FullTableName()
	currentAggr.ctx = cw.Ctx
	if queryPartRaw, ok := queryAsMap["query"]; ok {
		if queryPart, ok := queryPartRaw.(QueryMap); ok {
			currentAggr.whereBuilder = cw.parseQueryMap(queryPart)
		} else {
			logger.WarnWithCtx(cw.Ctx).Msgf("query is not a map, but %T, query: %v. Skipping", queryPartRaw, queryPartRaw)
		}
	}

	aggregationQueries := make([]*model.Query, 0)

	if aggsRaw, ok := queryAsMap["aggs"]; ok {
		if aggs, okType := aggsRaw.(QueryMap); okType {
			subAggregations, err := cw.parseAggregationNames(&currentAggr, aggs)
			if err != nil {
				return aggregationQueries, err
			}
			aggregationQueries = append(aggregationQueries, subAggregations...)
		} else {
			logger.WarnWithCtx(cw.Ctx).Msgf("aggs is not a map, but %T, aggs: %v", aggsRaw, aggsRaw)
		}
	}

	return aggregationQueries, nil
}

// 'resultQueries' - array when we store results
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

func (cw *ClickhouseQueryTranslator) parseAggregationNames(currentAggr *aggrQueryBuilder, aggs QueryMap) ([]*model.Query, error) {
	aggregationQueries := make([]*model.Query, 0)

	for aggrName, aggrDict := range aggs {
		aggregators := currentAggr.Aggregators
		currentAggr.Aggregators = append(aggregators, model.NewAggregator(aggrName))
		if subAggregation, ok := aggrDict.(QueryMap); ok {
			subAggregations, err := cw.parseAggregation(currentAggr, subAggregation)
			if err != nil {
				return aggregationQueries, err
			}
			aggregationQueries = append(aggregationQueries, subAggregations...)
		} else {
			logger.ErrorWithCtxAndReason(cw.Ctx, logger.ReasonUnsupportedQuery("unexpected_type")).
				Msgf("unexpected type of subaggregation: (%v: %v), value type: %T. Skipping", aggrName, aggrDict, aggrDict)
		}
		currentAggr.Aggregators = aggregators
	}
	return aggregationQueries, nil
}

// Builds aggregations recursively. Seems to be working on all examples so far,
// even though it's a pretty simple algorithm.
// When making changes, look at the order in which we parse fields, it is very important for correctness.
//
// 'resultQueries' - array when we store results
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
func (cw *ClickhouseQueryTranslator) parseAggregation(prevAggr *aggrQueryBuilder, queryMap QueryMap) ([]*model.Query, error) {
	aggregationQueries := make([]*model.Query, 0)

	if len(queryMap) == 0 {
		return aggregationQueries, nil
	}

	currentAggr := *prevAggr
	currentAggr.SelectCommand.Limit = 0

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
		metricAggr := currentAggr.buildMetricsAggregation(metricsAggrResult, metadata)
		if metricAggr != nil {
			aggregationQueries = append(aggregationQueries, metricAggr)
		}
		return aggregationQueries, nil
	}

	// 2. Pipeline aggregation => always leaf (for now)
	pipelineAggregationType, isPipelineAggregation := cw.parsePipelineAggregations(queryMap)
	if isPipelineAggregation {
		aggregationQueries = append(aggregationQueries, currentAggr.finishBuildingAggregationPipeline(pipelineAggregationType, metadata))
	}

	// 3. Now process filter(s) first, because they apply to everything else on the same level or below.
	// Also filter introduces count to current level.
	if filterRaw, ok := queryMap["filter"]; ok {
		if filter, ok := filterRaw.(QueryMap); ok {
			currentAggr.Type = metrics_aggregations.NewCount(cw.Ctx)
			currentAggr.whereBuilder = model.CombineWheres(cw.Ctx, currentAggr.whereBuilder, cw.parseQueryMap(filter))
			aggregationQueries = append(aggregationQueries, currentAggr.buildCountAggregation(metadata))
		} else {
			logger.WarnWithCtx(cw.Ctx).Msgf("filter is not a map, but %T, value: %v. Skipping", filterRaw, filterRaw)
		}
		delete(queryMap, "filter")
	}

	// 4. Bucket aggregations. They introduce new subaggregations, even if no explicit subaggregation defined on this level.
	bucketAggrPresent, groupByFieldsAdded, err := cw.tryBucketAggregation(&currentAggr, queryMap)
	if err != nil {
		return aggregationQueries, err
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
		subAggregations, err := cw.processRangeAggregation(&currentAggr, Range, queryMap, metadata)
		if err != nil {
			return aggregationQueries, err
		}
		aggregationQueries = append(aggregationQueries, subAggregations...)
	}

	terms, isTerms := currentAggr.Type.(bucket_aggregations.Terms)
	if isTerms {
		aggregationQueries = append(aggregationQueries, currentAggr.buildBucketAggregation(metadata))
		cte := currentAggr.Query
		cte.CopyAggregationFields(currentAggr.Query)
		cte.SelectCommand.WhereClause = currentAggr.whereBuilder.WhereClause
		cte.SelectCommand.Columns = append(cte.SelectCommand.Columns,
			model.NewAliasedExpr(terms.OrderByExpr, fmt.Sprintf("cte_%d_cnt", len(currentAggr.SelectCommand.CTEs)+1))) // FIXME unify this name creation with one in model/expr_as_string
		cte.SelectCommand.CTEs = nil // CTEs don't have CTEs themselves (so far, maybe that'll need to change)
		if len(cte.SelectCommand.OrderBy) > 2 {
			// we can reduce nr of ORDER BYs in CTEs. Last 2 seem to be always enough. Proper ordering is done anyway in the outer SELECT.
			cte.SelectCommand.OrderBy = cte.SelectCommand.OrderBy[len(cte.SelectCommand.OrderBy)-2:]
		}
		currentAggr.SelectCommand.CTEs = append(currentAggr.SelectCommand.CTEs, &cte.SelectCommand)
	}

	// TODO what happens if there's all: filters, range, and subaggregations at current level?
	// We probably need to do |ranges| * |filters| * |subaggregations| queries, but we don't do that yet.
	// Or probably a bit less, if optimized correctly.
	// Let's wait until we see such a query, maybe range and filters are mutually exclusive.

	filters, isFilters := currentAggr.Type.(bucket_aggregations.Filters)
	if isFilters {
		subAggregations, err := cw.processFiltersAggregation(&currentAggr, filters, queryMap)
		if err != nil {
			return aggregationQueries, err
		}
		aggregationQueries = append(aggregationQueries, subAggregations...)
	}

	aggsHandledSeparately := isRange || isFilters
	if aggs, ok := queryMap["aggs"]; ok && !aggsHandledSeparately {
		subAggregations, err := cw.parseAggregationNames(&currentAggr, aggs.(QueryMap))
		if err != nil {
			return aggregationQueries, err
		}
		aggregationQueries = append(aggregationQueries, subAggregations...)
	}
	delete(queryMap, "aggs") // no-op if no "aggs"

	if bucketAggrPresent && !aggsHandledSeparately && !isTerms {
		// range aggregation has separate, optimized handling
		aggregationQueries = append(aggregationQueries, currentAggr.buildBucketAggregation(metadata))
	}

	for k, v := range queryMap {
		// should be empty by now. If it's not, it's an unsupported/unrecognized type of aggregation.
		logger.ErrorWithCtxAndReason(cw.Ctx, logger.ReasonUnsupportedQuery(k)).
			Msgf("unexpected type of subaggregation: (%v: %v), value type: %T. Skipping", k, v, v)
	}

	return aggregationQueries, nil
}

// Tries to parse metrics aggregation from queryMap. If it's not a metrics aggregation, returns false.
func (cw *ClickhouseQueryTranslator) tryMetricsAggregation(queryMap QueryMap) (metricAggregation metricsAggregation, success bool) {
	if len(queryMap) != 1 {
		return metricsAggregation{}, false
	}

	// full list: https://www.elastic.co/guide/en/elasticsearch/reference/current/search-Aggregations-metrics.html
	// shouldn't be hard to handle others, if necessary

	metricsAggregations := []string{"sum", "avg", "min", "max", "cardinality", "value_count", "stats", "geo_centroid"}
	for k, v := range queryMap {
		if slices.Contains(metricsAggregations, k) {
			field, isFromScript := cw.parseFieldFieldMaybeScript(v, k)

			return metricsAggregation{
				AggrType:            k,
				Fields:              []model.Expr{field},
				FieldType:           cw.GetDateTimeTypeFromSelectClause(cw.Ctx, field),
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
			FieldType:   cw.GetDateTimeTypeFromSelectClause(cw.Ctx, field),
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
	success bool, groupByFieldsAdded int, err error) {

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

		currentAggr.SelectCommand.Columns = append(currentAggr.SelectCommand.Columns, col)
		currentAggr.SelectCommand.GroupBy = append(currentAggr.SelectCommand.GroupBy, col)
		currentAggr.SelectCommand.LimitBy = append(currentAggr.SelectCommand.LimitBy, col)
		currentAggr.SelectCommand.OrderBy = append(currentAggr.SelectCommand.OrderBy, model.NewOrderByExprWithoutOrder(col))

		delete(queryMap, "histogram")
		return success, 1, nil
	}
	if dateHistogramRaw, ok := queryMap["date_histogram"]; ok {
		dateHistogram, ok := dateHistogramRaw.(QueryMap)
		if !ok {
			logger.WarnWithCtx(cw.Ctx).Msgf("date_histogram is not a map, but %T, value: %v", dateHistogramRaw, dateHistogramRaw)
		}
		field := cw.parseFieldField(dateHistogram, "date_histogram")
		minDocCount := cw.parseMinDocCount(dateHistogram)
		interval, intervalType := cw.extractInterval(dateHistogram)
		dateTimeType := cw.Table.GetDateTimeTypeFromExpr(cw.Ctx, field)

		if dateTimeType == clickhouse.Invalid {
			logger.WarnWithCtx(cw.Ctx).Msgf("invalid date time type for field %s", field)
		}

		dateHistogramAggr := bucket_aggregations.NewDateHistogram(cw.Ctx, field, interval, minDocCount, intervalType, dateTimeType)
		currentAggr.Type = dateHistogramAggr

		sqlQuery := dateHistogramAggr.GenerateSQL()
		currentAggr.SelectCommand.Columns = append(currentAggr.SelectCommand.Columns, sqlQuery)
		currentAggr.SelectCommand.GroupBy = append(currentAggr.SelectCommand.GroupBy, sqlQuery)
		currentAggr.SelectCommand.LimitBy = append(currentAggr.SelectCommand.LimitBy, sqlQuery)
		currentAggr.SelectCommand.OrderBy = append(currentAggr.SelectCommand.OrderBy, model.NewOrderByExprWithoutOrder(sqlQuery))

		delete(queryMap, "date_histogram")
		return success, 1, nil
	}
	for _, termsType := range []string{"terms", "significant_terms"} {
		termsRaw, ok := queryMap[termsType]
		if !ok {
			continue
		}
		terms, ok := termsRaw.(QueryMap)
		if !ok {
			logger.WarnWithCtx(cw.Ctx).Msgf("%s is not a map, but %T, value: %v", termsType, termsRaw, termsRaw)
			continue
		}

		// Parse 'missing' parameter. It can be any type.
		var missingPlaceholder any
		if terms["missing"] != nil {
			missingPlaceholder = terms["missing"]
		}

		fieldExpression := cw.parseFieldField(terms, termsType)

		// apply missing placeholder if it is set
		if missingPlaceholder != nil {
			var value model.LiteralExpr

			// Maybe we should check the input type against the schema?
			// Right now we quote if it's a string.
			switch val := missingPlaceholder.(type) {
			case string:
				value = model.NewLiteral("'" + val + "'")
			default:
				value = model.NewLiteral(missingPlaceholder)
			}

			fieldExpression = model.NewFunction("COALESCE", fieldExpression, value)
		}

		size := 10
		if sizeRaw, ok := terms["size"]; ok {
			if sizeParsed, ok := sizeRaw.(float64); ok {
				size = int(sizeParsed)
			} else {
				logger.WarnWithCtx(cw.Ctx).Msgf("size is not an float64, but %T, value: %v. Using default", sizeRaw, sizeRaw)
			}
		}

		defaultMainOrderBy := model.NewCountFunc()
		defaultDirection := model.DescOrder

		var mainOrderBy model.Expr = defaultMainOrderBy
		fullOrderBy := []model.OrderByExpr{ // default
			{Exprs: []model.Expr{mainOrderBy}, Direction: defaultDirection, ExchangeToAliasInCTE: true},
			{Exprs: []model.Expr{fieldExpression}},
		}
		direction := defaultDirection
		if orderRaw, exists := terms["order"]; exists {
			if order, ok := orderRaw.(QueryMap); ok { // TODO it can be array too, don't handle it yet
				if len(order) == 1 {
					for key, valueRaw := range order { // value == "asc" or "desc"
						value, ok := valueRaw.(string)
						if !ok {
							logger.WarnWithCtx(cw.Ctx).Msgf("order value is not a string, but %T, value: %v. Using default (desc)", valueRaw, valueRaw)
							value = "desc"
						}
						if strings.ToLower(value) == "asc" {
							direction = model.AscOrder
						}

						if key == "_key" {
							fullOrderBy = []model.OrderByExpr{{Exprs: []model.Expr{fieldExpression}, Direction: direction}}
							break // mainOrderBy remains default
						} else if key != "_count" {
							mainOrderBy = cw.findMetricAggregation(queryMap, key, currentAggr)
						}

						fullOrderBy = []model.OrderByExpr{
							{Exprs: []model.Expr{mainOrderBy}, Direction: direction, ExchangeToAliasInCTE: true},
							{Exprs: []model.Expr{fieldExpression}},
						}
					}
				} else {
					logger.ErrorWithCtx(cw.Ctx).Msgf("order has more than 1 key, but %d. Order: %+v. Using default", len(order), order)
				}
			} else {
				logger.ErrorWithCtx(cw.Ctx).Msgf("order is not a map, but %T, value: %v. Using default order", orderRaw, orderRaw)
			}
		}

		currentAggr.Type = bucket_aggregations.NewTerms(cw.Ctx, termsType == "significant_terms", mainOrderBy)
		currentAggr.SelectCommand.Limit = size
		currentAggr.SelectCommand.Columns = append(currentAggr.SelectCommand.Columns, fieldExpression)
		currentAggr.SelectCommand.GroupBy = append(currentAggr.SelectCommand.GroupBy, fieldExpression)
		currentAggr.SelectCommand.LimitBy = append(currentAggr.SelectCommand.LimitBy, fieldExpression)
		currentAggr.SelectCommand.OrderBy = append(currentAggr.SelectCommand.OrderBy, fullOrderBy...)
		if missingPlaceholder == nil { // TODO replace with schema
			currentAggr.whereBuilder = model.CombineWheres(cw.Ctx, currentAggr.whereBuilder,
				model.NewSimpleQuery(model.NewInfixExpr(fieldExpression, "IS", model.NewLiteral("NOT NULL")), true))
		}

		delete(queryMap, termsType)
		return success, 1, nil
	}
	if multiTermsRaw, exists := queryMap["multi_terms"]; exists {
		multiTerms, ok := multiTermsRaw.(QueryMap)
		if !ok {
			logger.WarnWithCtx(cw.Ctx).Msgf("multi_terms is not a map, but %T, value: %v", multiTermsRaw, multiTermsRaw)
		}

		orderByAdded := false
		isEmptyGroupBy := len(currentAggr.SelectCommand.GroupBy) == 0
		const defaultSize = 10
		size := cw.parseIntField(multiTerms, "size", defaultSize)
		if _, exists := queryMap["aggs"]; isEmptyGroupBy && !exists { // we can do limit only it terms are not nested
			currentAggr.SelectCommand.OrderBy = append(currentAggr.SelectCommand.OrderBy, model.NewSortByCountColumn(model.DescOrder))
			currentAggr.SelectCommand.Limit = size
			orderByAdded = true
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
				currentAggr.SelectCommand.Columns = append(currentAggr.SelectCommand.Columns, column)
				currentAggr.SelectCommand.GroupBy = append(currentAggr.SelectCommand.GroupBy, column)
				if !orderByAdded {
					currentAggr.SelectCommand.OrderBy = append(currentAggr.SelectCommand.OrderBy, model.NewOrderByExprWithoutOrder(column))
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
		return success, fieldsNr, nil
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
		return success, 0, nil
	}
	if dateRangeRaw, ok := queryMap["date_range"]; ok {
		dateRange, ok := dateRangeRaw.(QueryMap)
		if !ok {
			logger.WarnWithCtx(cw.Ctx).Msgf("date_range is not a map, but %T, value: %v. Using empty map", dateRangeRaw, dateRangeRaw)
		}
		dateRangeParsed, err := cw.parseDateRangeAggregation(dateRange)
		if err != nil {
			logger.ErrorWithCtx(cw.Ctx).Err(err).Msg("failed to parse date_range aggregation")
			return false, 0, err
		}
		currentAggr.Type = dateRangeParsed
		for _, interval := range dateRangeParsed.Intervals {

			currentAggr.SelectCommand.Columns = append(currentAggr.SelectCommand.Columns, interval.ToSQLSelectQuery(dateRangeParsed.FieldName))

			if sqlSelect, selectNeeded := interval.BeginTimestampToSQL(); selectNeeded {
				currentAggr.SelectCommand.Columns = append(currentAggr.SelectCommand.Columns, sqlSelect)
			}
			if sqlSelect, selectNeeded := interval.EndTimestampToSQL(); selectNeeded {
				currentAggr.SelectCommand.Columns = append(currentAggr.SelectCommand.Columns, sqlSelect)
			}
		}

		// TODO after https://github.com/QuesmaOrg/quesma/pull/99 it should be only in 1 of 2 cases (keyed or not), just like in range aggregation
		if len(currentAggr.Aggregators) > 0 {
			currentAggr.Aggregators[len(currentAggr.Aggregators)-1].SplitOverHowManyFields = 1
		} else {
			logger.ErrorWithCtx(cw.Ctx).Msg("no aggregators in currentAggr")
		}

		delete(queryMap, "date_range")
		return success, 0, nil
	}
	if geoTileGridRaw, ok := queryMap["geotile_grid"]; ok {
		geoTileGrid, ok := geoTileGridRaw.(QueryMap)
		if !ok {
			logger.WarnWithCtx(cw.Ctx).Msgf("geotile_grid is not a map, but %T, value: %v", geoTileGridRaw, geoTileGridRaw)
		}
		var precision float64
		precisionRaw, ok := geoTileGrid["precision"]
		if ok {
			switch cutValueTyped := precisionRaw.(type) {
			case float64:
				precision = cutValueTyped
			}
		}
		field := cw.parseFieldField(geoTileGrid, "geotile_grid")
		currentAggr.Type = bucket_aggregations.NewGeoTileGrid(cw.Ctx)

		// That's bucket (group by) formula for geotile_grid
		// zoom/x/y
		//	SELECT precision as zoom,
		//	    FLOOR(((toFloat64("Location::lon") + 180.0) / 360.0) * POWER(2, zoom)) AS x_tile,
		//	    FLOOR(
		//	        (
		//	            1 - LOG(TAN(RADIANS(toFloat64("Location::lat"))) + (1 / COS(RADIANS(toFloat64("Location::lat"))))) / PI()
		//	        ) / 2.0 * POWER(2, zoom)
		//	    ) AS y_tile, count()
		//	FROM
		//	     kibana_sample_data_flights Group by zoom, x_tile, y_tile

		// TODO columns names should be created according to the schema
		var lon = model.AsString(field)
		lon = strings.Trim(lon, "\"")
		lon = lon + "::lon"
		var lat = model.AsString(field)
		lat = strings.Trim(lat, "\"")
		lat = lat + "::lat"
		toFloatFunLon := model.NewFunction("toFloat64", model.NewColumnRef(lon))
		var infixX model.Expr
		infixX = model.NewParenExpr(model.NewInfixExpr(toFloatFunLon, "+", model.NewLiteral(180.0)))
		infixX = model.NewParenExpr(model.NewInfixExpr(infixX, "/", model.NewLiteral(360.0)))
		infixX = model.NewInfixExpr(infixX, "*",
			model.NewFunction("POWER", model.NewLiteral(2), model.NewLiteral("zoom")))
		xTile := model.NewAliasedExpr(model.NewFunction("FLOOR", infixX), "x_tile")
		toFloatFunLat := model.NewFunction("toFloat64", model.NewColumnRef(lat))
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
			model.NewFunction("POWER", model.NewLiteral(2), model.NewLiteral("zoom")))
		yTile := model.NewAliasedExpr(
			model.NewFunction("FLOOR", FloorContent), "y_tile")

		currentAggr.SelectCommand.Columns = append(currentAggr.SelectCommand.Columns, model.NewAliasedExpr(model.NewLiteral(precision), "zoom"))
		currentAggr.SelectCommand.Columns = append(currentAggr.SelectCommand.Columns, xTile)
		currentAggr.SelectCommand.Columns = append(currentAggr.SelectCommand.Columns, yTile)

		currentAggr.SelectCommand.GroupBy = append(currentAggr.SelectCommand.GroupBy, model.NewColumnRef("zoom"))
		currentAggr.SelectCommand.GroupBy = append(currentAggr.SelectCommand.GroupBy, model.NewColumnRef("x_tile"))
		currentAggr.SelectCommand.GroupBy = append(currentAggr.SelectCommand.GroupBy, model.NewColumnRef("y_tile"))

		delete(queryMap, "geotile_grid")
		return success, 3, err
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
			return model.NewColumnRef(cw.ResolveField(cw.Ctx, field)) // model.NewSelectColumnTableField(cw.Table.ResolveField(cw.Ctx, field)) // remove this resolve? we do all transforms after parsing is done?
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
			return model.NewColumnRef(cw.ResolveField(cw.Ctx, field)), true // remove this resolve? we do all transforms after parsing is done?
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

func (cw *ClickhouseQueryTranslator) findMetricAggregation(queryMap QueryMap, aggregationName string, currentAggr *aggrQueryBuilder) model.Expr {
	notFoundValue := model.NewLiteral("")
	aggsRaw, exists := queryMap["aggs"]
	if !exists {
		logger.WarnWithCtx(cw.Ctx).Msgf("no aggs in queryMap, queryMap: %+v", queryMap)
		return notFoundValue
	}
	aggs, ok := aggsRaw.(QueryMap)
	if !ok {
		logger.WarnWithCtx(cw.Ctx).Msgf("aggs is not a map, but %T, value: %v. Skipping", aggsRaw, aggsRaw)
		return notFoundValue
	}
	if aggMapRaw, exists := aggs[aggregationName]; exists {
		aggMap, ok := aggMapRaw.(QueryMap)
		if !ok {
			logger.WarnWithCtx(cw.Ctx).Msgf("aggregation %s is not a map, but %T, value: %v. Skipping", aggregationName, aggMapRaw, aggMapRaw)
			return notFoundValue
		}

		agg, success := cw.tryMetricsAggregation(aggMap)
		if !success {
			logger.WarnWithCtx(cw.Ctx).Msgf("failed to parse metric aggregation: %v", agg)
			return notFoundValue
		}

		// we build a temporary query only to extract the name of the metric
		tmpQuery := currentAggr.buildMetricsAggregation(agg, model.NoMetadataField)
		if len(tmpQuery.SelectCommand.Columns) != len(currentAggr.SelectCommand.Columns)+1 {
			logger.WarnWithCtx(cw.Ctx).Msgf("unexpected number of columns in metric aggregation: %d, expected %d",
				len(tmpQuery.SelectCommand.Columns), len(currentAggr.SelectCommand.Columns)+1)
			return notFoundValue
		}
		return tmpQuery.SelectCommand.Columns[len(tmpQuery.SelectCommand.Columns)-1]
	}
	return notFoundValue
}

// quoteArray returns a new array with the same elements, but quoted
func quoteArray(array []string) []string {
	quotedArray := make([]string, 0, len(array))
	for _, el := range array {
		quotedArray = append(quotedArray, strconv.Quote(el))
	}
	return quotedArray
}
