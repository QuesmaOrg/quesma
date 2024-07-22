// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package queryparser

import (
	"fmt"
	"github.com/barkimedes/go-deepcopy"
	"quesma/logger"
	"quesma/model"
	"quesma/model/bucket_aggregations"
	"quesma/model/metrics_aggregations"
	"quesma/quesma/types"
)

type aggregationLevelVersionUna struct {
	Aggregator      model.Aggregator
	Type            model.QueryType
	Children        []*aggregationLevelVersionUna
	SelectedColumns []model.Expr
	OrderBy         *model.OrderByExpr
	Limit           int // 0 if none, only for bucket aggregation
}

// Here is experimental code to generate aggregations in one SQL query. called Version Una.
func (cw *ClickhouseQueryTranslator) ParseAggregationJsonVersionUna(body types.JSON) ([]*model.Query, error) {
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
			subAggregations, err := cw.parseAggregationNamesVersionUna(&currentAggr, aggs)
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

func (cw *ClickhouseQueryTranslator) parseAggregationNamesVersionUna(currentAggr *aggrQueryBuilder, aggs QueryMap) ([]*model.Query, error) {
	aggregationQueries := make([]*model.Query, 0)

	for aggrName, aggrDict := range aggs {
		aggregators := currentAggr.Aggregators
		currentAggr.Aggregators = append(aggregators, model.NewAggregator(aggrName))
		if subAggregation, ok := aggrDict.(QueryMap); ok {
			subAggregations, err := cw.parseAggregationVersionUna(currentAggr, subAggregation)
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

func (cw *ClickhouseQueryTranslator) parseAggregationVersionUna(prevAggr *aggrQueryBuilder, queryMap QueryMap) ([]*model.Query, error) {
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
		subAggregations, err := cw.processRangeAggregationVersionUna(&currentAggr, Range, queryMap, metadata)
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
		subAggregations, err := cw.processFiltersAggregationVersionUna(&currentAggr, filters, queryMap)
		if err != nil {
			return aggregationQueries, err
		}
		aggregationQueries = append(aggregationQueries, subAggregations...)
	}

	aggsHandledSeparately := isRange || isFilters
	if aggs, ok := queryMap["aggs"]; ok && !aggsHandledSeparately {
		subAggregations, err := cw.parseAggregationNamesVersionUna(&currentAggr, aggs.(QueryMap))
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

func (cw *ClickhouseQueryTranslator) processRangeAggregationVersionUna(currentAggr *aggrQueryBuilder, Range bucket_aggregations.Range,
	queryCurrentLevel QueryMap, metadata JsonMap) ([]*model.Query, error) {
	aggregationQueries := make([]*model.Query, 0)

	// build this aggregation
	for _, interval := range Range.Intervals {
		stmt := Range.Expr
		currentAggr.SelectCommand.Columns = append(currentAggr.SelectCommand.Columns, interval.ToSQLSelectQuery(stmt))
	}
	if !Range.Keyed {
		// there's a difference in output structure whether the range is keyed or not
		// it can be easily modeled in our code via setting last aggregator's .Empty to true/false
		if len(currentAggr.Aggregators) > 0 {
			currentAggr.Aggregators[len(currentAggr.Aggregators)-1].SplitOverHowManyFields = 1
		} else {
			logger.ErrorWithCtx(cw.Ctx).Msg("no aggregators in currentAggr")
		}
	}
	aggregationQueries = append(aggregationQueries, currentAggr.buildBucketAggregation(metadata))
	currentAggr.SelectCommand.Columns = currentAggr.SelectCommand.Columns[:len(currentAggr.SelectCommand.Columns)-len(Range.Intervals)]

	// build subaggregations
	aggs, hasAggs := queryCurrentLevel["aggs"].(QueryMap)
	if !hasAggs {
		return aggregationQueries, nil
	}
	// TODO now we run a separate query for each range.
	// it's much easier to code it this way, but that can, quite easily, be improved.
	// Range aggregation with subaggregations should be a quite rare case, so I'm leaving that for later.
	whereBeforeNesting := currentAggr.whereBuilder
	for _, interval := range Range.Intervals {
		stmt := Range.Expr
		currentAggr.whereBuilder = model.CombineWheres(
			cw.Ctx, currentAggr.whereBuilder,
			model.NewSimpleQuery(interval.ToWhereClause(stmt), true),
		)
		currentAggr.Aggregators = append(currentAggr.Aggregators, model.NewAggregator(interval.String()))
		aggsCopy, err := deepcopy.Anything(aggs)
		if err == nil {
			currentAggr.Type = model.NewUnknownAggregationType(cw.Ctx)
			subAggregations, err := cw.parseAggregationNamesVersionUna(currentAggr, aggsCopy.(QueryMap))
			if err != nil {
				return aggregationQueries, err
			}
			aggregationQueries = append(aggregationQueries, subAggregations...)
		} else {
			logger.ErrorWithCtx(cw.Ctx).Msgf("deepcopy 'aggs' map error: %v. Skipping current range's interval: %v, aggs: %v", err, interval, aggs)
		}
		currentAggr.Aggregators = currentAggr.Aggregators[:len(currentAggr.Aggregators)-1]
		currentAggr.whereBuilder = whereBeforeNesting
	}
	return aggregationQueries, nil
}

func (cw *ClickhouseQueryTranslator) processFiltersAggregationVersionUna(aggrBuilder *aggrQueryBuilder,
	aggr bucket_aggregations.Filters, queryMap QueryMap) ([]*model.Query, error) {
	aggregationQueries := make([]*model.Query, 0)

	whereBeforeNesting := aggrBuilder.whereBuilder
	aggrBuilder.Aggregators[len(aggrBuilder.Aggregators)-1].Filters = true
	for _, filter := range aggr.Filters {
		// newBuilder := aggrBuilder.clone()
		// newBuilder.Type = bucket_aggregations.NewFilters(cw.Ctx, []bucket_aggregations.Filter{filter})
		// newBuilder.whereBuilder.CombineWheresWith(filter.Sql)
		// newBuilder.Aggregators = append(aggrBuilder.Aggregators, model.NewAggregatorEmpty(filter.Name))
		aggrBuilder.Type = aggr
		aggrBuilder.whereBuilder = model.CombineWheres(cw.Ctx, aggrBuilder.whereBuilder, filter.Sql)
		aggrBuilder.Aggregators = append(aggrBuilder.Aggregators, model.NewAggregator(filter.Name))
		aggregationQueries = append(aggregationQueries, aggrBuilder.buildBucketAggregation(nil)) // nil for now, will be changed
		if aggs, ok := queryMap["aggs"].(QueryMap); ok {
			aggsCopy, errAggs := deepcopy.Anything(aggs)
			if errAggs == nil {
				//err := cw.parseAggregationNames(newBuilder, aggsCopy.(QueryMap), resultAccumulator)
				subAggregations, err := cw.parseAggregationNamesVersionUna(aggrBuilder, aggsCopy.(QueryMap))
				if err != nil {
					return aggregationQueries, err
				}
				aggregationQueries = append(aggregationQueries, subAggregations...)
			} else {
				logger.ErrorWithCtx(cw.Ctx).Msgf("deepcopy 'aggs' map error: %v. Skipping. aggs: %v", errAggs, aggs)
			}
		}
		aggrBuilder.Aggregators = aggrBuilder.Aggregators[:len(aggrBuilder.Aggregators)-1]
		aggrBuilder.whereBuilder = whereBeforeNesting
	}
	delete(queryMap, "filters")
	return aggregationQueries, nil
}
