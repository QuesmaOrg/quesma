// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package queryparser

import (
	"context"
	"errors"
	"quesma/logger"
	"quesma/model"
	"quesma/model/bucket_aggregations"
	"quesma/quesma/types"
)

type aggregationLevelVersionUna struct {
	Aggregator      model.Aggregator
	Type            model.QueryType
	Children        []*aggregationLevelVersionUna
	SelectedColumns []model.Expr
	OrderBy         *[]model.OrderByExpr
	Limit           int // 0 if none, only for bucket aggregation

	metadata     model.JsonMap
	whereBuilder model.SimpleQuery
	ctx          context.Context
}

// Here is experimental code to generate aggregations in one SQL query. called Version Una.
func (cw *ClickhouseQueryTranslator) ParseAggregationJsonVersionUna(body types.JSON) ([]*model.Query, error) {
	queryAsMap := body.Clone()

	topLevel := aggregationLevelVersionUna{
		Aggregator: model.NewAggregator(""),
		Children:   []*aggregationLevelVersionUna{},
		ctx:        cw.Ctx,
	}

	if queryPartRaw, ok := queryAsMap["query"]; ok {
		if queryPart, ok := queryPartRaw.(QueryMap); ok {
			topLevel.whereBuilder = cw.parseQueryMap(queryPart)
		} else {
			logger.WarnWithCtx(cw.Ctx).Msgf("query is not a map, but %T, query: %v. Skipping", queryPartRaw, queryPartRaw)
		}
	}

	if aggsRaw, ok := queryAsMap["aggs"]; ok {
		if aggs, okType := aggsRaw.(QueryMap); okType {
			subAggregations, err := cw.parseAggregationNamesVersionUna(aggs)
			if err != nil {
				return nil, err
			}
			topLevel.Children = subAggregations
		} else {
			logger.WarnWithCtx(cw.Ctx).Msgf("aggs is not a map, but %T, aggs: %v", aggsRaw, aggsRaw)
		}
	}

	// TODO: Implement topLevel to SQL conversion
	aggregationQueries := make([]*model.Query, 0)
	// currentAggr.SelectCommand.FromClause = model.NewTableRef(cw.Table.FullTableName())
	// currentAggr.TableName = cw.Table.FullTableName()
	// currentAggr.ctx = cw.Ctx

	return aggregationQueries, nil
}

func (cw *ClickhouseQueryTranslator) parseAggregationNamesVersionUna(aggs QueryMap) ([]*aggregationLevelVersionUna, error) {
	aggregationLevels := make([]*aggregationLevelVersionUna, 0)

	for aggrName, aggrDict := range aggs {
		if subAggregation, ok := aggrDict.(QueryMap); ok {
			subLevel, err := cw.parseAggregationVersionUna(aggrName, subAggregation)
			if err != nil {
				return aggregationLevels, err
			}
			aggregationLevels = append(aggregationLevels, subLevel)
		} else {
			logger.ErrorWithCtxAndReason(cw.Ctx, logger.ReasonUnsupportedQuery("unexpected_type")).
				Msgf("unexpected type of subaggregation: (%v: %v), value type: %T. Skipping", aggrName, aggrDict, aggrDict)
		}
	}
	return aggregationLevels, nil
}

func (cw *ClickhouseQueryTranslator) parseAggregationVersionUna(aggregationName string, queryMap QueryMap) (*aggregationLevelVersionUna, error) {
	if len(queryMap) == 0 {
		return nil, nil
	}

	// check if metadata's present
	var metadata model.JsonMap
	if metaRaw, exists := queryMap["meta"]; exists {
		metadata = metaRaw.(model.JsonMap)
		delete(queryMap, "meta")
	} else {
		metadata = model.NoMetadataField
	}

	aggregation := &aggregationLevelVersionUna{
		Aggregator: model.NewAggregator(aggregationName),
		metadata:   metadata,
		ctx:        cw.Ctx,
	}

	// 1. Metrics aggregation => always leaf
	if metricsAggrResult, isMetrics := cw.tryMetricsAggregation(queryMap); isMetrics {
		err := aggregation.buildMetricsAggregation(metricsAggrResult)
		if err != nil {
			return nil, err
		}
		return aggregation, nil
	}

	// 2. Pipeline aggregation => always leaf (for now)
	_, isPipelineAggregation := cw.parsePipelineAggregations(queryMap)
	if isPipelineAggregation {
		return nil, errors.New("pipeline aggregations are not supported in version uno")
	}

	// 3. Now process filter(s) first, because they apply to everything else on the same level or below.
	// Also filter introduces count to current level.
	if _, ok := queryMap["filter"]; ok {
		return nil, errors.New("filter is not supported in version uno")
		delete(queryMap, "filter")
	}

	// 4. Bucket aggregations. They introduce new subaggregations, even if no explicit subaggregation defined on this level.
	bucketAggrPresent, err := cw.tryBucketAggregationVersionUna(aggregation, queryMap)
	if err != nil {
		return nil, err
	}

	// process "range" with subaggregations
	_, isRange := aggregation.Type.(bucket_aggregations.Range)
	if isRange {
		// see processRangeAggregation for details how to implement it
		return nil, errors.New("range is not supported in version uno")
	}

	_, isTerms := aggregation.Type.(bucket_aggregations.Terms)
	if isTerms {
		// No-op for now
	}

	// TODO what happens if there's all: filters, range, and subaggregations at current level?
	// We probably need to do |ranges| * |filters| * |subaggregations| queries, but we don't do that yet.
	// Or probably a bit less, if optimized correctly.
	// Let's wait until we see such a query, maybe range and filters are mutually exclusive.

	_, isFilters := aggregation.Type.(bucket_aggregations.Filters)
	if isFilters {
		return nil, errors.New("filters are not supported in version uno")
	}

	aggsHandledSeparately := isRange || isFilters
	if aggs, ok := queryMap["aggs"]; ok && !aggsHandledSeparately {
		subAggregations, err := cw.parseAggregationNamesVersionUna(aggs.(QueryMap))
		if err != nil {
			return aggregation, err
		}
		aggregation.Children = subAggregations
	}
	delete(queryMap, "aggs") // no-op if no "aggs"

	if bucketAggrPresent && !aggsHandledSeparately && !isTerms {
		// No-op for now
	}

	for k, v := range queryMap {
		// should be empty by now. If it's not, it's an unsupported/unrecognized type of aggregation.
		logger.ErrorWithCtxAndReason(cw.Ctx, logger.ReasonUnsupportedQuery(k)).
			Msgf("unexpected type of subaggregation: (%v: %v), value type: %T. Skipping", k, v, v)
	}

	return aggregation, nil
}
