// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package queryparser

import (
	"errors"
	"quesma/logger"
	"quesma/model"
	"quesma/model/bucket_aggregations"
	"quesma/model/typical_queries"
	"quesma/quesma/types"
)

const PancakeOptimizerName = "pancake"

// Here is experimental code to generate aggregations in one SQL query. called Version Una.
func (cw *ClickhouseQueryTranslator) PancakeParseAggregationJson(body types.JSON, addCount bool) ([]*model.Query, error) {
	queryAsMap := body.Clone()

	topLevel := pancakeAggregationTopLevel{
		children: []*pancakeAggregationLevel{},
	}

	if queryPartRaw, ok := queryAsMap["query"]; ok {
		if queryPart, ok := queryPartRaw.(QueryMap); ok {
			simpleQuery := cw.parseQueryMap(queryPart)
			if simpleQuery.CanParse {
				topLevel.whereClause = simpleQuery.WhereClause
			} else {
				return nil, errors.New("cannot parse query")
			}
		} else {
			logger.WarnWithCtx(cw.Ctx).Msgf("query is not a map, but %T, query: %v. Skipping", queryPartRaw, queryPartRaw)
		}
	}

	if aggsRaw, ok := queryAsMap["aggs"]; ok {
		if aggs, okType := aggsRaw.(QueryMap); okType {
			subAggregations, err := cw.pancakeParseAggregationNames(aggs)
			if err != nil {
				return nil, err
			}
			topLevel.children = subAggregations
		} else {
			logger.WarnWithCtx(cw.Ctx).Msgf("aggs is not a map, but %T, aggs: %v", aggsRaw, aggsRaw)
		}
	}

	pancakeQueries, err := pancakeTranslateFromAggregationToLayered(topLevel)

	if addCount {

		// use our building blocks to add count
		augmentedCountAggregation := &pancakeFillingMetricAggregation{
			name:            PancakeTotalCountMetricName,
			queryType:       typical_queries.Count{},
			selectedColumns: []model.Expr{model.NewFunction("count", model.NewLiteral("*"))},
		}

		pancakeQueries.layers[0].currentMetricAggregations = append(pancakeQueries.layers[0].currentMetricAggregations, augmentedCountAggregation)

	}

	if err != nil {
		return nil, err
	}
	dbQuery, err := pancakeGenerateQuery(pancakeQueries, cw.Table)
	if err != nil {
		return nil, err
	}

	aggregationQueries := make([]*model.Query, 0)
	aggregationQueries = append(aggregationQueries, dbQuery)

	return aggregationQueries, nil
}

func (cw *ClickhouseQueryTranslator) pancakeParseAggregationNames(aggs QueryMap) ([]*pancakeAggregationLevel, error) {
	aggregationLevels := make([]*pancakeAggregationLevel, 0)

	for aggrName, aggrDict := range aggs {
		if subAggregation, ok := aggrDict.(QueryMap); ok {
			subLevel, err := cw.pancakeParseAggregation(aggrName, subAggregation)
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

func (cw *ClickhouseQueryTranslator) pancakeParseAggregation(aggregationName string, queryMap QueryMap) (*pancakeAggregationLevel, error) {
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

	aggregation := &pancakeAggregationLevel{
		name:     aggregationName,
		metadata: metadata,
	}

	// 1. Metrics aggregation => always leaf
	if metricsAggrResult, isMetrics := cw.tryMetricsAggregation(queryMap); isMetrics {
		columns, err := generateMetricSelectedColumns(cw.Ctx, metricsAggrResult)
		if err != nil {
			return nil, err
		}
		aggregation.selectedColumns = columns
		aggregation.queryType = generateMetricsType(cw.Ctx, metricsAggrResult)
		if aggregation.queryType == nil { // Should never happen, we should hit earlier error
			return nil, errors.New("unknown metrics aggregation")
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
		delete(queryMap, "filter")
		return nil, errors.New("filter is not supported in version uno")
	}

	// 4. Bucket aggregations. They introduce new subaggregations, even if no explicit subaggregation defined on this level.
	// 	bucketAggrPresent, err := cw.pancakeTryBucketAggregation(aggregation, queryMap)
	_, err := cw.pancakeTryBucketAggregation(aggregation, queryMap)
	if err != nil {
		return nil, err
	}

	// process "range" with subaggregations
	_, isRange := aggregation.queryType.(bucket_aggregations.Range)
	if isRange {
		// see processRangeAggregation for details how to implement it
		return nil, errors.New("range is not supported in version uno")
	}

	// _, isTerms := aggregation.queryType.(bucket_aggregations.Terms)
	// if isTerms {
	// No-op for now
	//}

	// TODO what happens if there's all: filters, range, and subaggregations at current level?
	// We probably need to do |ranges| * |filters| * |subaggregations| queries, but we don't do that yet.
	// Or probably a bit less, if optimized correctly.
	// Let's wait until we see such a query, maybe range and filters are mutually exclusive.

	_, isFilters := aggregation.queryType.(bucket_aggregations.Filters)
	if isFilters {
		return nil, errors.New("filters are not supported in version uno")
	}

	aggsHandledSeparately := isRange || isFilters
	if aggs, ok := queryMap["aggs"]; ok && !aggsHandledSeparately {
		subAggregations, err := cw.pancakeParseAggregationNames(aggs.(QueryMap))
		if err != nil {
			return aggregation, err
		}
		aggregation.children = subAggregations
	}
	delete(queryMap, "aggs") // no-op if no "aggs"

	// if bucketAggrPresent && !aggsHandledSeparately && !isTerms {
	// No-op for now
	// }

	for k, v := range queryMap {
		// should be empty by now. If it's not, it's an unsupported/unrecognized type of aggregation.
		logger.ErrorWithCtxAndReason(cw.Ctx, logger.ReasonUnsupportedQuery(k)).
			Msgf("unexpected type of subaggregation: (%v: %v), value type: %T. Skipping", k, v, v)
	}

	return aggregation, nil
}
