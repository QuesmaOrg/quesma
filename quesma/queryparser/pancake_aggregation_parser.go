// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package queryparser

import (
	"errors"
	"fmt"
	"github.com/QuesmaOrg/quesma/quesma/logger"
	"github.com/QuesmaOrg/quesma/quesma/model"
	"github.com/QuesmaOrg/quesma/quesma/model/bucket_aggregations"
	"github.com/QuesmaOrg/quesma/quesma/model/typical_queries"
	"github.com/QuesmaOrg/quesma/quesma/quesma/types"
)

const PancakeOptimizerName = "pancake"

// New way of generating queries, based on pancake model:
// 1. Parse Query DSL into aggregation tree.
// 2. Translate aggregation tree into pancake model.
// 3. Generate SQL queries from pancake model.
func (cw *ClickhouseQueryTranslator) PancakeParseAggregationJson(body types.JSON, addCount bool) ([]*model.Query, error) {
	// Phase 1: Parse Query DSL into aggregation tree
	queryAsMap := body.Clone()

	topLevel := pancakeAggregationTree{
		children: []*pancakeAggregationTreeNode{},
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

	if len(topLevel.children) == 0 { // it's fine to have no aggregations
		return []*model.Query{}, nil
	}

	// Phase 2: Translate aggregation tree into pancake model
	transformer := newPancakeTransformer(cw.Ctx)
	pancakeQueries, err := transformer.aggregationTreeToPancakes(topLevel)

	if err != nil {
		return nil, err
	}

	if addCount {

		// use our building blocks to add count
		augmentedCountAggregation := &pancakeModelMetricAggregation{
			name:            PancakeTotalCountMetricName,
			internalName:    "metric__" + PancakeTotalCountMetricName,
			queryType:       typical_queries.Count{},
			selectedColumns: []model.Expr{model.NewCountFunc()},
		}

		pancakeQueries[0].layers[0].currentMetricAggregations = append(pancakeQueries[0].layers[0].currentMetricAggregations, augmentedCountAggregation)
	}

	// Phase 3: Generate SQL queries from pancake model
	aggregationQueries := make([]*model.Query, 0)
	for _, pancakeQuery := range pancakeQueries {
		generator := newPancakeSqlQueryGeneratorr(cw.Ctx)
		dbQuery, err := generator.generateQuery(pancakeQuery)
		if err != nil {
			return nil, err
		}

		aggregationQueries = append(aggregationQueries, dbQuery)
	}

	return aggregationQueries, nil
}

func (cw *ClickhouseQueryTranslator) pancakeParseAggregationNames(aggs QueryMap) ([]*pancakeAggregationTreeNode, error) {
	aggregationLevels := make([]*pancakeAggregationTreeNode, 0)

	for aggrName, aggrDict := range aggs {
		if subAggregation, ok := aggrDict.(QueryMap); ok {
			subLevel, err := cw.pancakeParseAggregation(aggrName, subAggregation)
			if err != nil {
				return aggregationLevels, err
			}
			aggregationLevels = append(aggregationLevels, subLevel)
		} else {
			logger.WarnWithCtxAndReason(cw.Ctx, logger.ReasonUnsupportedQuery("unexpected_type")).
				Msgf("unexpected type of subaggregation: (%v: %v), value type: %T. Skipping", aggrName, aggrDict, aggrDict)
		}
	}
	return aggregationLevels, nil
}

func (cw *ClickhouseQueryTranslator) pancakeParseAggregation(aggregationName string, queryMap QueryMap) (*pancakeAggregationTreeNode, error) {
	if len(queryMap) == 0 {
		return nil, nil
	}

	// check if metadata is present
	var metadata model.JsonMap
	if metaRaw, exists := queryMap["meta"]; exists {
		metadata = metaRaw.(model.JsonMap)
		delete(queryMap, "meta")
	} else {
		metadata = model.NoMetadataField
	}

	aggregation := &pancakeAggregationTreeNode{
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
			return nil, fmt.Errorf("unknown metrics aggregation: %v", metricsAggrResult.AggrType)
		}
		return aggregation, nil
	}

	// 2. Pipeline aggregation => always leaf (for now)
	if pipelineAggr, err := cw.parsePipelineAggregations(queryMap); err != nil || pipelineAggr != nil {
		if err != nil {
			return nil, err
		}
		aggregation.queryType = pipelineAggr
		return aggregation, nil
	}

	// 3. Now process filter(s) first, because they apply to everything else on the same level or below.
	// Also filter introduces count to current level.
	if filterRaw, ok := queryMap["filter"]; ok {
		if filter, ok := filterRaw.(QueryMap); ok {
			whereClause := cw.parseQueryMap(filter).WhereClause
			if whereClause == nil { // empty filter <=> true
				whereClause = model.TrueExpr
			}
			aggregation.queryType = bucket_aggregations.NewFilterAgg(cw.Ctx, whereClause)
		} else {
			logger.WarnWithCtx(cw.Ctx).Msgf("filter is not a map, but %T, value: %v. Skipping", filterRaw, filterRaw)
		}
		delete(queryMap, "filter")
	}

	// 4. Bucket aggregations. They introduce new subaggregations, even if no explicit subaggregation defined on this level.
	if err := cw.pancakeTryBucketAggregation(aggregation, queryMap); err != nil {
		return nil, err
	}

	if aggs, ok := queryMap["aggs"]; ok {
		subAggregations, err := cw.pancakeParseAggregationNames(aggs.(QueryMap))
		if err != nil {
			return aggregation, err
		}
		aggregation.children = subAggregations
	}
	delete(queryMap, "aggs") // no-op if no "aggs"

	for k, v := range queryMap {
		// should be empty by now. If it's not, it's an unsupported/unrecognized type of aggregation.
		logger.ErrorWithCtxAndReason(cw.Ctx, logger.ReasonUnsupportedQuery(k)).
			Msgf("unexpected type of subaggregation: (%v: %v), value type: %T. Skipping", k, v, v)
		// TODO: remove hard fail. Temporary to make development easier
		return nil, fmt.Errorf("unsupported aggregation type: (%v: %v), value type: %T", k, v, v)
	}

	return aggregation, nil
}
