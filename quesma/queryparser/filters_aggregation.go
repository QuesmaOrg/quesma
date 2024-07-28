// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package queryparser

import (
	"github.com/barkimedes/go-deepcopy"
	"quesma/logger"
	"quesma/model"
	"quesma/model/bucket_aggregations"
)

func (cw *ClickhouseQueryTranslator) parseFilters(queryMap QueryMap) (success bool, filtersAggr bucket_aggregations.Filters) {
	filtersAggr = bucket_aggregations.NewFiltersEmpty(cw.Ctx)

	filtersRaw, exists := queryMap["filters"]
	if !exists {
		return
	}

	filtersMap, ok := filtersRaw.(QueryMap)
	if !ok {
		logger.WarnWithCtx(cw.Ctx).Msgf("filters is not a map, but %T, value: %v. Using empty.", filtersRaw, filtersRaw)
		return
	}
	nested, exists := filtersMap["filters"]
	if !exists {
		logger.WarnWithCtx(cw.Ctx).Msgf("filters is not a map, but %T, value: %v. Skipping filters.", filtersRaw, filtersRaw)
		return
	}
	nestedMap, ok := nested.(QueryMap)
	if !ok {
		logger.WarnWithCtx(cw.Ctx).Msgf("filters is not a map, but %T, value: %v. Skipping filters.", nested, nested)
		return
	}

	filters := make([]bucket_aggregations.Filter, 0, len(nestedMap))
	for name, filter := range nestedMap {
		filterMap, ok := filter.(QueryMap)
		if !ok {
			logger.WarnWithCtx(cw.Ctx).Msgf("filter is not a map, but %T, value: %v. Skipping.", filter, filter)
			continue
		}
		filters = append(filters, bucket_aggregations.NewFilter(name, cw.parseQueryMap(filterMap)))
	}
	return true, bucket_aggregations.NewFilters(cw.Ctx, filters)
}

func (cw *ClickhouseQueryTranslator) processFiltersAggregation(aggrBuilder *aggrQueryBuilder,
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
				subAggregations, err := cw.parseAggregationNames(aggrBuilder, aggsCopy.(QueryMap))
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
