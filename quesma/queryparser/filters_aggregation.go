package queryparser

import (
	"github.com/barkimedes/go-deepcopy"
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/model"
	"mitmproxy/quesma/model/bucket_aggregations"
)

func (cw *ClickhouseQueryTranslator) parseFilters(queryMap QueryMap) (success bool, filtersAggr bucket_aggregations.Filters) {
	filtersRaw, exists := queryMap["filters"]
	if !exists {
		return false, bucket_aggregations.NewFiltersEmpty(cw.Ctx)
	}

	filtersMap, ok := filtersRaw.(QueryMap)
	if !ok {
		logger.WarnWithCtx(cw.Ctx).Msgf("filters is not a map, but %T, value: %v. Using empty.", filtersRaw, filtersRaw)
		return false, bucket_aggregations.NewFiltersEmpty(cw.Ctx)
	}
	filters := make([]bucket_aggregations.Filter, 0, len(filtersMap))
	for name, filter := range filtersMap {
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
	aggr bucket_aggregations.Filters, queryMap QueryMap, resultAccumulator *[]model.Query) error {
	for _, filter := range aggr.Filters {
		// newBuilder := aggrBuilder.clone()
		// newBuilder.Type = bucket_aggregations.NewFilters(cw.Ctx, []bucket_aggregations.Filter{filter})
		// newBuilder.whereBuilder.CombineWheresWith(filter.Sql)
		// newBuilder.Aggregators = append(aggrBuilder.Aggregators, model.NewAggregatorEmpty(filter.Name))
		aggrBuilder.Type = aggr
		aggrBuilder.whereBuilder.CombineWheresWith(cw.Ctx, filter.Sql)
		aggrBuilder.Aggregators = append(aggrBuilder.Aggregators, model.NewAggregatorEmpty(filter.Name))
		*resultAccumulator = append(*resultAccumulator, aggrBuilder.buildBucketAggregation(nil)) // nil for now, will be changed
		if aggs, ok := queryMap["aggs"].(QueryMap); ok {
			aggsCopy, errAggs := deepcopy.Anything(aggs)
			if errAggs == nil {
				//err := cw.parseAggregationNames(newBuilder, aggsCopy.(QueryMap), resultAccumulator)
				err := cw.parseAggregationNames(aggrBuilder, aggsCopy.(QueryMap), resultAccumulator)
				if err != nil {
					return err
				}
			} else {
				logger.ErrorWithCtx(cw.Ctx).Msgf("deepcopy 'aggs' map error: %v. Skipping. aggs: %v", errAggs, aggs)
			}
		}
	}
	delete(queryMap, "filters")
	return nil
}
