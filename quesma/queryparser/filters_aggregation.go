// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package queryparser

import (
	"quesma/logger"
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
