// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package queryparser

import (
	"fmt"
	"quesma/model"
	"quesma/model/bucket_aggregations"
	"sort"
)

func (cw *ClickhouseQueryTranslator) parseFilters(aggregation *pancakeAggregationTreeNode, paramsRaw any) error {
	params, ok := paramsRaw.(QueryMap)
	if !ok {
		return fmt.Errorf("filters is not a map, but %T, value: %v", paramsRaw, paramsRaw)
	}
	nestedRaw, exists := params["filters"]
	if !exists {
		return fmt.Errorf("filters is not a map, but %T, value: %v", params, params)
	}
	nested, ok := nestedRaw.(QueryMap)
	if !ok {
		return fmt.Errorf("filters is not a map, but %T, value: %v", nestedRaw, nestedRaw)
	}

	filters := make([]bucket_aggregations.Filter, 0, len(nested))
	for name, filterRaw := range nested {
		filterMap, ok := filterRaw.(QueryMap)
		if !ok {
			return fmt.Errorf("filter is not a map, but %T, value: %v", filterRaw, filterRaw)
		}
		filter := cw.parseQueryMap(filterMap)
		if filter.WhereClause == nil {
			filter.WhereClause = model.TrueExpr
			filter.CanParse = true
		}
		filters = append(filters, bucket_aggregations.NewFilter(name, filter))
	}

	sort.Slice(filters, func(i, j int) bool {
		return filters[i].Name < filters[j].Name
	})
	aggregation.queryType = bucket_aggregations.NewFilters(cw.Ctx, filters)
	aggregation.isKeyed = true
	return nil
}
