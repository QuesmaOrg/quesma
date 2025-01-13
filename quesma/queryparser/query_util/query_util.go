// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package query_util

import (
	"context"
	"quesma/logger"
	"quesma/model"
	"quesma/model/bucket_aggregations"
	"quesma/model/typical_queries"
)

func IsNonAggregationQuery(query *model.Query) bool {
	switch query.Type.(type) {
	// FIXME erase nil, always have type non-empty, but it's not that completely easy, as it seems
	case typical_queries.Count, *typical_queries.Hits, nil:
		return true
	default:
		return false
	}
}

// IsAnyKindOfTerms returns true if queryType is Terms, Significant Terms, or Multi Terms
func IsAnyKindOfTerms(queryType model.QueryType) bool {
	switch queryType.(type) {
	case bucket_aggregations.Terms, bucket_aggregations.MultiTerms:
		return true
	default:
		return false
	}
}

/* FIXME use this in MakeSearchResponse to stop relying on order of queries
func FilterAggregationQueries(queries []*model.Query) []*model.Query {
	filtered := make([]*model.Query, 0)
	for _, query := range queries {
		if IsNonAggregationQuery(query) {
			filtered = append(filtered, query)
		}
	}
	return filtered
}
*/

func BuildHitsQuery(ctx context.Context, tableName string, fieldNames []string, query *model.SimpleQuery, limit int,
	searchAfter any, searchAfterStrategy model.SearchAfterStrategy) *model.Query {
	var columns []model.Expr
	for _, fieldName := range fieldNames {
		if fieldName == "*" {
			columns = append(columns, model.NewWildcardExpr)
		} else {
			columns = append(columns, model.NewColumnRef(fieldName))
		}
	}

	return &model.Query{
		SelectCommand: *model.NewSelectCommand(columns, nil, query.OrderBy, model.NewTableRef(tableName),
			query.WhereClause, []model.Expr{}, applySizeLimit(ctx, limit), 0, false, []*model.CTE{}),
		SearchAfter:         searchAfter,
		SearchAfterStrategy: searchAfterStrategy,
	}
}

func applySizeLimit(ctx context.Context, size int) int {
	// FIXME hard limit here to prevent OOM
	const quesmaMaxSize = 10000
	if size > quesmaMaxSize {
		logger.WarnWithCtx(ctx).Msgf("setting hits size to=%d, got=%d", quesmaMaxSize, size)
		size = quesmaMaxSize
	}
	return size
}
