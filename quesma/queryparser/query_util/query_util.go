// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package query_util

import (
	"context"
	"quesma/logger"
	"quesma/model"
	"quesma/model/typical_queries"
)

func IsNonAggregationQuery(query *model.Query) bool {
	switch query.Type.(type) {
	case typical_queries.Count, *typical_queries.Hits, nil: // FIXME erase nil, always have type non-empty, but it's not that completely easy, as it seems
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

func BuildHitsQuery(ctx context.Context, tableName string, fieldName string, query *model.SimpleQuery, limit int) *model.Query {
	var col model.Expr
	if fieldName == "*" {
		col = model.NewWildcardExpr
	} else {
		col = model.NewColumnRef(fieldName)
	}
	return &model.Query{
		SelectCommand: *model.NewSelectCommand([]model.Expr{col}, nil, query.OrderBy, model.NewTableRef(tableName), query.WhereClause, applySizeLimit(ctx, limit), 0, false),
		TableName:     tableName,
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
