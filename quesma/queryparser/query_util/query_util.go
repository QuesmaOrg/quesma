package query_util

import (
	"context"
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/model"
	"mitmproxy/quesma/model/typical_queries"
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
	var col model.SelectColumn
	if fieldName == "*" {
		col = model.SelectColumn{Expression: model.NewWildcardExpr}
	} else {
		col = model.SelectColumn{Expression: model.NewTableColumnExpr(fieldName)}
	}
	return &model.Query{
		Columns:     []model.SelectColumn{col},
		WhereClause: query.WhereClause,
		OrderBy:     query.OrderBy,
		Limit:       applySizeLimit(ctx, limit),
		FromClause:  model.NewTableRef(tableName),
		TableName:   tableName,
		CanParse:    true,
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
