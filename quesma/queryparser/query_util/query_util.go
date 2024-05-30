package query_util

import (
	"context"
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/model"
	"mitmproxy/quesma/queryparser/aexp"
	"mitmproxy/quesma/quesma/types"
)

func IsNonAggregationQuery(queryInfo model.SearchQueryInfo, body types.JSON) bool {
	_, hasAggs := body["aggs"]
	return ((queryInfo.Typ == model.ListByField ||
		queryInfo.Typ == model.ListAllFields ||
		queryInfo.Typ == model.Normal) &&
		!hasAggs) ||
		queryInfo.Typ == model.Facets ||
		queryInfo.Typ == model.FacetsNumeric ||
		queryInfo.Typ == model.CountAsync
}

func BuildNRowsQuery(ctx context.Context, tableName string, fieldName string, query model.SimpleQuery, limit int) *model.Query {
	var col model.SelectColumn
	if fieldName == "*" {
		col = model.SelectColumn{Expression: aexp.Wildcard}
	} else {
		col = model.SelectColumn{Expression: aexp.TableColumn(fieldName)}
	}

	return &model.Query{
		Columns:     []model.SelectColumn{col},
		WhereClause: query.WhereClauseAsString(),
		OrderBy:     query.OrderBy,
		Limit:       applySizeLimit(ctx, limit),
		FromClause:  tableName,
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
