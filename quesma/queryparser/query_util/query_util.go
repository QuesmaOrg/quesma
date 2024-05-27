package query_util

import (
	"bytes"
	"context"
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/model"
	"mitmproxy/quesma/queryparser/aexp"
	"strconv"
	"strings"
)

func IsNonAggregationQuery(queryInfo model.SearchQueryInfo, body []byte) bool {
	return ((queryInfo.Typ == model.ListByField ||
		queryInfo.Typ == model.ListAllFields ||
		queryInfo.Typ == model.Normal) &&
		!bytes.Contains(body, []byte("aggs"))) ||
		queryInfo.Typ == model.Facets ||
		queryInfo.Typ == model.FacetsNumeric ||
		queryInfo.Typ == model.CountAsync
}

func BuildNRowsQuery(ctx context.Context, tableName string, fieldName string, query model.SimpleQuery, limit int) *model.Query {
	suffixClauses := make([]string, 0)
	if len(query.SortFields) > 0 {
		suffixClauses = append(suffixClauses, "ORDER BY "+AsQueryString(query.SortFields))
	}
	if limit > 0 {
		suffixClauses = append(suffixClauses, "LIMIT "+strconv.Itoa(applySizeLimit(ctx, limit)))
	}

	var col model.SelectColumn
	if fieldName == "*" {
		col = model.SelectColumn{Expression: aexp.Wildcard}
	} else {
		col = model.SelectColumn{Expression: aexp.TableColumn(fieldName)}
	}

	return &model.Query{
		Columns:       []model.SelectColumn{col},
		WhereClause:   query.Sql.Stmt,
		SuffixClauses: suffixClauses,
		FromClause:    tableName,
		CanParse:      true,
	}
}

func AsQueryString(sortFields []model.SortField) string {
	if len(sortFields) == 0 {
		return ""
	}
	sortStrings := make([]string, 0, len(sortFields))
	for _, sortField := range sortFields {
		query := strings.Builder{}
		query.WriteString(strconv.Quote(sortField.Field))
		if sortField.Desc {
			query.WriteString(" desc")
		}
		sortStrings = append(sortStrings, query.String())
	}
	return strings.Join(sortStrings, ", ")
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
