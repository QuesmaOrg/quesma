package eql

import (
	"context"
	"encoding/json"
	"mitmproxy/quesma/clickhouse"
	"mitmproxy/quesma/eql/transform"
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/model"
	"mitmproxy/quesma/queryparser"
	"strconv"
	"strings"
)

// It implements quesma.IQueryTranslator for EQL queries.

type ClickhouseEQLQueryTranslator struct {
	ClickhouseLM *clickhouse.LogManager
	Table        *clickhouse.Table
	Ctx          context.Context
}

func (cw *ClickhouseEQLQueryTranslator) applySizeLimit(size int) int {
	// FIXME hard limit here to prevent OOM
	const quesmaMaxSize = 10000
	if size > quesmaMaxSize {
		logger.WarnWithCtx(cw.Ctx).Msgf("setting hits size to=%d, got=%d", quesmaMaxSize, size)
		size = quesmaMaxSize
	}
	return size
}

func (cw *ClickhouseEQLQueryTranslator) BuildNRowsQuery(fieldName string, simpleQuery model.SimpleQuery, limit int) *model.Query {
	suffixClauses := make([]string, 0)
	if len(simpleQuery.SortFields) > 0 {
		suffixClauses = append(suffixClauses, "ORDER BY "+strings.Join(simpleQuery.SortFields, ", "))
	}
	if limit > 0 {
		suffixClauses = append(suffixClauses, "LIMIT "+strconv.Itoa(cw.applySizeLimit(limit)))
	}
	return &model.Query{
		Fields:          []string{fieldName},
		NonSchemaFields: []string{},
		WhereClause:     simpleQuery.Sql.Stmt,
		SuffixClauses:   suffixClauses,
		FromClause:      cw.Table.FullTableName(),
		CanParse:        true,
	}
}

func (cw *ClickhouseEQLQueryTranslator) MakeSearchResponse(ResultSet []model.QueryResultRow, query model.Query) (*model.SearchResp, error) {

	// This shares a lot of code with the ClickhouseQueryTranslator
	//

	hits := make([]model.SearchHit, len(ResultSet))
	for i := range ResultSet {
		resultRow := ResultSet[i]

		hits[i].Fields = make(map[string][]interface{})
		hits[i].Highlight = make(map[string][]string)
		hits[i].Source = []byte(resultRow.String(cw.Ctx))
		if query.QueryInfo.Typ == model.ListAllFields {
			hits[i].ID = strconv.Itoa(i + 1)
			hits[i].Index = cw.Table.Name
			hits[i].Score = 1
			hits[i].Version = 1
			hits[i].Sort = []any{
				"2024-01-30T19:38:54.607Z",
				2944,
			}
		}
	}

	return &model.SearchResp{
		Hits: model.SearchHits{
			Total: &model.Total{
				Value:    len(ResultSet),
				Relation: "eq",
			},
			Events: hits,
		},
		Shards: model.ResponseShards{
			Total:      1,
			Successful: 1,
			Failed:     0,
		},
	}, nil
}

func (cw *ClickhouseEQLQueryTranslator) ParseQuery(queryAsJson string) (query model.SimpleQuery, searchQueryInfo model.SearchQueryInfo, highlighter model.Highlighter, err error) {

	// no highlighting here
	highlighter = queryparser.NewEmptyHighlighter()

	searchQueryInfo.Typ = model.ListAllFields
	query.Sql = model.Statement{}

	queryAsMap := make(map[string]interface{})
	err = json.Unmarshal([]byte(queryAsJson), &queryAsMap)
	if err != nil {
		logger.ErrorWithCtx(cw.Ctx).Err(err).Msg("error parsing query request's JSON")

		query.CanParse = false
		query.Sql.Stmt = "Invalid JSON"
		return query, model.NewSearchQueryInfoNone(), highlighter, err
	}

	var eqlQuery string

	if queryAsMap["query"] != nil {
		eqlQuery = queryAsMap["query"].(string)
	}

	if eqlQuery == "" {
		query.CanParse = false
		query.Sql.Stmt = "Empty query"
		return query, model.NewSearchQueryInfoNone(), highlighter, nil
	}

	// FIXME this is a naive translation.
	// It should use the table schema to translate field names
	translateName := func(name *transform.Symbol) (*transform.Symbol, error) {
		res := strings.ReplaceAll(name.Name, ".", "::")
		res = "\"" + res + "\"" // TODO proper escaping
		return transform.NewSymbol(res), nil
	}

	trans := NewTransformer()
	trans.FieldNameTranslator = translateName

	// We don't extract parameters for now.
	// Query execution does not support parameters yet.
	trans.ExtractParameters = false
	where, _, err := trans.TransformQuery(eqlQuery)

	if err != nil {
		logger.ErrorWithCtx(cw.Ctx).Err(err).Msgf("error transforming EQL query: '%s'", eqlQuery)
		query.CanParse = false
		query.Sql.Stmt = "Invalid EQL query"
		return query, model.NewSearchQueryInfoNone(), highlighter, err
	}

	query.Sql.Stmt = where
	query.CanParse = true
	query.SortFields = []string{"\"@timestamp\""}

	return query, searchQueryInfo, highlighter, nil
}

// These methods are not supported by EQL. They are here to satisfy the interface.

func (cw *ClickhouseEQLQueryTranslator) BuildSimpleCountQuery(whereClause string) *model.Query {
	panic("EQL does not support count")
}

func (cw *ClickhouseEQLQueryTranslator) MakeResponseAggregation(aggregations []model.Query, aggregationResults [][]model.QueryResultRow) *model.SearchResp {
	panic("EQL does not support aggregations")
}

func (cw *ClickhouseEQLQueryTranslator) BuildFacetsQuery(fieldName string, simpleQuery model.SimpleQuery, limit int) *model.Query {
	panic("EQL does not support facets")
}

func (cw *ClickhouseEQLQueryTranslator) ParseAggregationJson(aggregationJson string) ([]model.Query, error) {
	panic("EQL does not support aggregations")
}
