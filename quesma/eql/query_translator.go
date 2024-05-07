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

func (cw *ClickhouseEQLQueryTranslator) BuildNRowsQuery(fieldName string, simpleQuery queryparser.SimpleQuery, limit int) *model.Query {

	return &model.Query{
		Fields:          []string{fieldName},
		NonSchemaFields: []string{},
		WhereClause:     simpleQuery.Sql.Stmt,
		SuffixClauses:   []string{},
		FromClause:      cw.Table.FullTableName(),
		CanParse:        true,
	}
}

func (cw *ClickhouseEQLQueryTranslator) MakeSearchResponse(ResultSet []model.QueryResultRow, typ model.SearchQueryType, highlighter queryparser.Highlighter) (*model.SearchResp, error) {

	// This shares a lot of code with the ClickhouseQueryTranslator
	//

	hits := make([]model.SearchHit, len(ResultSet))
	for i := range ResultSet {
		resultRow := ResultSet[i]

		hits[i].Fields = make(map[string][]interface{})
		hits[i].Highlight = make(map[string][]string)
		hits[i].Source = []byte(resultRow.String(cw.Ctx))
		if typ == model.ListAllFields {
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

func (cw *ClickhouseEQLQueryTranslator) ParseQuery(queryAsJson string) (query queryparser.SimpleQuery, searchQueryInfo model.SearchQueryInfo, highlighter queryparser.Highlighter) {

	// no highlighting here
	highlighter = queryparser.NewEmptyHighlighter()

	searchQueryInfo.Typ = model.ListAllFields
	query.Sql = queryparser.Statement{}

	queryAsMap := make(map[string]interface{})
	err := json.Unmarshal([]byte(queryAsJson), &queryAsMap)
	if err != nil {
		logger.ErrorWithCtx(cw.Ctx).Err(err).Msg("error parsing query request's JSON")

		query.CanParse = false
		query.Sql.Stmt = "Invalid JSON"
		return query, model.NewSearchQueryInfoNone(), highlighter
	}

	var eqlQuery string

	if queryAsMap["query"] != nil {
		eqlQuery = queryAsMap["query"].(string)
	}

	if eqlQuery == "" {
		query.CanParse = false
		query.Sql.Stmt = "Empty query"
		return query, model.NewSearchQueryInfoNone(), highlighter
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
		logger.ErrorWithCtx(cw.Ctx).Err(err).Msg("error transforming EQL query")
		query.CanParse = false
		query.Sql.Stmt = "Invalid EQL query"
		return query, model.NewSearchQueryInfoNone(), highlighter
	}

	query.Sql.Stmt = where
	query.CanParse = true

	return query, searchQueryInfo, highlighter
}

// These methods are not supported by EQL. They are here to satisfy the interface.

func (cw *ClickhouseEQLQueryTranslator) BuildSimpleCountQuery(whereClause string) *model.Query {
	panic("EQL does not support count")
}

func (cw *ClickhouseEQLQueryTranslator) BuildSimpleSelectQuery(whereClause string, size int) *model.Query {
	panic("EQL does not support this method")
}

func (cw *ClickhouseEQLQueryTranslator) MakeResponseAggregation(aggregations []model.QueryWithAggregation, aggregationResults [][]model.QueryResultRow) *model.SearchResp {
	panic("EQL does not support aggregations")
}

func (cw *ClickhouseEQLQueryTranslator) BuildFacetsQuery(fieldName string, simpleQuery queryparser.SimpleQuery, limit int) *model.Query {
	panic("EQL does not support facets")
}

func (cw *ClickhouseEQLQueryTranslator) ParseAggregationJson(aggregationJson string) ([]model.QueryWithAggregation, error) {
	panic("EQL does not support aggregations")
}
