package eql

import (
	"context"
	"mitmproxy/quesma/clickhouse"
	"mitmproxy/quesma/elasticsearch"
	"mitmproxy/quesma/eql/transform"
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/model"
	"mitmproxy/quesma/queryparser"
	"mitmproxy/quesma/queryparser/query_util"
	"mitmproxy/quesma/quesma/types"
	"strconv"
	"strings"
)

// It implements quesma.IQueryTranslator for EQL queries.

type ClickhouseEQLQueryTranslator struct {
	ClickhouseLM *clickhouse.LogManager
	Table        *clickhouse.Table
	Ctx          context.Context
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
		}
		for _, property := range query.SortFields.Properties() {
			if val, ok := hits[i].Fields[property]; ok {
				hits[i].Sort = append(hits[i].Sort, elasticsearch.FormatSortValue(val[0]))
			} else {
				logger.WarnWithCtx(cw.Ctx).Msgf("property %s not found in fields", property)
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

func (cw *ClickhouseEQLQueryTranslator) ParseQuery(body types.JSON) ([]model.Query, []string, bool, bool, error) {
	simpleQuery, queryInfo, highlighter, err := cw.parseQuery(body)
	if err != nil {
		logger.ErrorWithCtx(cw.Ctx).Msgf("error parsing query: %v", err)
		return nil, nil, false, false, err
	}
	var columns []string
	var query *model.Query
	var queries []model.Query
	var isAggregation bool
	canParse := false

	if simpleQuery.CanParse {
		canParse = true
		query = query_util.BuildNRowsQuery(cw.Ctx, cw.Table.Name, "*", simpleQuery, queryInfo.I2)
		query.QueryInfo = queryInfo
		query.Highlighter = highlighter
		query.SortFields = simpleQuery.SortFields
		queries = append(queries, *query)
		isAggregation = false
		return queries, columns, isAggregation, canParse, nil
	}

	return nil, nil, false, false, err
}

func (cw *ClickhouseEQLQueryTranslator) parseQuery(queryAsMap types.JSON) (query model.SimpleQuery, searchQueryInfo model.SearchQueryInfo, highlighter model.Highlighter, err error) {

	// no highlighting here
	highlighter = queryparser.NewEmptyHighlighter()

	searchQueryInfo.Typ = model.ListAllFields

	var eqlQuery string

	if queryAsMap["query"] != nil {
		eqlQuery = queryAsMap["query"].(string)
	}

	if eqlQuery == "" {
		query.CanParse = false
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
	query.SortFields = []model.SortField{{Field: "@timestamp"}}

	return query, searchQueryInfo, highlighter, nil
}

// These methods are not supported by EQL. They are here to satisfy the interface.

func (cw *ClickhouseEQLQueryTranslator) MakeResponseAggregation(aggregations []model.Query, aggregationResults [][]model.QueryResultRow) *model.SearchResp {
	panic("EQL does not support aggregations")
}
