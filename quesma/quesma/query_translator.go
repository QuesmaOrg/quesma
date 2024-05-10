package quesma

import (
	"context"
	"mitmproxy/quesma/clickhouse"
	"mitmproxy/quesma/eql"
	"mitmproxy/quesma/model"
	"mitmproxy/quesma/queryparser"
)

// This is an extracted interface for query translation.
// FIXME it should split into smaller interfaces: parser, builder and response maker
// FIXME it should have a better name
//
// Right now it has two implementation:
// 1. ClickhouseQueryTranslator (origin implementation)
// 2. ClickhouseEQLQueryTranslator (implements only a subset of methods)

type IQueryTranslator interface {
	ParseQuery(queryAsJson string) (queryparser.SimpleQuery, model.SearchQueryInfo, queryparser.Highlighter)
	ParseAggregationJson(aggregationJson string) ([]model.QueryWithAggregation, error)

	BuildSimpleCountQuery(whereClause string) *model.Query
	BuildSimpleSelectQuery(whereClause string, size int) *model.Query
	BuildNRowsQuery(fieldName string, simpleQuery queryparser.SimpleQuery, limit int) *model.Query
	BuildFacetsQuery(fieldName string, simpleQuery queryparser.SimpleQuery, limit int) *model.Query

	MakeSearchResponse(ResultSet []model.QueryResultRow, typ model.SearchQueryType, highlighter queryparser.Highlighter) (*model.SearchResp, error)
	MakeResponseAggregation(aggregations []model.QueryWithAggregation, aggregationResults [][]model.QueryResultRow) *model.SearchResp
}

type QueryLanguage string

const (
	QueryLanguageDefault = "default"
	QueryLanguageEQL     = "eql"
)

func NewQueryTranslator(ctx context.Context, language QueryLanguage, table *clickhouse.Table, logManager *clickhouse.LogManager) (queryTranslator IQueryTranslator) {

	switch language {
	case QueryLanguageEQL:
		queryTranslator = &eql.ClickhouseEQLQueryTranslator{ClickhouseLM: logManager, Table: table, Ctx: ctx}
	default:
		queryTranslator = &queryparser.ClickhouseQueryTranslator{ClickhouseLM: logManager, Table: table, Ctx: ctx}
	}

	return queryTranslator

}
