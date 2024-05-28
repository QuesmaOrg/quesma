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
	ParseQuery(body []byte) ([]model.Query, bool, bool, error)

	MakeSearchResponse(ResultSet []model.QueryResultRow, query model.Query) (*model.SearchResp, error)
	MakeResponseAggregation(aggregations []model.Query, aggregationResults [][]model.QueryResultRow) *model.SearchResp
}

type QueryLanguage string

const (
	QueryLanguageDefault = "default"
	QueryLanguageEQL     = "eql"
)

func NewQueryTranslator(ctx context.Context, language QueryLanguage, table *clickhouse.Table, logManager *clickhouse.LogManager, dateMathRenderer string) (queryTranslator IQueryTranslator) {
	switch language {
	case QueryLanguageEQL:
		return &eql.ClickhouseEQLQueryTranslator{ClickhouseLM: logManager, Table: table, Ctx: ctx}
	default:
		return &queryparser.ClickhouseQueryTranslator{ClickhouseLM: logManager, Table: table, Ctx: ctx, DateMathRenderer: dateMathRenderer}
	}
}
