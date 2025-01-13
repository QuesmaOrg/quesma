// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package quesma

import (
	"context"
	"quesma/clickhouse"
	"quesma/eql"
	"quesma/model"
	"quesma/queryparser"
	"quesma/quesma/config"
	"quesma/quesma/types"
	"quesma/schema"
)

// This is an extracted interface for query translation.
// FIXME it should split into smaller interfaces: parser, builder and response maker
// FIXME it should have a better name
//
// Right now it has two implementation:
// 1. ClickhouseQueryTranslator (origin implementation)
// 2. ClickhouseEQLQueryTranslator (implements only a subset of methods)

type IQueryTranslator interface {
	ParseQuery(body types.JSON) (*model.ExecutionPlan, error)
	MakeSearchResponse(queries []*model.Query, ResultSets [][]model.QueryResultRow) *model.SearchResp
}

type QueryLanguage string

const (
	QueryLanguageDefault = "default"
	QueryLanguageEQL     = "eql"
)

func NewQueryTranslator(ctx context.Context, language QueryLanguage, schema schema.Schema, table *clickhouse.Table, logManager clickhouse.LogManagerIFace, dateMathRenderer string, searchAfterStrategy model.SearchAfterStrategy, indexes []string, configuration *config.QuesmaConfiguration) (queryTranslator IQueryTranslator) {
	switch language {
	case QueryLanguageEQL:
		return &eql.ClickhouseEQLQueryTranslator{ClickhouseLM: logManager, Table: table, Ctx: ctx, SearchAfterStrategy: searchAfterStrategy}
	default:
		return &queryparser.ClickhouseQueryTranslator{Table: table, Ctx: ctx, DateMathRenderer: dateMathRenderer, SearchAfterStrategy: searchAfterStrategy, Indexes: indexes, Config: configuration, Schema: schema}
	}
}
