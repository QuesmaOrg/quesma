// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package quesma

import (
	"context"
	"github.com/QuesmaOrg/quesma/quesma/clickhouse"
	"github.com/QuesmaOrg/quesma/quesma/model"
	"github.com/QuesmaOrg/quesma/quesma/queryparser"
	"github.com/QuesmaOrg/quesma/quesma/quesma/types"
	"github.com/QuesmaOrg/quesma/quesma/schema"
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

func NewQueryTranslator(ctx context.Context, schema schema.Schema, table *clickhouse.Table, logManager clickhouse.LogManagerIFace, dateMathRenderer string, indexes []string) (queryTranslator IQueryTranslator) {
	return &queryparser.ClickhouseQueryTranslator{Ctx: ctx, DateMathRenderer: dateMathRenderer, Indexes: indexes, Schema: schema, Table: table}
}
