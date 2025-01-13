// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package eql

import (
	"context"
	"fmt"
	"quesma/clickhouse"
	"quesma/elasticsearch"
	"quesma/eql/transform"
	"quesma/logger"
	"quesma/model"
	"quesma/model/typical_queries"
	"quesma/queryparser"
	"quesma/queryparser/query_util"
	"quesma/quesma/types"
	"strconv"
	"strings"
)

// It implements quesma.IQueryTranslator for EQL queries.

type ClickhouseEQLQueryTranslator struct {
	ClickhouseLM        clickhouse.LogManagerIFace
	Table               *clickhouse.Table
	Ctx                 context.Context
	SearchAfterStrategy model.SearchAfterStrategy
}

func (cw *ClickhouseEQLQueryTranslator) MakeSearchResponse(queries []*model.Query, ResultSets [][]model.QueryResultRow) *model.SearchResp {
	// for now len(queries) should be 1, len(ResultSets) should be 1
	if len(queries) < 1 || len(ResultSets) < 1 {
		logger.WarnWithCtx(cw.Ctx).Msgf("queries or ResultSets are empty, queries=%+v, ResultSets=%+v", queries, ResultSets)
		return &model.SearchResp{}
	}

	query := queries[0]
	ResultSet := ResultSets[0]

	// This shares a lot of code with the ClickhouseQueryTranslator
	//
	hits := make([]model.SearchHit, len(ResultSet))
	for i := range ResultSet {
		resultRow := ResultSet[i]

		hits[i].Fields = make(map[string][]interface{})
		hits[i].Highlight = make(map[string][]string)
		hits[i].Source = []byte(resultRow.String(cw.Ctx))
		hits[i].ID = strconv.Itoa(i + 1)
		hits[i].Index = cw.Table.Name
		hits[i].Score = 1
		hits[i].Version = 1
		for _, fieldName := range query.SelectCommand.OrderByFieldNames() {
			if val, ok := hits[i].Fields[fieldName]; ok {
				hits[i].Sort = append(hits[i].Sort, elasticsearch.FormatSortValue(val[0]))
			} else {
				logger.WarnWithCtx(cw.Ctx).Msgf("field %s not found in fields", fieldName)
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
	}
}

func (cw *ClickhouseEQLQueryTranslator) ParseQuery(body types.JSON) (*model.ExecutionPlan, error) {
	simpleQuery, queryInfo, highlighter, err := cw.parseQuery(body)

	if err != nil {
		logger.ErrorWithCtx(cw.Ctx).Msgf("error parsing query: %v", err)
		return nil, err
	}

	var query *model.Query
	var queries []*model.Query

	if simpleQuery.CanParse {

		query = query_util.BuildHitsQuery(cw.Ctx, cw.Table.Name, []string{"*"}, &simpleQuery, queryInfo.Size, queryInfo.SearchAfter, cw.SearchAfterStrategy)
		queryType := typical_queries.NewHits(cw.Ctx, cw.Table, &highlighter, cw.SearchAfterStrategy, query.SelectCommand.OrderByFieldNames(), true, false, false, []string{cw.Table.Name})
		query.Type = &queryType
		query.Highlighter = highlighter
		query.SelectCommand.OrderBy = simpleQuery.OrderBy
		queries = append(queries, query)
		return &model.ExecutionPlan{Queries: queries}, nil

	}

	return nil, fmt.Errorf("could not parse query")
}

func (cw *ClickhouseEQLQueryTranslator) parseQuery(queryAsMap types.JSON) (query model.SimpleQuery, searchQueryInfo model.HitsCountInfo, highlighter model.Highlighter, err error) {

	// no highlighting here
	highlighter = queryparser.NewEmptyHighlighter()

	searchQueryInfo.Type = model.ListAllFields

	var eqlQuery string

	if queryAsMap["query"] != nil {
		eqlQuery = queryAsMap["query"].(string)
	}

	if eqlQuery == "" {
		query.CanParse = false
		return query, model.NewEmptyHitsCountInfo(), highlighter, nil
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
		return query, model.NewEmptyHitsCountInfo(), highlighter, err
	}

	query.WhereClause = model.NewLiteral(where) // @TODO that's to be fixed
	query.CanParse = true
	query.OrderBy = []model.OrderByExpr{model.NewSortColumn("@timestamp", model.DescOrder)}

	return query, searchQueryInfo, highlighter, nil
}

// These methods are not supported by EQL. They are here to satisfy the interface.

func (cw *ClickhouseEQLQueryTranslator) MakeResponseAggregation(aggregations []*model.Query, aggregationResults [][]model.QueryResultRow) *model.SearchResp {
	panic("EQL does not support aggregations")
}
