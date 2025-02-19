// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package elastic_query_dsl

import (
	"fmt"
	"github.com/QuesmaOrg/quesma/quesma/model"
	"github.com/QuesmaOrg/quesma/quesma/model/metrics_aggregations"
	"strconv"
)

func (p *pancakeSqlQueryGenerator) quotedLiteral(name string) model.LiteralExpr {
	return model.NewLiteral(strconv.Quote(name))
}

// generateSimpleTopHitsQuery generates an SQL for top_hits/top_metrics
// original query does not any group aggregations and we don't need to do JOIN.
func (p *pancakeSqlQueryGenerator) generateSimpleTopHitsQuery(topHits *pancakeModelMetricAggregation,
	whereClause model.Expr,
	orderBy []model.OrderByExpr,
	size int) (*model.SelectCommand, error) {

	newSelects := make([]model.AliasedExpr, 0, len(topHits.selectedColumns))
	for topHitsIdx, selectedTopHits := range topHits.selectedColumns {
		aliasedColumn := model.NewAliasedExpr(selectedTopHits, topHits.InternalNameForCol(topHitsIdx))
		newSelects = append(newSelects, aliasedColumn)
	}

	resultQuery := &model.SelectCommand{
		Columns:     p.aliasedExprArrayToExpr(newSelects),
		FromClause:  model.NewTableRef(model.SingleTableNamePlaceHolder),
		WhereClause: whereClause,
		OrderBy:     orderBy,
		Limit:       size,
	}

	return resultQuery, nil
}

// generateTopHitsQuery generates an SQL for top_hits/top_metrics.
// It takes original group by query and JOIN it with select to actual fields.
func (p *pancakeSqlQueryGenerator) generateTopHitsQuery(aggregation *pancakeModel,
	combinatorWhere []model.Expr,
	topHits *pancakeModelMetricAggregation,
	groupBys []model.AliasedExpr,
	selectColumns []model.AliasedExpr,
	origQuery *model.SelectCommand) (*model.SelectCommand, error) {

	var sizeLimit int
	var topOrderBy []model.OrderByExpr

	switch queryType := topHits.queryType.(type) {
	case *metrics_aggregations.TopHits:
		topOrderBy = queryType.OrderBy
		sizeLimit = queryType.Size
	case *metrics_aggregations.TopMetrics:
		if len(queryType.SortBy) > 0 {
			topOrderBy = []model.OrderByExpr{
				model.NewOrderByExpr(
					model.NewColumnRef(queryType.SortBy),
					queryType.SortOrder,
				),
			}
		} else {
			topOrderBy = []model.OrderByExpr{}
		}
		sizeLimit = queryType.Size
	default:
		return nil, fmt.Errorf("expected top_hits/top_metrics query type, got: %T", topHits.queryType)
	}

	// Add combinators if needed
	whereClause := aggregation.whereClause
	if len(combinatorWhere) > 0 {
		whereClause = model.And(append(combinatorWhere, aggregation.whereClause))
	}

	if len(groupBys) == 0 {
		return p.generateSimpleTopHitsQuery(topHits, whereClause, topOrderBy, sizeLimit)
	}

	groupTableName := "group_table"
	hitTableName := "hit_table"

	groupTableLiteral := func(reference string) model.Expr {
		return model.NewLiteral(strconv.Quote(groupTableName) + "." + strconv.Quote(reference))
	}

	convertColumnRefToHitTable := func(expr model.Expr) model.Expr {
		switch exprTyped := expr.(type) {
		case model.ColumnRef:

			return model.ColumnRef{
				TableAlias: hitTableName,
				ColumnName: exprTyped.ColumnName,
			}
		}
		return expr
	}

	var joinExprs []model.Expr
	var partitionByExprs []model.Expr
	for _, groupBy := range groupBys {
		partitionByExprs = append(partitionByExprs, groupTableLiteral(groupBy.Alias))
		joinExprs = append(joinExprs, model.NewInfixExpr(
			groupTableLiteral(groupBy.Alias),
			"=",
			convertColumnRefToHitTable(groupBy.Expr)))
	}

	topHitsSourceName := "quesma_top_hits_group_table"

	fromClause := model.NewJoinExpr(
		model.NewAliasedExpr(model.NewLiteral(topHitsSourceName), groupTableName),
		model.NewAliasedExpr(model.NewTableRef(model.SingleTableNamePlaceHolder), hitTableName),
		"LEFT OUTER",
		model.And(joinExprs))

	newSelects := make([]model.AliasedExpr, 0, len(selectColumns)+len(topHits.selectedColumns))
	for _, selectColumn := range selectColumns {
		aliasedColumn := model.NewAliasedExpr(groupTableLiteral(selectColumn.Alias), selectColumn.Alias)
		newSelects = append(newSelects, aliasedColumn)
	}

	selectsForOrderBy := make([]model.Expr, 0, len(origQuery.OrderBy))
	for _, usedByOrderBy := range origQuery.OrderBy {
		if orderByLiteral, ok := usedByOrderBy.Expr.(model.LiteralExpr); ok {
			unquoted, err := strconv.Unquote(orderByLiteral.Value.(string))
			if err != nil {
				unquoted = orderByLiteral.Value.(string)
			}
			alreadyAdded := false
			for _, newSelect := range newSelects {
				if newSelect.Alias == unquoted {
					alreadyAdded = true
					break
				}
			}
			if !alreadyAdded {
				selectsForOrderBy = append(selectsForOrderBy, model.NewAliasedExpr(
					groupTableLiteral(unquoted), unquoted))
			}
		} else {
			panic("all pancake orderBy are on aliases, so we should have LiteralExpr here")
		}
	}

	for topHitsIdx, selectedTopHits := range topHits.selectedColumns {
		withConvertedHitTable := convertColumnRefToHitTable(selectedTopHits)
		aliasedColumn := model.NewAliasedExpr(withConvertedHitTable, topHits.InternalNameForCol(topHitsIdx))
		newSelects = append(newSelects, aliasedColumn)
	}

	// TODO: we need to test order by here
	rankSelect := model.NewAliasedExpr(
		model.NewWindowFunction("ROW_NUMBER", []model.Expr{}, partitionByExprs, topOrderBy),
		"top_hits_rank")
	newSelects = append(newSelects, rankSelect)

	joinQuery := model.SelectCommand{
		Columns:     append(p.aliasedExprArrayToExpr(newSelects), selectsForOrderBy...),
		FromClause:  fromClause,
		WhereClause: whereClause,
	}

	joinQueryName := "quesma_top_hits_join"

	namedCTEs := []*model.CTE{
		{
			Name:          topHitsSourceName,
			SelectCommand: origQuery,
		},
		{
			Name:          joinQueryName,
			SelectCommand: &joinQuery,
		},
	}

	orderBy := append(origQuery.OrderBy, model.NewOrderByExpr(
		p.quotedLiteral("top_hits_rank"),
		model.AscOrder,
	))

	resultQuery := &model.SelectCommand{
		Columns:    p.aliasedExprArrayToLiteralExpr(newSelects),
		FromClause: p.quotedLiteral(joinQueryName),
		WhereClause: model.NewInfixExpr(
			p.quotedLiteral("top_hits_rank"),
			"<=",
			model.NewLiteral(sizeLimit)),
		OrderBy:   orderBy,
		NamedCTEs: namedCTEs,
	}

	return resultQuery, nil
}
