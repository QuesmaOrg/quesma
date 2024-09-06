// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package queryparser

import (
	"fmt"
	"quesma/model"
	"quesma/model/metrics_aggregations"
	"strconv"
	"strings"
)

func (p *pancakeSqlQueryGenerator) generateTopHitsQuery(aggregation *pancakeModel,
	combinatorWhere []model.Expr,
	topHits *pancakeModelMetricAggregation,
	groupBys []model.AliasedExpr,
	selectColumns []model.AliasedExpr,
	origQuery *model.SelectCommand) (*model.SelectCommand, error) {

	// TODO: we assume some group bys
	var topHitsQueryType metrics_aggregations.TopHits
	if queryType, ok := topHits.queryType.(metrics_aggregations.TopHits); ok {
		topHitsQueryType = queryType
	} else {
		return nil, fmt.Errorf("expected top_hits query type, got: %T", topHits.queryType)
	}

	// Add combinators if needed
	whereClause := aggregation.whereClause
	if len(combinatorWhere) > 0 {
		whereClause = model.And(append(combinatorWhere, aggregation.whereClause))
	}

	groupTableName := "group_table"
	hitTableName := "hit_table"

	convertColumnRefToHitTable := func(expr model.Expr) model.Expr {
		switch exprTyped := expr.(type) {
		case model.ColumnRef:
			// TODO: hack alert, we treat geo here in unique way
			if strings.HasSuffix(exprTyped.ColumnName, "Location") {
				return model.NewFunction("map",
					model.NewLiteral("'lat'"),
					model.NewLiteral(strconv.Quote(hitTableName)+"."+strconv.Quote(exprTyped.ColumnName+"::lat")),
					model.NewLiteral("'lon'"),
					model.NewLiteral(strconv.Quote(hitTableName)+"."+strconv.Quote(exprTyped.ColumnName+"::lon")),
				)
			}
			// TODO: Need better type, this should not be NewLiteral, but ColumnRefWithTable
			return model.NewLiteral(strconv.Quote(hitTableName) + "." + strconv.Quote(exprTyped.ColumnName))
		}
		return expr
	}

	var joinExprs []model.Expr
	var partitionByExprs []model.Expr
	for _, groupBy := range groupBys {
		partitionByExprs = append(partitionByExprs,
			model.NewLiteral(strconv.Quote(groupTableName)+"."+strconv.Quote(groupBy.Alias)))
		joinExprs = append(joinExprs, model.NewInfixExpr(
			model.NewLiteral(strconv.Quote(groupTableName)+"."+strconv.Quote(groupBy.Alias)),
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
		aliasedColumnLiteral := model.NewLiteral(fmt.Sprintf("%s.%s", strconv.Quote(groupTableName), selectColumn.AliasRef().Value))
		aliasedColumn := model.NewAliasedExpr(aliasedColumnLiteral, selectColumn.Alias)
		newSelects = append(newSelects, aliasedColumn)
	}

	for topHitsIdx, selectedTopHits := range topHits.selectedColumns {
		aliasedColumnName := fmt.Sprintf("top_hits_%d", topHitsIdx+1)
		withConvertedHitTable := convertColumnRefToHitTable(selectedTopHits)
		aliasedColumn := model.NewAliasedExpr(withConvertedHitTable, aliasedColumnName)
		newSelects = append(newSelects, aliasedColumn)
	}

	// TODO: we need to implement order by here
	rankSelect := model.NewAliasedExpr(
		model.NewWindowFunction("ROW_NUMBER", []model.Expr{}, partitionByExprs, []model.OrderByExpr{}),
		"top_hits_rank")
	newSelects = append(newSelects, rankSelect)

	prefixWithTopHists := func(orderByExprs []model.OrderByExpr) (result []model.OrderByExpr) {
		for _, orderBy := range orderByExprs {
			if orderByLiteral, ok := orderBy.Expr.(model.LiteralExpr); ok {
				result = append(result, model.NewOrderByExpr(
					model.NewLiteral(strconv.Quote(groupTableName)+"."+orderByLiteral.Value.(string)),
					orderBy.Direction,
				))
			} else {
				panic("todo it is bug")
			}
		}
		return
	}

	joinQuery := model.SelectCommand{
		Columns:     p.aliasedExprArrayToExpr(newSelects),
		FromClause:  fromClause,
		WhereClause: whereClause,
		OrderBy:     prefixWithTopHists(origQuery.OrderBy),
	}

	joinQueryName := "quesma_top_hits_join"

	namedCte := []*model.CTE{
		{
			Name:          topHitsSourceName,
			SelectCommand: origQuery,
		},
		{
			Name:          joinQueryName,
			SelectCommand: &joinQuery,
		},
	}

	// TODO: Simplify
	resultQuery := &model.SelectCommand{
		Columns:    p.aliasedExprArrayToLiteralExpr(newSelects),
		FromClause: model.NewLiteral(joinQueryName),
		WhereClause: model.NewInfixExpr(
			model.NewLiteral("top_hits_rank"),
			"<=",
			model.NewLiteral(strconv.Itoa(topHitsQueryType.Size))),
		NamedCTEs: namedCte,
	}

	return resultQuery, nil
}
