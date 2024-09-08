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

func (p *pancakeSqlQueryGenerator) quotedLiteral(name string) model.LiteralExpr {
	return model.NewLiteral(strconv.Quote(name))
}

func (p *pancakeSqlQueryGenerator) generateSimpleTopHitsQuery(topHits *pancakeModelMetricAggregation,
	whereClause model.Expr,
	topHitsQueryType metrics_aggregations.TopHits) (*model.SelectCommand, error) {

	newSelects := make([]model.AliasedExpr, 0, len(topHits.selectedColumns))
	for topHitsIdx, selectedTopHits := range topHits.selectedColumns {
		aliasedColumnName := fmt.Sprintf("%s%d", topHits.InternalColumnNamePrefix(), topHitsIdx+1)
		aliasedColumn := model.NewAliasedExpr(selectedTopHits, aliasedColumnName)
		newSelects = append(newSelects, aliasedColumn)
	}

	resultQuery := &model.SelectCommand{
		Columns:     p.aliasedExprArrayToExpr(newSelects),
		FromClause:  model.NewTableRef(model.SingleTableNamePlaceHolder),
		WhereClause: whereClause,
		OrderBy:     topHitsQueryType.OrderBy,
		Limit:       topHitsQueryType.Size,
	}

	return resultQuery, nil
}

func (p *pancakeSqlQueryGenerator) generateTopHitsQuery(aggregation *pancakeModel,
	combinatorWhere []model.Expr,
	topHits *pancakeModelMetricAggregation,
	groupBys []model.AliasedExpr,
	selectColumns []model.AliasedExpr,
	origQuery *model.SelectCommand) (*model.SelectCommand, error) {

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

	if len(groupBys) == 0 {
		return p.generateSimpleTopHitsQuery(topHits, whereClause, topHitsQueryType)
	}

	groupTableName := "group_table"
	hitTableName := "hit_table"

	hitTableLiteral := func(reference string) model.Expr {
		return model.NewLiteral(strconv.Quote(hitTableName) + "." + strconv.Quote(reference))
	}
	groupTableLiteral := func(reference string) model.Expr {
		return model.NewLiteral(strconv.Quote(groupTableName) + "." + strconv.Quote(reference))
	}

	convertColumnRefToHitTable := func(expr model.Expr) model.Expr {
		switch exprTyped := expr.(type) {
		case model.ColumnRef:
			// TODO: hack alert, we treat geo here in unique way
			if strings.HasSuffix(exprTyped.ColumnName, "Location") {
				return model.NewFunction("map",
					model.NewLiteral("'lat'"),
					hitTableLiteral(exprTyped.ColumnName+"::lat"),
					model.NewLiteral("'lon'"),
					hitTableLiteral(exprTyped.ColumnName+"::lon"),
				)
			}
			return model.ColumnRef{
				OptPrefixTable: hitTableName,
				ColumnName:     exprTyped.ColumnName,
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
		aliasedColumnName := fmt.Sprintf("%s%d", topHits.InternalColumnNamePrefix(), topHitsIdx+1)
		withConvertedHitTable := convertColumnRefToHitTable(selectedTopHits)
		aliasedColumn := model.NewAliasedExpr(withConvertedHitTable, aliasedColumnName)
		newSelects = append(newSelects, aliasedColumn)
	}

	// TODO: we need to test order by here
	rankSelect := model.NewAliasedExpr(
		model.NewWindowFunction("ROW_NUMBER", []model.Expr{}, partitionByExprs, topHitsQueryType.OrderBy),
		"top_hits_rank")
	newSelects = append(newSelects, rankSelect)

	joinQuery := model.SelectCommand{
		Columns:     append(p.aliasedExprArrayToExpr(newSelects), selectsForOrderBy...),
		FromClause:  fromClause,
		WhereClause: whereClause,
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
			model.NewLiteral(strconv.Itoa(topHitsQueryType.Size))),
		OrderBy:   orderBy,
		NamedCTEs: namedCte,
	}

	return resultQuery, nil
}
