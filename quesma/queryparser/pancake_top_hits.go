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
					model.NewLiteral(strconv.Quote(groupTableName)+"."+orderByLiteral.Value.(string)),
					unquoted))
			}
		} else {
			panic("all pancake orderBy are on aliases, so we should have LiteralExpr here")
		}
	}

	for topHitsIdx, selectedTopHits := range topHits.selectedColumns {
		aliasedColumnName := fmt.Sprintf("top_hits_%d", topHitsIdx+1)
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
