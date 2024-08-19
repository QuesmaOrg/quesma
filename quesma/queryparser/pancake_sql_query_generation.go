// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package queryparser

import (
	"errors"
	"fmt"
	"quesma/clickhouse"
	"quesma/model"
	"quesma/queryparser/query_util"
	"strconv"
)

type pancakeSqlQueryGenerator struct {
}

func (p *pancakeSqlQueryGenerator) newQuotedLiteral(value string) model.LiteralExpr {
	return model.LiteralExpr{Value: strconv.Quote(value)}
}

func (p *pancakeSqlQueryGenerator) aliasedExprArrayToExpr(aliasedExprs []model.AliasedExpr) []model.Expr {
	exprs := make([]model.Expr, 0, len(aliasedExprs))
	for _, aliasedExpr := range aliasedExprs {
		exprs = append(exprs, aliasedExpr)
	}
	return exprs
}

func (p *pancakeSqlQueryGenerator) aliasedExprArrayToLiteralExpr(aliasedExprs []model.AliasedExpr) []model.Expr {
	exprs := make([]model.Expr, 0, len(aliasedExprs))
	for _, aliasedExpr := range aliasedExprs {
		exprs = append(exprs, p.newQuotedLiteral(aliasedExpr.Alias))
	}
	return exprs
}

func (p *pancakeSqlQueryGenerator) generatePartitionBy(groupByColumns []model.AliasedExpr) []model.Expr {
	partitionBy := make([]model.Expr, 0)
	for _, col := range groupByColumns {
		partitionBy = append(partitionBy, p.newQuotedLiteral(col.Alias))
	}
	return partitionBy
}

// TODO: Implement more if needed.
func (p *pancakeSqlQueryGenerator) generateAccumAggrFunctions(origExpr model.Expr, queryType model.QueryType) (accumExpr model.Expr, aggrFuncName string, err error) {
	switch origFunc := origExpr.(type) {
	case model.FunctionExpr:
		switch origFunc.Name {
		case "sum", "sumOrNull", "min", "minOrNull", "max", "maxOrNull":
			return origExpr, origFunc.Name, nil
		case "count", "countIf":
			return model.NewFunction(origFunc.Name, origFunc.Args...), "sum", nil
		case "avg", "avgOrNull", "varPop", "varSamp", "stddevPop", "stddevSamp", "uniq":
			// TODO: I debate whether make that default
			// This is ClickHouse specific: https://clickhouse.com/docs/en/sql-reference/aggregate-functions/combinators
			return model.NewFunction(origFunc.Name+"State", origFunc.Args...), origFunc.Name + "Merge", nil
		}
	}
	debugQueryType := "<nil>"
	if queryType != nil {
		debugQueryType = queryType.String()
	}
	return nil, "",
		fmt.Errorf("not implemented, queryType: %s, origExpr: %s", debugQueryType, model.AsString(origExpr))
}

func (p *pancakeSqlQueryGenerator) generateMetricSelects(metric *pancakeModelMetricAggregation, groupByColumns []model.AliasedExpr, hasMoreBucketAggregations bool) (addSelectColumns []model.AliasedExpr, err error) {
	for columnId, column := range metric.selectedColumns {
		aliasedName := fmt.Sprintf("%s_col_%d", metric.internalName, columnId)
		finalColumn := column

		if hasMoreBucketAggregations {
			partColumn, aggFunctionName, err := p.generateAccumAggrFunctions(column, metric.queryType)
			if err != nil {
				return nil, err
			}
			finalColumn = model.WindowFunction{Name: aggFunctionName,
				Args:        []model.Expr{partColumn},
				PartitionBy: p.generatePartitionBy(groupByColumns),
				OrderBy:     []model.OrderByExpr{},
			}
		}
		aliasedColumn := model.AliasedExpr{Expr: finalColumn, Alias: aliasedName}
		addSelectColumns = append(addSelectColumns, aliasedColumn)
	}
	return
}

func (p *pancakeSqlQueryGenerator) isPartOfGroupBy(column model.Expr, groupByColumns []model.AliasedExpr) *model.AliasedExpr {
	for _, groupByColumn := range groupByColumns {
		if model.PartlyImplementedIsEqual(column, groupByColumn) {
			return &groupByColumn
		}
	}
	return nil
}

func (p *pancakeSqlQueryGenerator) addPotentialParentCount(bucketAggregation *pancakeModelBucketAggregation, groupByColumns []model.AliasedExpr) []model.AliasedExpr {
	if query_util.IsAnyKindOfTerms(bucketAggregation.queryType) {
		parentCountColumn := model.WindowFunction{Name: "sum",
			Args:        []model.Expr{model.NewFunction("count", model.NewLiteral("*"))},
			PartitionBy: p.generatePartitionBy(groupByColumns),
			OrderBy:     []model.OrderByExpr{},
		}
		parentCountAliasedColumn := model.AliasedExpr{Expr: parentCountColumn, Alias: bucketAggregation.InternalNameForParentCount()}
		return []model.AliasedExpr{parentCountAliasedColumn}
	}
	return []model.AliasedExpr{}
}

func (p *pancakeSqlQueryGenerator) generateBucketSqlParts(bucketAggregation *pancakeModelBucketAggregation, groupByColumns []model.AliasedExpr, hasMoreBucketAggregations bool) (
	addSelectColumns, addGroupBys, addRankColumns []model.AliasedExpr, addRankWheres []model.Expr, addRankOrderBys []model.OrderByExpr, err error) {

	// For some group by such as terms, we need total count. We add it in this method.
	addSelectColumns = append(addSelectColumns, p.addPotentialParentCount(bucketAggregation, groupByColumns)...)

	// TODO: ...
	for columnId, column := range bucketAggregation.selectedColumns {
		aliasedColumn := model.AliasedExpr{Expr: column, Alias: bucketAggregation.InternalNameForKey(columnId)}
		addSelectColumns = append(addSelectColumns, aliasedColumn)
		addGroupBys = append(addGroupBys, aliasedColumn)
	}

	// build count for aggr
	var countColumn model.Expr
	if hasMoreBucketAggregations {
		partCountColumn := model.NewFunction("count", model.NewLiteral("*"))

		countColumn = model.WindowFunction{Name: "sum",
			Args:        []model.Expr{partCountColumn},
			PartitionBy: p.generatePartitionBy(append(groupByColumns, addGroupBys...)), /// TODO
			OrderBy:     []model.OrderByExpr{},
		}
	} else {
		countColumn = model.NewFunction("count", model.NewLiteral("*"))
	}
	countAliasedColumn := model.AliasedExpr{Expr: countColumn, Alias: bucketAggregation.InternalNameForCount()}
	addSelectColumns = append(addSelectColumns, countAliasedColumn)

	if bucketAggregation.orderBy != nil && len(bucketAggregation.orderBy) > 0 {
		rankColumnOrderBy := make([]model.OrderByExpr, 0)

		for i, orderBy := range bucketAggregation.orderBy {
			columnId := len(bucketAggregation.selectedColumns) + i
			orderByExpr := orderBy.Exprs[0]

			partOfGroupByOpt := p.isPartOfGroupBy(orderByExpr, append(groupByColumns, addGroupBys...))
			if partOfGroupByOpt != nil {
				direction := orderBy.Direction
				if direction == model.DefaultOrder {
					direction = model.AscOrder // primarily needed for tests
				}
				rankColumnOrderBy = append(rankColumnOrderBy, model.NewOrderByExpr(
					[]model.Expr{p.newQuotedLiteral(partOfGroupByOpt.Alias)}, direction))
				continue
			}

			if hasMoreBucketAggregations {
				partColumn, aggFunctionName, err := p.generateAccumAggrFunctions(orderByExpr, nil)
				if err != nil {
					return nil, nil, nil, nil, nil, err
				}
				orderByExpr = model.WindowFunction{Name: aggFunctionName,
					Args:        []model.Expr{partColumn},
					PartitionBy: p.generatePartitionBy(append(groupByColumns, addGroupBys...)),
					OrderBy:     []model.OrderByExpr{},
				}
			}
			aliasedColumn := model.AliasedExpr{Expr: orderByExpr, Alias: bucketAggregation.InternalNameForOrderBy(columnId)}
			addSelectColumns = append(addSelectColumns, aliasedColumn)

			rankColumnOrderBy = append(rankColumnOrderBy, model.NewOrderByExpr(
				[]model.Expr{p.newQuotedLiteral(aliasedColumn.Alias)}, orderBy.Direction))
		}

		// We order by count, but add key to get right dense_rank()
		for _, addedGroupByAlias := range p.aliasedExprArrayToLiteralExpr(addGroupBys) {
			alreadyAdded := false
			for _, orderBy := range rankColumnOrderBy {
				if toAdd, ok := addedGroupByAlias.(model.LiteralExpr); ok {
					if added, ok2 := orderBy.Exprs[0].(model.LiteralExpr); ok2 {
						if added.Value == toAdd.Value {
							alreadyAdded = true
							break
						}
					}
				}
			}
			if !alreadyAdded {
				rankColumnOrderBy = append(rankColumnOrderBy, model.NewOrderByExpr([]model.Expr{addedGroupByAlias}, model.AscOrder))
			}
		}

		rankColum := model.WindowFunction{Name: "dense_rank",
			Args:        []model.Expr{},
			PartitionBy: p.generatePartitionBy(groupByColumns),
			OrderBy:     rankColumnOrderBy,
		}
		aliasedRank := model.AliasedExpr{Expr: rankColum, Alias: bucketAggregation.InternalNameForOrderBy(1) + "_rank"}
		addRankColumns = append(addRankColumns, aliasedRank)

		if bucketAggregation.limit != pancakeBucketAggregationNoLimit {
			// if where not null, increase limit by 1
			limit := bucketAggregation.limit
			if bucketAggregation.filterOurEmptyKeyBucket {
				limit += 1
			}
			whereRank := model.NewInfixExpr(p.newQuotedLiteral(aliasedRank.Alias), "<=", model.NewLiteral(limit))
			addRankWheres = append(addRankWheres, whereRank)
		}

		rankOrderBy := model.NewOrderByExpr([]model.Expr{p.newQuotedLiteral(aliasedRank.Alias)}, model.AscOrder)
		addRankOrderBys = append(addRankOrderBys, rankOrderBy)
	}
	return
}

// TODO: deduplicate metric names
func (p *pancakeSqlQueryGenerator) generateSelectCommand(aggregation *pancakeModel, table *clickhouse.Table) (*model.SelectCommand, bool, error) {
	if aggregation == nil {
		return nil, false, errors.New("aggregation is nil in generateQuery")
	}

	selectColumns := make([]model.AliasedExpr, 0)
	rankColumns := make([]model.AliasedExpr, 0)
	rankWheres := make([]model.Expr, 0)
	rankOrderBys := make([]model.OrderByExpr, 0)
	groupBys := make([]model.AliasedExpr, 0)
	for layerId, layer := range aggregation.layers {
		hasMoreBucketAggregations := layerId+1 < len(aggregation.layers)

		for _, metric := range layer.currentMetricAggregations {
			addSelectColumns, err := p.generateMetricSelects(metric, groupBys, hasMoreBucketAggregations)
			if err != nil {
				return nil, false, err
			}
			selectColumns = append(selectColumns, addSelectColumns...)
		}

		if layer.nextBucketAggregation != nil {
			hasMoreBucketAggregations = hasMoreBucketAggregations && aggregation.layers[layerId+1].nextBucketAggregation != nil
			addSelectColumns, addGroupBys, addRankColumns, addRankWheres, addRankOrderBys, err :=
				p.generateBucketSqlParts(layer.nextBucketAggregation, groupBys, hasMoreBucketAggregations)
			if err != nil {
				return nil, false, err
			}
			selectColumns = append(selectColumns, addSelectColumns...)
			groupBys = append(groupBys, addGroupBys...)
			rankColumns = append(rankColumns, addRankColumns...)
			rankWheres = append(rankWheres, addRankWheres...)
			rankOrderBys = append(rankOrderBys, addRankOrderBys...)
		}
	}

	// if we have single layer we can emit simpler query
	if len(aggregation.layers) == 1 || len(aggregation.layers) == 2 && aggregation.layers[1].nextBucketAggregation == nil {
		limit := 0
		orderBy := make([]model.OrderByExpr, 0)
		if aggregation.layers[0].nextBucketAggregation != nil {
			limit = aggregation.layers[0].nextBucketAggregation.limit
			// if where not null, increase limit by 1
			if aggregation.layers[0].nextBucketAggregation.filterOurEmptyKeyBucket {
				if limit != 0 {
					limit += 1
				}
			}

			if len(rankColumns) > 0 {
				orderBy = rankColumns[0].Expr.(model.WindowFunction).OrderBy
			}
		}

		query := model.SelectCommand{
			Columns:     p.aliasedExprArrayToExpr(selectColumns),
			GroupBy:     p.aliasedExprArrayToExpr(groupBys),
			WhereClause: aggregation.whereClause,
			FromClause:  model.NewTableRef(table.FullTableName()),
			OrderBy:     orderBy,
			Limit:       limit,
		}
		return &query, false, nil
	}

	windowCte := model.SelectCommand{
		Columns:     p.aliasedExprArrayToExpr(selectColumns),
		GroupBy:     p.aliasedExprArrayToExpr(groupBys),
		WhereClause: aggregation.whereClause,
		FromClause:  model.NewTableRef(table.FullTableName()),
		SampleLimit: aggregation.sampleLimit,
	}

	rankCte := model.SelectCommand{
		Columns:    append(p.aliasedExprArrayToLiteralExpr(selectColumns), p.aliasedExprArrayToExpr(rankColumns)...),
		FromClause: windowCte,
	}

	finalQuery := model.SelectCommand{
		Columns:     p.aliasedExprArrayToLiteralExpr(selectColumns),
		FromClause:  rankCte,
		WhereClause: model.And(rankWheres),
		OrderBy:     rankOrderBys,
	}

	return &finalQuery, true, nil
}

func (p *pancakeSqlQueryGenerator) generateQuery(aggregation *pancakeModel, table *clickhouse.Table) (*model.Query, error) {
	if aggregation == nil {
		return nil, errors.New("aggregation is nil in generateQuery")
	}

	resultSelectCommand, isFullPancake, err := p.generateSelectCommand(aggregation, table)
	if err != nil {
		return nil, err
	}

	resultQuery := &model.Query{
		SelectCommand: *resultSelectCommand,
		TableName:     table.FullTableName(),
		Type:          PancakeQueryType{pancakeAggregation: aggregation},
		OptimizeHints: model.NewQueryExecutionHints(),
	}

	if isFullPancake {
		resultQuery.OptimizeHints.OptimizationsPerformed = append(resultQuery.OptimizeHints.OptimizationsPerformed, PancakeOptimizerName)
	} else {
		resultQuery.OptimizeHints.OptimizationsPerformed = append(resultQuery.OptimizeHints.OptimizationsPerformed, PancakeOptimizerName+"(half)")
	}

	return resultQuery, nil
}
