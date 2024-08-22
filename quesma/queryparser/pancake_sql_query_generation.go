// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package queryparser

import (
	"errors"
	"fmt"
	"quesma/clickhouse"
	"quesma/model"
	"quesma/model/bucket_aggregations"
	"quesma/queryparser/query_util"
)

type pancakeSqlQueryGenerator struct {
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
		exprs = append(exprs, aliasedExpr.AliasRef())
	}
	return exprs
}

func (p *pancakeSqlQueryGenerator) generatePartitionBy(groupByColumns []model.AliasedExpr) []model.Expr {
	partitionBy := make([]model.Expr, 0)
	for _, col := range groupByColumns {
		partitionBy = append(partitionBy, col.AliasRef())
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
			finalColumn = model.NewWindowFunction(aggFunctionName, []model.Expr{partColumn},
				p.generatePartitionBy(groupByColumns), []model.OrderByExpr{})
		}
		aliasedColumn := model.NewAliasedExpr(finalColumn, aliasedName)
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

func (p *pancakeSqlQueryGenerator) isPartOfOrderBy(alias model.AliasedExpr, orderByColumns []model.OrderByExpr) bool {
	for _, orderBy := range orderByColumns {
		if orderByLiteral, ok := orderBy.Expr.(model.LiteralExpr); ok {
			if alias.AliasRef().Value == orderByLiteral.Value {
				return true
			}
		}
	}
	return false
}

func (p *pancakeSqlQueryGenerator) addPotentialParentCount(bucketAggregation *pancakeModelBucketAggregation, groupByColumns []model.AliasedExpr) []model.AliasedExpr {
	if query_util.IsAnyKindOfTerms(bucketAggregation.queryType) {
		parentCountColumn := model.NewWindowFunction("sum",
			[]model.Expr{model.NewFunction("count", model.NewLiteral("*"))},
			p.generatePartitionBy(groupByColumns), []model.OrderByExpr{})
		parentCountAliasedColumn := model.NewAliasedExpr(parentCountColumn, bucketAggregation.InternalNameForParentCount())
		return []model.AliasedExpr{parentCountAliasedColumn}
	}
	return []model.AliasedExpr{}
}

func (p *pancakeSqlQueryGenerator) generateBucketSqlParts(bucketAggregation *pancakeModelBucketAggregation, groupByColumns []model.AliasedExpr, hasMoreBucketAggregations bool) (
	addSelectColumns, addGroupBys, addRankColumns []model.AliasedExpr, addRankWheres []model.Expr, addRankOrderBys []model.OrderByExpr, err error) {

	// For some group by such as terms, we need total count. We add it in this method.
	addSelectColumns = append(addSelectColumns, p.addPotentialParentCount(bucketAggregation, groupByColumns)...)

	for columnId, column := range bucketAggregation.selectedColumns {
		aliasedColumn := model.NewAliasedExpr(column, bucketAggregation.InternalNameForKey(columnId))
		addSelectColumns = append(addSelectColumns, aliasedColumn)
		addGroupBys = append(addGroupBys, aliasedColumn)
	}

	// build count for aggr
	var countColumn model.Expr
	if hasMoreBucketAggregations {
		partCountColumn := model.NewFunction("count", model.NewLiteral("*"))

		countColumn = model.NewWindowFunction("sum", []model.Expr{partCountColumn},
			p.generatePartitionBy(append(groupByColumns, addGroupBys...)), []model.OrderByExpr{})
	} else {
		countColumn = model.NewFunction("count", model.NewLiteral("*"))
	}
	countAliasedColumn := model.NewAliasedExpr(countColumn, bucketAggregation.InternalNameForCount())
	addSelectColumns = append(addSelectColumns, countAliasedColumn)

	if bucketAggregation.orderBy != nil && len(bucketAggregation.orderBy) > 0 {
		rankOrderBy := make([]model.OrderByExpr, 0)

		for i, orderBy := range bucketAggregation.orderBy {
			columnId := len(bucketAggregation.selectedColumns) + i
			direction := orderBy.Direction

			rankColumn := p.isPartOfGroupBy(orderBy.Expr, append(groupByColumns, addGroupBys...))
			if rankColumn != nil { // rank is part of group by
				if direction == model.DefaultOrder {
					direction = model.AscOrder // primarily needed for tests
				}
			} else { // we need new columns for rank
				orderByExpr := orderBy.Expr
				if hasMoreBucketAggregations {
					partColumn, aggFunctionName, err := p.generateAccumAggrFunctions(orderByExpr, nil)
					if err != nil {
						return nil, nil, nil, nil, nil, err
					}
					orderByExpr = model.NewWindowFunction(aggFunctionName, []model.Expr{partColumn},
						p.generatePartitionBy(append(groupByColumns, addGroupBys...)), []model.OrderByExpr{})
				}
				aliasedExpr := model.NewAliasedExpr(orderByExpr, bucketAggregation.InternalNameForOrderBy(columnId))
				addSelectColumns = append(addSelectColumns, aliasedExpr)
				rankColumn = &aliasedExpr
			}
			rankOrderBy = append(rankOrderBy, model.NewOrderByExpr(rankColumn.AliasRef(), direction))
		}

		// We order by count, but add key to get right dense_rank()
		for _, addedGroupByAlias := range addGroupBys {
			if !p.isPartOfOrderBy(addedGroupByAlias, rankOrderBy) {
				rankOrderBy = append(rankOrderBy, model.NewOrderByExpr(addedGroupByAlias.AliasRef(), model.AscOrder))
			}
		}

		rankColumn := model.NewWindowFunction("dense_rank", []model.Expr{},
			p.generatePartitionBy(groupByColumns), rankOrderBy)
		aliasedRank := model.NewAliasedExpr(rankColumn, bucketAggregation.InternalNameForOrderBy(1)+"_rank")
		addRankColumns = append(addRankColumns, aliasedRank)

		if bucketAggregation.limit != pancakeBucketAggregationNoLimit {
			// if where not null, increase limit by 1
			limit := bucketAggregation.limit
			if bucketAggregation.filterOurEmptyKeyBucket {
				limit += 1
			}
			whereRank := model.NewInfixExpr(aliasedRank.AliasRef(), "<=", model.NewLiteral(limit))
			addRankWheres = append(addRankWheres, whereRank)
		}

		addRankOrderBys = append(addRankOrderBys, model.NewOrderByExpr(aliasedRank.AliasRef(), model.AscOrder))
	}
	return
}

func (p *pancakeSqlQueryGenerator) generateLeafFilter(layer *pancakeModelLayer, whereClause model.Expr) (addSelectColumns []model.AliasedExpr, err error) {
	if layer == nil { // no metric aggregations in filter
		return nil, nil
	}
	if layer.nextBucketAggregation != nil {
		return nil, errors.New("filter layer can't have sub bucket aggregations")
	}

	for _, metric := range layer.currentMetricAggregations {
		for columnId, column := range metric.selectedColumns {
			aliasedName := fmt.Sprintf("%s_col_%d", metric.internalName, columnId)
			// Add if
			var columnWithIf model.Expr
			switch function := column.(type) {
			case model.FunctionExpr:
				if function.Name == "count" {
					columnWithIf = model.NewFunction("countIf", whereClause)
				} else if len(function.Args) == 1 {
					// https://clickhouse.com/docs/en/sql-reference/aggregate-functions/combinators#-if
					columnWithIf = model.NewFunction(function.Name+"If", function.Args[0], whereClause)
				} else {
					return nil, fmt.Errorf("not implemented -iF for func with more than one argument: %s", model.AsString(column))
				}
			default:
				return nil, fmt.Errorf("not implemented -iF for expr: %s", model.AsString(column))
			}

			aliasedColumn := model.NewAliasedExpr(columnWithIf, aliasedName)
			addSelectColumns = append(addSelectColumns, aliasedColumn)
		}
	}
	return
}

func (p *pancakeSqlQueryGenerator) countRealBucketAggregations(aggregation *pancakeModel) int {
	bucketAggregationCount := 0
	for _, layer := range aggregation.layers {
		if layer.nextBucketAggregation != nil {
			if layer.nextBucketAggregation.DoesHaveGroupBy() {
				bucketAggregationCount++
			}
		}
	}
	return bucketAggregationCount
}

func (p *pancakeSqlQueryGenerator) generateSelectCommand(aggregation *pancakeModel, table *clickhouse.Table) (*model.SelectCommand, bool, error) {
	if aggregation == nil {
		return nil, false, errors.New("aggregation is nil in generateQuery")
	}

	bucketAggregationCount := p.countRealBucketAggregations(aggregation)
	bucketAggregationSoFar := 0

	selectColumns := make([]model.AliasedExpr, 0)
	rankColumns := make([]model.AliasedExpr, 0)
	rankWheres := make([]model.Expr, 0)
	rankOrderBys := make([]model.OrderByExpr, 0)
	groupBys := make([]model.AliasedExpr, 0)
	for layerId, layer := range aggregation.layers {
		for _, metric := range layer.currentMetricAggregations {
			hasMoreBucketAggregations := bucketAggregationSoFar < bucketAggregationCount
			addSelectColumns, err := p.generateMetricSelects(metric, groupBys, hasMoreBucketAggregations)
			if err != nil {
				return nil, false, err
			}
			selectColumns = append(selectColumns, addSelectColumns...)
		}

		if layer.nextBucketAggregation != nil {
			if filter, isFilter := layer.nextBucketAggregation.queryType.(bucket_aggregations.FilterAgg); isFilter {

				for i, newFilterColumn := range layer.nextBucketAggregation.selectedColumns {
					aliasName := fmt.Sprintf("%s_col_%d", layer.nextBucketAggregation.internalName, i)
					aliasedColumn := model.NewAliasedExpr(newFilterColumn, aliasName)
					selectColumns = append(selectColumns, aliasedColumn)
				}

				if layerId+1 < len(aggregation.layers) {
					addSelectColumns, err := p.generateLeafFilter(aggregation.layers[layerId+1], filter.WhereClause)
					if err != nil {
						return nil, false, err
					}
					selectColumns = append(selectColumns, addSelectColumns...)
				}
				break
			}

			if layer.nextBucketAggregation.DoesHaveGroupBy() {
				bucketAggregationSoFar += 1
			}
			hasMoreBucketAggregations := bucketAggregationSoFar < bucketAggregationCount
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
	if bucketAggregationCount <= 1 {
		limit := 0
		for _, layer := range aggregation.layers {
			if layer.nextBucketAggregation != nil && layer.nextBucketAggregation.DoesHaveGroupBy() {
				limit = layer.nextBucketAggregation.limit
				// if where not null, increase limit by 1
				if layer.nextBucketAggregation.filterOurEmptyKeyBucket {
					if limit != 0 {
						limit += 1
					}
				}
			}
		}

		orderBy := make([]model.OrderByExpr, 0)
		if len(rankColumns) > 0 {
			orderBy = rankColumns[0].Expr.(model.WindowFunction).OrderBy
		}

		query := model.SelectCommand{
			Columns:     p.aliasedExprArrayToExpr(selectColumns),
			GroupBy:     p.aliasedExprArrayToExpr(groupBys),
			WhereClause: aggregation.whereClause,
			FromClause:  model.NewTableRef(table.FullTableName()),
			OrderBy:     orderBy,
			Limit:       limit,
			SampleLimit: aggregation.sampleLimit,
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
