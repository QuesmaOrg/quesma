// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package queryparser

import (
	"errors"
	"fmt"
	"quesma/model"
	"quesma/model/bucket_aggregations"
	"quesma/model/metrics_aggregations"
	"quesma/queryparser/query_util"
	"strconv"
	"strings"
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

func (p *pancakeSqlQueryGenerator) addIfCombinator(column model.Expr, whereClause model.Expr) (model.Expr, error) {
	switch function := column.(type) {
	case model.FunctionExpr:
		splits := strings.SplitN(function.Name, "(", 2)
		baseFunctionName := splits[0]
		functionSuffix := ""
		if len(splits) > 1 {
			functionSuffix = "(" + splits[1]
		}

		if function.Name == "count" {
			return model.NewFunction("countIf", whereClause), nil
		} else if strings.HasSuffix(baseFunctionName, "If") && len(function.Args) > 0 {
			newArgs := make([]model.Expr, len(function.Args))
			copy(newArgs, function.Args)
			newArgs[len(newArgs)-1] = model.And([]model.Expr{newArgs[len(newArgs)-1], whereClause})
			return model.NewFunction(function.Name, newArgs...), nil
		} else if len(function.Args) == 1 {
			// https://clickhouse.com/docs/en/sql-reference/aggregate-functions/combinators#-if
			return model.NewFunction(baseFunctionName+"If"+functionSuffix, function.Args[0], whereClause), nil
		} else {
			return nil, fmt.Errorf("not implemented -iF for func with more than one argument: %s", model.AsString(column))
		}
	case model.AliasedExpr:
		// TODO: maybe preserve alias
		return p.addIfCombinator(function.Expr, whereClause)
	case model.WindowFunction:
		newArgs := make([]model.Expr, 0, len(function.Args))
		for _, arg := range function.Args {
			newArg, err := p.addIfCombinator(arg, whereClause)
			if err != nil {
				return nil, err
			}
			newArgs = append(newArgs, newArg)
		}
		newWindow := model.WindowFunction{
			Name:        function.Name,
			Args:        newArgs,
			PartitionBy: function.PartitionBy,
			OrderBy:     function.OrderBy,
		}
		return newWindow, nil
	default:
		return nil, fmt.Errorf("not implemented -iF for expr: %s %T", model.AsString(column), column)
	}
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

func (p *pancakeSqlQueryGenerator) generateSelectCommand(aggregation *pancakeModel) (resultQuery *model.SelectCommand, optimizerName string, err error) {
	if aggregation == nil {
		return nil, "", errors.New("aggregation is nil in generateQuery")
	}

	bucketAggregationCount := p.countRealBucketAggregations(aggregation)
	bucketAggregationSoFar := 0

	selectColumns := make([]model.AliasedExpr, 0)
	rankColumns := make([]model.AliasedExpr, 0)
	rankWheres := make([]model.Expr, 0)
	rankOrderBys := make([]model.OrderByExpr, 0)
	groupBys := make([]model.AliasedExpr, 0)

	type addIfCombinator struct {
		selectNr  int
		queryType bucket_aggregations.CombinatorAggregationInterface
	}
	addIfCombinators := make([]addIfCombinator, 0)

	for _, layer := range aggregation.layers {
		for _, metric := range layer.currentMetricAggregations {
			hasMoreBucketAggregations := bucketAggregationSoFar < bucketAggregationCount
			addSelectColumns, err := p.generateMetricSelects(metric, groupBys, hasMoreBucketAggregations)
			if err != nil {
				return nil, "", err
			}
			selectColumns = append(selectColumns, addSelectColumns...)
		}

		if layer.nextBucketAggregation != nil {
			if combinator, isCombinator := layer.nextBucketAggregation.queryType.(bucket_aggregations.CombinatorAggregationInterface); isCombinator {
				addIfCombinators = append(addIfCombinators, addIfCombinator{len(selectColumns), combinator})
			}

			if layer.nextBucketAggregation.DoesHaveGroupBy() {
				bucketAggregationSoFar += 1
			}
			hasMoreBucketAggregations := bucketAggregationSoFar < bucketAggregationCount
			addSelectColumns, addGroupBys, addRankColumns, addRankWheres, addRankOrderBys, err :=
				p.generateBucketSqlParts(layer.nextBucketAggregation, groupBys, hasMoreBucketAggregations)
			if err != nil {
				return nil, "", err
			}
			selectColumns = append(selectColumns, addSelectColumns...)
			groupBys = append(groupBys, addGroupBys...)
			rankColumns = append(rankColumns, addRankColumns...)
			rankWheres = append(rankWheres, addRankWheres...)
			rankOrderBys = append(rankOrderBys, addRankOrderBys...)
		}
	}

	// process combinators, e.g. filter, filters, range and dataRange
	// this change selects by adding -If suffix, e.g. count(*) -> countIf(response_time < 1000)
	// they may also add more columns with different prefix and where clauses
	for i := len(addIfCombinators) - 1; i >= 0; i-- { // reverse order is important
		combinator := addIfCombinators[i]
		selectsBefore := selectColumns[:combinator.selectNr]
		selectsAfter := selectColumns[combinator.selectNr:]
		newAfterSelects := make([]model.AliasedExpr, 0, len(selectsAfter))

		for _, subGroup := range combinator.queryType.CombinatorGroups() {
			for _, selectAfter := range selectsAfter {
				var withCombinator model.Expr
				if p.isPartOfGroupBy(selectAfter.Expr, groupBys) != nil {
					withCombinator = selectAfter.Expr
				} else {
					withIfCombinator, err := p.addIfCombinator(selectAfter.Expr, subGroup.WhereClause)
					if err != nil {
						return nil, "", err
					}
					withCombinator = withIfCombinator
				}
				aliasedColumn := model.NewAliasedExpr(withCombinator,
					fmt.Sprintf("%s%s", subGroup.Prefix, selectAfter.Alias))
				newAfterSelects = append(newAfterSelects, aliasedColumn)
			}
		}
		selectColumns = append(selectsBefore, newAfterSelects...)
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

		resultQuery = &model.SelectCommand{
			Columns:     p.aliasedExprArrayToExpr(selectColumns),
			GroupBy:     p.aliasedExprArrayToExpr(groupBys),
			WhereClause: aggregation.whereClause,
			FromClause:  model.NewTableRef(model.SingleTableNamePlaceHolder),
			OrderBy:     orderBy,
			Limit:       limit,
			SampleLimit: aggregation.sampleLimit,
		}
		optimizerName = PancakeOptimizerName + "(half)"
	} else {
		windowCte := model.SelectCommand{
			Columns:     p.aliasedExprArrayToExpr(selectColumns),
			GroupBy:     p.aliasedExprArrayToExpr(groupBys),
			WhereClause: aggregation.whereClause,
			FromClause:  model.NewTableRef(model.SingleTableNamePlaceHolder),
			SampleLimit: aggregation.sampleLimit,
		}

		rankCte := model.SelectCommand{
			Columns:    append(p.aliasedExprArrayToLiteralExpr(selectColumns), p.aliasedExprArrayToExpr(rankColumns)...),
			FromClause: windowCte,
		}

		resultQuery = &model.SelectCommand{
			Columns:     p.aliasedExprArrayToLiteralExpr(selectColumns),
			FromClause:  rankCte,
			WhereClause: model.And(rankWheres),
			OrderBy:     rankOrderBys,
		}
		optimizerName = PancakeOptimizerName
	}

	if aggregation.optTopHits != nil {
		// TODO: we assume some group bys
		topHits := aggregation.optTopHits
		var topHitsQueryType metrics_aggregations.TopHits
		if queryType, ok := topHits.queryType.(metrics_aggregations.TopHits); ok {
			topHitsQueryType = queryType
		} else {
			return nil, "", fmt.Errorf("expected top_hits query type, got: %T", topHits.queryType)
		}

		// TODO: add combinators if there exist
		whereClause := aggregation.whereClause

		groupTableName := "group_table"
		hitTableName := "hit_table"

		convertColumnRefToHitTable := func(expr model.Expr) model.Expr {
			switch exprTyped := expr.(type) {
			case model.ColumnRef:
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
		newSelects = append(newSelects, model.NewAliasedExpr(
			model.NewWindowFunction("ROW_NUMBER", []model.Expr{}, partitionByExprs, []model.OrderByExpr{}),
			"top_hits_rank"))

		joinQuery := model.SelectCommand{
			Columns: p.aliasedExprArrayToExpr(newSelects),
			// rank by hits
			FromClause:  fromClause,
			WhereClause: whereClause,
		}

		joinQueryName := "quesma_top_hits_join"

		namedCte := []*model.CTE{
			{
				Name:          topHitsSourceName,
				SelectCommand: resultQuery,
			},
			{
				Name:          joinQueryName,
				SelectCommand: &joinQuery,
			},
		}

		// TODO: Simplify
		resultQuery = &model.SelectCommand{
			Columns:    p.aliasedExprArrayToLiteralExpr(newSelects),
			FromClause: model.NewLiteral(joinQueryName),
			WhereClause: model.NewInfixExpr(
				model.NewLiteral("top_hits_rank"),
				"<=",
				model.NewLiteral(strconv.Itoa(topHitsQueryType.Size))),
			NamedCTEs: namedCte,
		}

		optimizerName = PancakeOptimizerName + "(with top_hits)"
	}

	return
}

func (p *pancakeSqlQueryGenerator) generateQuery(aggregation *pancakeModel) (*model.Query, error) {
	if aggregation == nil {
		return nil, errors.New("aggregation is nil in generateQuery")
	}

	resultSelectCommand, optimizerName, err := p.generateSelectCommand(aggregation)
	if err != nil {
		return nil, err
	}

	resultQuery := &model.Query{
		SelectCommand: *resultSelectCommand,
		Type:          PancakeQueryType{pancakeAggregation: aggregation},
		OptimizeHints: model.NewQueryExecutionHints(),
	}

	resultQuery.OptimizeHints.OptimizationsPerformed = append(resultQuery.OptimizeHints.OptimizationsPerformed, optimizerName)

	return resultQuery, nil
}
