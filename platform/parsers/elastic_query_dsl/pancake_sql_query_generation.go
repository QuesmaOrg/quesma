// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package elastic_query_dsl

import (
	"context"
	"errors"
	"fmt"
	"github.com/QuesmaOrg/quesma/platform/model"
	"github.com/QuesmaOrg/quesma/platform/model/bucket_aggregations"
	"github.com/QuesmaOrg/quesma/platform/model/metrics_aggregations"
	"github.com/QuesmaOrg/quesma/platform/parsers/elastic_query_dsl/query_util"
	"strings"
)

type pancakeSqlQueryGenerator struct {
	ctx context.Context
}

func newPancakeSqlQueryGeneratorr(ctx context.Context) *pancakeSqlQueryGenerator {
	return &pancakeSqlQueryGenerator{ctx: ctx}
}

func (p *pancakeSqlQueryGenerator) aliasedExprArrayToExpr(aliasedExprs []model.AliasedExpr) []model.Expr {
	exprs := make([]model.Expr, 0, len(aliasedExprs))
	for _, aliasedExpr := range aliasedExprs {
		exprs = append(exprs, aliasedExpr)
	}
	return exprs
}

func (p *pancakeSqlQueryGenerator) groupByExprArrayToExpr(groupExprs []model.GroupByExpr) []model.Expr {
	exprs := make([]model.Expr, 0, len(groupExprs))
	for _, groupExpr := range groupExprs {
		exprs = append(exprs, groupExpr)
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

func (p *pancakeSqlQueryGenerator) generatePartitionBy(groupByColumns []model.GroupByExpr, useGroupByColumn bool) []model.Expr {
	partitionBy := make([]model.Expr, 0)
	for _, col := range groupByColumns {
		if useGroupByColumn {
			if colRef, ok := col.Expr.(model.ColumnRef); ok {
				partitionBy = append(partitionBy, model.NewLiteral(colRef.ColumnName))
				continue
			}
		}
		partitionBy = append(partitionBy, col.GroupAliasRef())
	}
	return partitionBy
}

// TODO: Implement more if needed.
func (p *pancakeSqlQueryGenerator) generateAccumAggrFunctions(origExpr model.Expr, queryType model.QueryType) (accumExpr model.Expr, aggrFuncName string, err error) {
	switch origExprTyped := origExpr.(type) {
	case model.FunctionExpr:
		origFunc := origExprTyped
		switch origFunc.Name {
		case "sum", "sumOrNull", "min", "max":
			return origExpr, origFunc.Name, nil
		case "count", "countIf":
			return model.NewFunction(origFunc.Name, origFunc.Args...), "sum", nil
		case "avg", "avgOrNull", "varPop", "varSamp", "stddevPop", "stddevSamp", "uniq":
			// TODO: I debate whether make that default
			// This is ClickHouse specific: https://clickhouse.com/docs/en/sql-reference/aggregate-functions/combinators
			return model.NewFunction(origFunc.Name+"State", origFunc.Args...), origFunc.Name + "Merge", nil
		case "NDV":
			firstArg := origFunc.Args[0]
			var columnName string
			if colRef, ok := firstArg.(model.ColumnRef); ok {
				columnName = colRef.ColumnName
			}
			return model.NewFunction("APPROX_COUNT_DISTINCT", model.NewColumnRef(columnName)), "", nil
		}

		if strings.HasPrefix(origFunc.Name, "PERCENTILE_APPROX") {
			if len(origFunc.Args) < 2 {
				return nil, "", fmt.Errorf("invalid quantiles format, expected 2 arguments")
			}

			firstArg := origFunc.Args[0]
			var columnName string
			if colRef, ok := firstArg.(model.ColumnRef); ok {
				columnName = colRef.ColumnName
			}

			secondArg := origFunc.Args[1]
			var percentileValue float64
			if literal, ok := secondArg.(model.LiteralExpr); ok {
				percentileValue = literal.Value.(float64)
			}

			return model.NewFunction("PERCENTILE_APPROX", model.NewColumnRef(columnName), model.NewLiteral(percentileValue)), "", nil
		}
	case model.InfixExpr:
		origInfix := origExprTyped
		if f, ok := origInfix.Left.(model.FunctionExpr); ok && f.Name == "count" {
			return origInfix, "sum", nil
		}
	}
	debugQueryType := "<nil>"
	if queryType != nil {
		debugQueryType = queryType.String()
	}
	return nil, "",
		fmt.Errorf("not implemented, queryType: %s, origExpr: %s", debugQueryType, model.AsString(origExpr))
}

func (p *pancakeSqlQueryGenerator) generateMetricSelects(metric *pancakeModelMetricAggregation, groupByColumns []model.GroupByExpr, hasMoreBucketAggregations bool, bucketSelectedColumns map[string]struct{}) (addSelectColumns []model.AliasedExpr, err error) {
	for columnId, column := range metric.selectedColumns {
		finalColumn := column

		if hasMoreBucketAggregations {
			if len(bucketSelectedColumns) > 0 {
				if fun, ok := column.(model.FunctionExpr); ok {
					if columnRef, ok := fun.Args[0].(model.ColumnRef); ok {
						if _, exists := bucketSelectedColumns[columnRef.ColumnName]; !exists {
							continue
						}
					}
				}
			}
			partColumn, aggFunctionName, err := p.generateAccumAggrFunctions(column, metric.queryType)
			if err != nil {
				return nil, err
			}
			finalColumn = model.NewWindowFunction(aggFunctionName, []model.Expr{partColumn},
				p.generatePartitionBy(groupByColumns, false), []model.OrderByExpr{})
		}
		aliasedColumn := model.NewAliasedExpr(finalColumn, metric.InternalNameForCol(columnId))
		addSelectColumns = append(addSelectColumns, aliasedColumn)
	}
	return
}

func (p *pancakeSqlQueryGenerator) isPartOf(column model.Expr, aliasedColumns []model.GroupByExpr) *model.AliasedExpr {
	for _, aliasedColumn := range aliasedColumns {
		if model.PartlyImplementedIsEqual(column, aliasedColumn.Expr) {
			result := model.AliasedExpr{
				Expr:  aliasedColumn.Expr,
				Alias: aliasedColumn.GroupAlias,
			}
			return &result
		}
	}
	return nil
}

func (p *pancakeSqlQueryGenerator) isPartOfOrderBy(alias model.GroupByExpr, orderByColumns []model.OrderByExpr) bool {
	for _, orderBy := range orderByColumns {
		if orderByLiteral, ok := orderBy.Expr.(model.LiteralExpr); ok {
			if alias.GroupAlias == orderByLiteral.Value {
				return true
			}
		}
	}
	return false
}

func (p *pancakeSqlQueryGenerator) addPotentialParentCount(bucketAggregation *pancakeModelBucketAggregation, groupByColumns []model.GroupByExpr) []model.AliasedExpr {
	if query_util.IsAnyKindOfTerms(bucketAggregation.queryType) {
		parentCountColumn := model.NewWindowFunction("sum",
			[]model.Expr{model.NewCountFunc()},
			p.generatePartitionBy(groupByColumns, true), []model.OrderByExpr{})
		parentCountAliasedColumn := model.NewAliasedExpr(parentCountColumn, bucketAggregation.InternalNameForParentCount())
		return []model.AliasedExpr{parentCountAliasedColumn}
	}
	return []model.AliasedExpr{}
}

func (p *pancakeSqlQueryGenerator) generateBucketSqlParts(query *pancakeModel, bucketAggregation *pancakeModelBucketAggregation, groupByColumns []model.GroupByExpr, hasMoreBucketAggregations bool) (
	addSelectColumns []model.AliasedExpr, addGroupBys []model.GroupByExpr, addRankColumns []model.AliasedExpr, addRankWheres []model.Expr, addRankOrderBys []model.OrderByExpr, err error) {

	// For some group by such as terms, we need total count. We add it in this method.
	addSelectColumns = append(addSelectColumns, p.addPotentialParentCount(bucketAggregation, groupByColumns)...)

	for columnId, column := range bucketAggregation.selectedColumns {
		columnAliasString := bucketAggregation.InternalNameForKey(columnId)
		addSelectColumns = append(addSelectColumns, model.NewAliasedExpr(column, columnAliasString))
		addGroupBys = append(addGroupBys, model.NewGroupByExpr(column, columnAliasString))
	}

	// build count for aggr
	var countColumn model.Expr
	if hasMoreBucketAggregations {
		partCountColumn := model.NewCountFunc()

		countColumn = model.NewWindowFunction("sum", []model.Expr{partCountColumn},
			p.generatePartitionBy(append(groupByColumns, addGroupBys...), true), []model.OrderByExpr{})
	} else {
		countColumn = model.NewCountFunc()
	}
	countAliasedColumn := model.NewAliasedExpr(countColumn, bucketAggregation.InternalNameForCount())
	addSelectColumns = append(addSelectColumns, countAliasedColumn)

	if len(bucketAggregation.orderBy) > 0 {
		rankOrderBy := make([]model.OrderByExpr, 0)

		for i, orderBy := range bucketAggregation.orderBy {
			columnId := len(bucketAggregation.selectedColumns) + i
			direction := orderBy.Direction

			rankColumn := p.isPartOf(orderBy.Expr, append(append(groupByColumns, addGroupBys...),
				// We need count before window functions
				model.NewGroupByExpr(model.NewCountFunc(), bucketAggregation.InternalNameForCount())))
			if rankColumn != nil { // rank is part of group by
				if direction == model.DefaultOrder {
					direction = model.AscOrder // primarily needed for tests
				}
			} else {
				transformer := newPancakeOrderByTransformer(p.ctx)
				rankColumn = transformer.transformSingleOrderBy(orderBy.Expr, bucketAggregation, query)

				if rankColumn == nil {
					// we need new columns for rank
					orderByExpr := orderBy.Expr
					_, orderByAlreadyDone := orderByExpr.(model.ColumnRef)
					if !orderByAlreadyDone && hasMoreBucketAggregations {
						partColumn, aggFunctionName, err := p.generateAccumAggrFunctions(orderByExpr, nil)
						if err != nil {
							return nil, nil, nil, nil, nil, err
						}
						orderByExpr = model.NewWindowFunction(aggFunctionName, []model.Expr{partColumn},
							p.generatePartitionBy(append(groupByColumns, addGroupBys...), false), []model.OrderByExpr{})
					}
					aliasedExpr := model.NewAliasedExpr(orderByExpr, bucketAggregation.InternalNameForOrderBy(columnId))
					addSelectColumns = append(addSelectColumns, aliasedExpr)
					rankColumn = &aliasedExpr
				}
			}
			rankOrderBy = append(rankOrderBy, model.NewOrderByExpr(rankColumn.AliasRef(), direction))
		}

		// We order by count, but add key to get right dense_rank()
		for _, addedGroupByAlias := range addGroupBys {
			if !p.isPartOfOrderBy(addedGroupByAlias, rankOrderBy) {
				rankOrderBy = append(rankOrderBy, model.NewOrderByExpr(addedGroupByAlias.GroupAliasRef(), model.AscOrder))
			}
		}

		rankColumn := model.NewWindowFunction("dense_rank", []model.Expr{},
			p.generatePartitionBy(groupByColumns, false), rankOrderBy)
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
		//functionSuffix := ""
		//if len(splits) > 1 {
		//	functionSuffix = "(" + splits[1]
		//}

		if function.Name == "count" {
			//return model.NewFunction("countIf", whereClause), nil
			//ifLiteral := model.NewLiteral(" IF (" + whereClause.Accept() + ", 1, 0)")

			ifFunction := model.NewFunction("IF", whereClause, model.NewLiteral(1), model.NewLiteral(0))
			return model.NewFunction("SUM", ifFunction), nil
		} else if strings.HasSuffix(baseFunctionName, "If") && len(function.Args) > 0 {
			newArgs := make([]model.Expr, len(function.Args))
			copy(newArgs, function.Args)
			newArgs[len(newArgs)-1] = model.And([]model.Expr{newArgs[len(newArgs)-1], whereClause})
			return model.NewFunction(function.Name, newArgs...), nil
		} else if function.Name == "sum" || function.Name == "avg" {
			ifFunction := model.NewFunction("IF", whereClause, model.NewLiteral(1), model.NewLiteral(0))
			return model.NewFunction(baseFunctionName, ifFunction), nil
		} else if len(function.Args) == 1 {
			// min and max function
			return model.NewFunction(baseFunctionName, function.Args[0]), nil
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
	groupBys := make([]model.GroupByExpr, 0)

	type addIfCombinator struct {
		selectNr  int
		queryType bucket_aggregations.CombinatorAggregationInterface
	}
	addIfCombinators := make([]addIfCombinator, 0)
	var optTopHitsOrMetrics *pancakeModelMetricAggregation

	for i, layer := range aggregation.layers {
		// In doris's syntax, the group by column must appear in the select condition.
		// like: select col1 from tab group by col1, col2. the col2 must be in the select condition.
		bucketSelectedColumns := make(map[string]struct{})
		if layer.nextBucketAggregation != nil {
			if _, isTerms := layer.nextBucketAggregation.queryType.(bucket_aggregations.Terms); isTerms {
				for _, column := range layer.nextBucketAggregation.selectedColumns {
					if columnRef, ok := column.(model.ColumnRef); ok {
						bucketSelectedColumns[columnRef.ColumnName] = struct{}{}
					}
				}
			}
		}
		for _, metric := range layer.currentMetricAggregations {
			switch metric.queryType.(type) {
			case *metrics_aggregations.TopMetrics, *metrics_aggregations.TopHits:
				optTopHitsOrMetrics = metric
			default:
				hasMoreBucketAggregations := bucketAggregationSoFar < bucketAggregationCount
				addSelectColumns, err := p.generateMetricSelects(metric, groupBys, hasMoreBucketAggregations, bucketSelectedColumns)
				if err != nil {
					return nil, "", err
				}
				selectColumns = append(selectColumns, addSelectColumns...)
			}
		}

		if layer.nextBucketAggregation != nil {
			if combinator, isCombinator := layer.nextBucketAggregation.queryType.(bucket_aggregations.CombinatorAggregationInterface); isCombinator {
				var isFilter bool
				switch combinator.(type) {
				case *bucket_aggregations.FilterAgg, bucket_aggregations.Filters:
					isFilter = true
				}
				filterAlreadyInWhereClause := i == 0 && len(aggregation.layers) > 1 && len(layer.currentMetricAggregations) == 0 && len(layer.currentPipelineAggregations) == 0
				if !isFilter || !filterAlreadyInWhereClause {
					addIfCombinators = append(addIfCombinators, addIfCombinator{len(selectColumns), combinator})
				}
			}

			if layer.nextBucketAggregation.DoesHaveGroupBy() {
				bucketAggregationSoFar += 1
			}
			hasMoreBucketAggregations := bucketAggregationSoFar < bucketAggregationCount
			addSelectColumns, addGroupBys, addRankColumns, addRankWheres, addRankOrderBys, err :=
				p.generateBucketSqlParts(aggregation, layer.nextBucketAggregation, groupBys, hasMoreBucketAggregations)
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
	var combinatorWhere []model.Expr
	for i := len(addIfCombinators) - 1; i >= 0; i-- { // reverse order is important
		combinator := addIfCombinators[i]
		selectsBefore := selectColumns[:combinator.selectNr]
		selectsAfter := selectColumns[combinator.selectNr:]
		newAfterSelects := make([]model.AliasedExpr, 0, len(selectsAfter))

		for _, subGroup := range combinator.queryType.CombinatorGroups() {
			combinatorWhere = append(combinatorWhere, subGroup.WhereClause)
			for _, selectAfter := range selectsAfter {
				var withCombinator model.Expr
				if p.isPartOf(selectAfter.Expr, groupBys) != nil {
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
		rankColumns = []model.AliasedExpr{} // needed if there would be top hits

		resultQuery = &model.SelectCommand{
			Columns:     p.aliasedExprArrayToExpr(selectColumns),
			GroupBy:     p.groupByExprArrayToExpr(groupBys),
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
			GroupBy:     p.groupByExprArrayToExpr(groupBys),
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

	if optTopHitsOrMetrics != nil {
		resultQuery.Columns = append(resultQuery.Columns, p.aliasedExprArrayToLiteralExpr(rankColumns)...)
		resultQuery, err = p.generateTopHitsQuery(aggregation, combinatorWhere, optTopHitsOrMetrics, groupBys, selectColumns, resultQuery)
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
