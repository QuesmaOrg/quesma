// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package queryparser

import (
	"context"
	"errors"
	"fmt"
	"quesma/logger"
	"quesma/model"
	"quesma/model/metrics_aggregations"
	"quesma/util"
	"strconv"
)

func generateMetricSelectedColumns(ctx context.Context, metricsAggr metricsAggregation) (result []model.Expr, err error) {
	getFirstExpression := func() model.Expr {
		if len(metricsAggr.Fields) > 0 {
			return metricsAggr.Fields[0]
		}
		logger.ErrorWithCtx(ctx).Msg("No field names in metrics aggregation. Using empty.")
		return nil
	}

	switch metricsAggr.AggrType {
	case "sum", "min", "max", "avg":
		result = []model.Expr{model.NewFunction(metricsAggr.AggrType+"OrNull", getFirstExpression())}
	case "quantile":
		// Sorting here useful mostly for determinism in tests.
		// It wasn't there before, and everything worked fine. We could safely remove it, if needed.
		usersPercents := util.MapKeysSortedByValue(metricsAggr.Percentiles)
		result = make([]model.Expr, 0, len(usersPercents))
		for _, usersPercent := range usersPercents {
			percentAsFloat := metricsAggr.Percentiles[usersPercent]
			result = append(result, model.NewAliasedExpr(
				model.MultiFunctionExpr{
					Name: "quantiles",
					Args: []model.Expr{model.NewLiteral(percentAsFloat), getFirstExpression()}},
				fmt.Sprintf("quantile_%s", usersPercent),
			))

		}
	case "cardinality":
		result = []model.Expr{model.NewCountFunc(model.NewDistinctExpr(getFirstExpression()))}

	case "value_count":
		result = []model.Expr{model.NewCountFunc()}

	case "stats":
		expr := getFirstExpression()
		result = make([]model.Expr, 0, 4)

		result = append(result, model.NewCountFunc(expr),
			model.NewFunction("minOrNull", expr),
			model.NewFunction("maxOrNull", expr),
			model.NewFunction("avgOrNull", expr),
			model.NewFunction("sumOrNull", expr))

	case "top_hits":
		// see other buildMetricsAggregation(), we don't implement it now
		return nil, errors.New("top_hits is not implemented yet in version una")
	case "top_metrics":
		// see other buildMetricsAggregation(), we don't implement it now
		return nil, errors.New("top_hits is not implemented yet in version una")
	case "percentile_ranks":
		result = make([]model.Expr, 0, len(metricsAggr.Fields[1:]))
		for _, cutValueAsString := range metricsAggr.Fields[1:] {
			unquoted := model.AsString(cutValueAsString)
			cutValue, _ := strconv.ParseFloat(unquoted, 64)

			// full exp we create below looks like this:
			// fmt.Sprintf("count(if(%s<=%f, 1, NULL))/count(*)*100", strconv.Quote(getFirstFieldName()), cutValue)

			ifExp := model.NewFunction(
				"if",
				model.NewInfixExpr(getFirstExpression(), "<=", model.NewLiteral(cutValue)),
				model.NewLiteral(1),
				model.NewStringExpr("NULL"),
			)
			firstCountExp := model.NewFunction("count", ifExp)
			twoCountsExp := model.NewInfixExpr(firstCountExp, "/", model.NewCountFunc(model.NewWildcardExpr))
			fullExp := model.NewInfixExpr(twoCountsExp, "*", model.NewLiteral(100))

			result = append(result, fullExp)
		}
	case "extended_stats":

		expr := getFirstExpression()
		result = make([]model.Expr, 0, 10)

		// add column with fn applied to field
		addColumn := func(funcName string) {
			result = append(result, model.NewFunction(funcName, expr))
		}

		addColumn("count")
		addColumn("minOrNull")
		addColumn("maxOrNull")
		addColumn("avgOrNull")
		addColumn("sumOrNull")

		result = append(result, model.NewFunction("sumOrNull", model.NewInfixExpr(expr, "*", expr)))

		addColumn("varPop")
		addColumn("varSamp")
		addColumn("stddevPop")
		addColumn("stddevSamp")
	case "geo_centroid":
		firstExpr := getFirstExpression()
		result = make([]model.Expr, 0, 3)
		if col, ok := firstExpr.(model.ColumnRef); ok {
			colName := col.ColumnName
			// TODO we have create columns according to the schema
			latColumn := model.NewColumnRef(colName + "::lat")
			lonColumn := model.NewColumnRef(colName + "::lon")
			castLat := model.NewFunction("CAST", latColumn, model.NewLiteral(fmt.Sprintf("'%s'", "Float")))
			castLon := model.NewFunction("CAST", lonColumn, model.NewLiteral(fmt.Sprintf("'%s'", "Float")))
			result = append(result, model.NewFunction("avgOrNull", castLat))
			result = append(result, model.NewFunction("avgOrNull", castLon))
			result = append(result, model.NewFunction("count"))
		}
	default:
		logger.WarnWithCtx(ctx).Msgf("unknown metrics aggregation: %s", metricsAggr.AggrType)
		return nil, fmt.Errorf("unknown metrics aggregation %s", metricsAggr.AggrType)
	}
	return
}

func generateMetricsType(ctx context.Context, metricsAggr metricsAggregation) model.QueryType {
	switch metricsAggr.AggrType {
	case "sum":
		return metrics_aggregations.NewSum(ctx, metricsAggr.FieldType)
	case "min":
		return metrics_aggregations.NewMin(ctx, metricsAggr.FieldType)
	case "max":
		return metrics_aggregations.NewMax(ctx, metricsAggr.FieldType)
	case "avg":
		return metrics_aggregations.NewAvg(ctx, metricsAggr.FieldType)
	case "stats":
		return metrics_aggregations.NewStats(ctx)
	case "extended_stats":
		return metrics_aggregations.NewExtendedStats(ctx, metricsAggr.sigma)
	case "cardinality":
		return metrics_aggregations.NewCardinality(ctx)
	case "quantile":
		return metrics_aggregations.NewQuantile(ctx, util.MapKeysSortedByValue(metricsAggr.Percentiles), metricsAggr.Keyed, metricsAggr.FieldType)
	case "top_hits":
		return metrics_aggregations.NewTopHits(ctx)
	case "top_metrics":
		return metrics_aggregations.NewTopMetrics(ctx, metricsAggr.sortByExists())
	case "value_count":
		return metrics_aggregations.NewValueCount(ctx)
	case "percentile_ranks":
		return metrics_aggregations.NewPercentileRanks(ctx, metricsAggr.PercentilesArr, metricsAggr.Keyed)
	case "geo_centroid":
		return metrics_aggregations.NewGeoCentroid(ctx)
	}
	return nil
}
