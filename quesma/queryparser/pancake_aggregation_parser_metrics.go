// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package queryparser

import (
	"context"
	"fmt"
	"github.com/QuesmaOrg/quesma/quesma/logger"
	"github.com/QuesmaOrg/quesma/quesma/model"
	"github.com/QuesmaOrg/quesma/quesma/model/metrics_aggregations"
	"github.com/QuesmaOrg/quesma/quesma/util"
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
			result = append(result, model.FunctionExpr{
				// Rare function that has two brackets: quantiles(0.5)(x)
				// https://clickhouse.com/docs/en/sql-reference/aggregate-functions/reference/quantiles
				Name: fmt.Sprintf("quantiles(%f)", percentAsFloat),
				Args: []model.Expr{getFirstExpression()}},
			)
		}
	case "cardinality":
		// In ElasticSearch it is approximate algorithm
		result = []model.Expr{model.NewFunction("uniq", getFirstExpression())}

	case "value_count":
		result = []model.Expr{model.NewCountFunc(getFirstExpression())}

	case "stats":
		expr := getFirstExpression()
		result = make([]model.Expr, 0, 4)

		result = append(result, model.NewCountFunc(expr),
			model.NewFunction("minOrNull", expr),
			model.NewFunction("maxOrNull", expr),
			model.NewFunction("avgOrNull", expr),
			model.NewFunction("sumOrNull", expr))

	case "top_hits":
		innerFieldsAsSelect := make([]model.Expr, len(metricsAggr.Fields))
		copy(innerFieldsAsSelect, metricsAggr.Fields)
		return innerFieldsAsSelect, nil
	case "top_metrics":
		innerFieldsAsSelect := make([]model.Expr, len(metricsAggr.Fields))
		copy(innerFieldsAsSelect, metricsAggr.Fields)
		if len(metricsAggr.SortBy) > 0 {
			innerFieldsAsSelect = append(innerFieldsAsSelect, model.NewColumnRef(metricsAggr.SortBy))
		}
		return innerFieldsAsSelect, nil
	case "rate":
		if len(metricsAggr.Fields) > 0 {
			switch metrics_aggregations.NewRateMode(ctx, metricsAggr.mode) {
			case metrics_aggregations.RateModeSum:
				result = []model.Expr{model.NewFunction("sumOrNull", getFirstExpression())}
			case metrics_aggregations.RateModeValueCount:
				result = []model.Expr{model.NewCountFunc(getFirstExpression())}
			default:
				// should never happen because of parsing checks
				logger.ErrorWithCtx(ctx).Msgf("unknown rate mode: %s", metricsAggr.mode)
			}
		}
	case "percentile_ranks":
		result = make([]model.Expr, 0, len(metricsAggr.CutValues))
		for _, cutValueAsString := range metricsAggr.CutValues {
			cutValue, _ := strconv.ParseFloat(cutValueAsString, 64)

			// full exp we create below looks like this: countIf(field <= cutValue)/count(*) * 100
			countIfExp := model.NewFunction(
				"countIf", model.NewInfixExpr(getFirstExpression(), "<=", model.NewLiteral(cutValue)))
			bothCountsExp := model.NewInfixExpr(countIfExp, "/", model.NewCountFunc(model.NewWildcardExpr))
			fullExp := model.NewInfixExpr(bothCountsExp, "*", model.NewLiteral(100))

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
			// TODO this is internalPropertyName and should be taken from schema
			// instead of using util.FieldToColumnEncoder and doing encoding in-place
			colName := util.FieldToColumnEncoder(col.ColumnName)
			// TODO we have create columns according to the schema
			latColumn := model.NewGeoLat(colName)
			lonColumn := model.NewGeoLon(colName)
			castLat := model.NewFunction("CAST", latColumn, model.NewLiteral(fmt.Sprintf("'%s'", "Float")))
			castLon := model.NewFunction("CAST", lonColumn, model.NewLiteral(fmt.Sprintf("'%s'", "Float")))
			result = append(result, model.NewFunction("avgOrNull", castLat))
			result = append(result, model.NewFunction("avgOrNull", castLon))
			result = append(result, model.NewCountFunc())
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
		return metrics_aggregations.NewTopHits(ctx, metricsAggr.Size, metricsAggr.OrderBy)
	case "top_metrics":
		return metrics_aggregations.NewTopMetrics(ctx, metricsAggr.Size, metricsAggr.SortBy, metricsAggr.Order)
	case "value_count":
		return metrics_aggregations.NewValueCount(ctx)
	case "percentile_ranks":
		return metrics_aggregations.NewPercentileRanks(ctx, metricsAggr.CutValues, metricsAggr.Keyed)
	case "geo_centroid":
		return metrics_aggregations.NewGeoCentroid(ctx)
	case "rate":
		fieldPresent := len(metricsAggr.Fields) > 0
		return metrics_aggregations.NewRate(ctx, metricsAggr.unit, fieldPresent)
	}
	return nil
}
