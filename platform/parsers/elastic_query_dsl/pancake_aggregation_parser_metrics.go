// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package elastic_query_dsl

import (
	"context"
	"fmt"
	"github.com/QuesmaOrg/quesma/platform/logger"
	"github.com/QuesmaOrg/quesma/platform/model"
	"github.com/QuesmaOrg/quesma/platform/model/metrics_aggregations"
	"github.com/QuesmaOrg/quesma/platform/util"
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
		result = []model.Expr{model.NewFunction(metricsAggr.AggrType, getFirstExpression())}
	case "quantile":
		firstField := metricsAggr.Fields[0]
		var columnName string
		if colRef, ok := firstField.(model.ColumnRef); ok {
			columnName = colRef.ColumnName
		}

		// Sorting here useful mostly for determinism in tests.
		// It wasn't there before, and everything worked fine. We could safely remove it, if needed.
		usersPercents := util.MapKeysSortedByValue(metricsAggr.Percentiles)
		result = make([]model.Expr, 0, len(usersPercents))
		for _, usersPercent := range usersPercents {
			percentAsFloat := metricsAggr.Percentiles[usersPercent]
			// https://doris.apache.org/docs/sql-manual/sql-functions/aggregate-functions/percentile-approx
			result = append(result, model.NewFunction("PERCENTILE_APPROX", model.NewColumnRef(columnName), model.NewLiteral(percentAsFloat)))
			//result = append(result, model.FunctionExpr{
			//	// Rare function that has two brackets: PERCENTILE_APPROX(x, 0.5)
			//	//https://doris.apache.org/docs/sql-manual/sql-functions/aggregate-functions/percentile-approx
			//	Name: fmt.Sprintf("PERCENTILE_APPROX(%f)", percentAsFloat),
			//	Args: []model.Expr{getFirstExpression()}},
			//)
		}
	case "cardinality":
		// In ElasticSearch it is approximate algorithm
		result = []model.Expr{model.NewFunction("NDV", getFirstExpression())}

	case "value_count":
		result = []model.Expr{model.NewCountFunc(getFirstExpression())}

	case "stats":
		expr := getFirstExpression()
		result = make([]model.Expr, 0, 4)

		result = append(result, model.NewCountFunc(expr),
			model.NewFunction("min", expr),
			model.NewFunction("max", expr),
			model.NewFunction("avg", expr),
			model.NewFunction("sum", expr))

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
				result = []model.Expr{model.NewFunction("sum", getFirstExpression())}
			case metrics_aggregations.RateModeValueCount:
				result = []model.Expr{model.NewCountFunc(getFirstExpression())}
			default:
				// should never happen because of parsing checks
				return nil, fmt.Errorf("unknown rate mode: %s", metricsAggr.mode)
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
		addColumn("min")
		addColumn("max")
		addColumn("avg")
		addColumn("sum")

		result = append(result, model.NewFunction("sum", model.NewInfixExpr(expr, "*", expr)))

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
			result = append(result, model.NewFunction("avg", latColumn))
			result = append(result, model.NewFunction("avg", lonColumn))
			result = append(result, model.NewCountFunc())
		}
	case "geo_bounds":
		firstExpr := getFirstExpression()
		result = make([]model.Expr, 0, 3)
		if col, ok := firstExpr.(model.ColumnRef); ok {
			// TODO this is internalPropertyName and should be taken from schema
			// instead of using util.FieldToColumnEncoder and doing encoding in-place
			colName := util.FieldToColumnEncoder(col.ColumnName)
			// TODO we have create columns according to the schema
			latColumn := model.NewGeoLat(colName)
			lonColumn := model.NewGeoLon(colName)
			result = append(result, model.NewFunction("min", lonColumn))
			result = append(result, model.NewFunction("argMinOrNull", latColumn, lonColumn))
			result = append(result, model.NewFunction("min", latColumn))
			result = append(result, model.NewFunction("argMinOrNull", lonColumn, latColumn))
		}
	default:
		logger.WarnWithCtx(ctx).Msgf("unknown metrics aggregation: %s", metricsAggr.AggrType)
		return nil, fmt.Errorf("unknown metrics aggregation %s", metricsAggr.AggrType)
	}
	return
}

func (cw *ClickhouseQueryTranslator) generateMetricsType(metricsAggr metricsAggregation) model.QueryType {
	switch metricsAggr.AggrType {
	case "sum":
		return metrics_aggregations.NewSum(cw.Ctx, metricsAggr.FieldType)
	case "min":
		return metrics_aggregations.NewMin(cw.Ctx, metricsAggr.FieldType)
	case "max":
		return metrics_aggregations.NewMax(cw.Ctx, metricsAggr.FieldType)
	case "avg":
		return metrics_aggregations.NewAvg(cw.Ctx, metricsAggr.FieldType)
	case "stats":
		return metrics_aggregations.NewStats(cw.Ctx)
	case "extended_stats":
		return metrics_aggregations.NewExtendedStats(cw.Ctx, metricsAggr.sigma)
	case "cardinality":
		return metrics_aggregations.NewCardinality(cw.Ctx)
	case "quantile":
		return metrics_aggregations.NewQuantile(cw.Ctx, util.MapKeysSortedByValue(metricsAggr.Percentiles), metricsAggr.Keyed, metricsAggr.FieldType)
	case "top_hits":
		return metrics_aggregations.NewTopHits(cw.Ctx, metricsAggr.Size, metricsAggr.OrderBy, cw.Table.FullTableNameUnquoted())
	case "top_metrics":
		return metrics_aggregations.NewTopMetrics(cw.Ctx, metricsAggr.Size, metricsAggr.SortBy, metricsAggr.Order)
	case "value_count":
		return metrics_aggregations.NewValueCount(cw.Ctx)
	case "percentile_ranks":
		return metrics_aggregations.NewPercentileRanks(cw.Ctx, metricsAggr.CutValues, metricsAggr.Keyed)
	case "geo_centroid":
		return metrics_aggregations.NewGeoCentroid(cw.Ctx)
	case "geo_bounds":
		return metrics_aggregations.NewGeoBounds(cw.Ctx)
	case "rate":
		isFieldPresent := len(metricsAggr.Fields) > 0
		if rate, err := metrics_aggregations.NewRate(cw.Ctx, metricsAggr.unit, isFieldPresent); err == nil {
			return rate
		} else {
			logger.ErrorWithCtx(cw.Ctx).Msgf("error creating rate aggregation: %s", err)
			return nil
		}
	}

	return nil
}
