// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package queryparser

import (
	"context"
	"quesma/logger"
	"quesma/model"
	"quesma/model/metrics_aggregations"
	"strings"
)

type pancakeOrderByTransformer struct {
	ctx context.Context
}

func newPancakeOrderByTransformer(ctx context.Context) *pancakeOrderByTransformer {
	return &pancakeOrderByTransformer{ctx: ctx}
}

// transformSingleOrderBy transforms a single order by expression, of query `query` and bucket aggregation `bucketAggrInternalName`.
// What it does, is only replace order expressions that are paths to metric aggregations, with their names
// (we know that they are paths to metric aggregations, when `orderBy` is of type LiteralExpr, with string value)
//
// It's necessary if orderBy looks like this "[...].submetric", e.g. "2.sum".
// It'll then transform it to a proper column of the SELECT.
// Otherwise it's necessary, but still SQL looks a bit better, e.g.
//
//	sumOrNull(sumOrNull("total")) OVER (PARTITION BY "aggr__2__key_0",
//	"aggr__2__7__key_0") AS "aggr__2__7__order_1",
//	sumOrNull(sumOrNull("total")) OVER (PARTITION BY "aggr__2__key_0",
//	"aggr__2__7__key_0") AS "metric__2__7__1_col_0"
//
// Will become simpler:
//
//	"metric__2__7__1_col_0" AS "aggr__2__7__order_1",
//	sumOrNull(sumOrNull("total")) OVER (PARTITION BY "aggr__2__key_0",
//	"aggr__2__7__key_0") AS "metric__2__7__1_col_0",
//
// TODO: maybe the same logic needs to be applied to pipeline aggregations, needs checking.
func (t *pancakeOrderByTransformer) transformSingleOrderBy(orderBy model.Expr, bucketAggrInternalName string, query *pancakeModel) *model.AliasedExpr {
	fullPathToOrderByExprRaw, isPath := orderBy.(model.LiteralExpr)
	if !isPath {
		return nil
	}

	fullPathToOrderByExpr, ok := fullPathToOrderByExprRaw.Value.(string)
	if !ok {
		logger.ErrorWithCtx(t.ctx).Msgf("path to metric is not a string, but %T (val: %v)",
			fullPathToOrderByExprRaw.Value, fullPathToOrderByExprRaw.Value)
		return nil
	}

	// fullPathToOrderByExpr is in the form of "[aggr1][>aggr2...]>metric_aggr[.submetric]" ([] means optional)
	// submetric: e.g. "percentiles.50", or "stats.sum", "extended_stats.std_deviation"
	// Most metric aggregations don't have submetrics
	var fullPathWithoutSubmetric, submetricName string
	splitByDot := strings.Split(fullPathToOrderByExpr, ".")
	switch len(splitByDot) {
	case 1:
		fullPathWithoutSubmetric = splitByDot[0]
	case 2:
		fullPathWithoutSubmetric, submetricName = splitByDot[0], splitByDot[1]
	default:
		logger.ErrorWithCtx(t.ctx).Msgf("path to metric is not valid: %s", fullPathToOrderByExpr)
		return nil
	}

	for _, metric := range query.allMetricAggregations() {
		metricAggrInternalName := metric.InternalNameWithoutPrefix()
		columnIdx := 0 // when no multiple columns, it must be 0
		if multipleColumnsMetric, ok := metric.queryType.(metrics_aggregations.MultipleMetricColumnsInterface); ok {
			columnIdx = multipleColumnsMetric.ColumnIdx(submetricName)
		}

		if bucketAggrInternalName+strings.ReplaceAll(fullPathWithoutSubmetric, ">", "__") == metricAggrInternalName {
			result := model.NewAliasedExpr(orderBy, metric.InternalNameForCol(columnIdx))
			return &result
		}
	}

	logger.ErrorWithCtx(t.ctx).Msgf("no metric found for path: %s", fullPathToOrderByExpr)
	return nil
}

// transform transforms all order by expressions of query `query`.
// What it does, is only replace order expressions that are paths to metric aggregations, with their names.
//
// It's necessary if orderBy looks like this "[...].submetric", e.g. "2.sum".
// It'll then transform it to a proper column of the SELECT.
// Otherwise it's necessary, but still SQL looks a bit better, e.g.
//
//	sumOrNull(sumOrNull("total")) OVER (PARTITION BY "aggr__2__key_0",
//	"aggr__2__7__key_0") AS "aggr__2__7__order_1",
//	sumOrNull(sumOrNull("total")) OVER (PARTITION BY "aggr__2__key_0",
//	"aggr__2__7__key_0") AS "metric__2__7__1_col_0"
//
// Will become simpler:
//
//	"metric__2__7__1_col_0" AS "aggr__2__7__order_1",
//	sumOrNull(sumOrNull("total")) OVER (PARTITION BY "aggr__2__key_0",
//	"aggr__2__7__key_0") AS "metric__2__7__1_col_0",
//
// TODO: maybe the same logic needs to be applied to pipeline aggregations, needs checking.
func (t *pancakeOrderByTransformer) transform(query *pancakeModel) *pancakeModel {
	for _, layer := range query.layers {
		bucketAggr := layer.nextBucketAggregation
		if bucketAggr == nil {
			continue
		}

		for i, orderBy := range bucketAggr.orderBy {
			bucketAggr.orderBy[i].Expr = *t.transformSingleOrderBy(orderBy.Expr, bucketAggr.InternalNameWithoutPrefix(), query)
		}
	}
	return query
}
