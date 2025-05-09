// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package elastic_query_dsl

import (
	"context"
	"github.com/QuesmaOrg/quesma/platform/logger"
	"github.com/QuesmaOrg/quesma/platform/model"
	"github.com/QuesmaOrg/quesma/platform/model/metrics_aggregations"
	"strings"
)

type pancakeOrderByTransformer struct {
	ctx context.Context
}

func newPancakeOrderByTransformer(ctx context.Context) *pancakeOrderByTransformer {
	return &pancakeOrderByTransformer{ctx: ctx}
}

// transformSingleOrderBy transforms a single order by expression, of query `query` and bucket aggregation `bucketAggrInternalName`.
// What it does, it finds metric aggregation that corresponds to the order by expression, and returns a new aliased expression
//
// TODO: maybe the same logic needs to be applied to pipeline aggregations, needs checking.
func (t *pancakeOrderByTransformer) transformSingleOrderBy(orderBy model.Expr, bucketAggregation *pancakeModelBucketAggregation, query *pancakeModel) *model.AliasedExpr {
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

	foundLayerIdx := -1
	for layerIdx, layer := range query.layers {
		if layer.nextBucketAggregation == bucketAggregation {
			foundLayerIdx = layerIdx
			break
		}
	}
	if foundLayerIdx == -1 {
		logger.ErrorWithCtx(t.ctx).Msgf("bucket aggregation not found in query")
		return nil
	}
	foundLayerIdx += 1
	fullPath := strings.Split(fullPathWithoutSubmetric, ">")
	path := fullPath

	for len(path) > 1 {
		if foundLayerIdx >= len(query.layers) {
			logger.ErrorWithCtx(t.ctx).Msgf("out of layers in path: %s", fullPathToOrderByExpr)
			return nil
		}
		if query.layers[foundLayerIdx].nextBucketAggregation == nil {
			logger.ErrorWithCtx(t.ctx).Msgf("no bucket aggregation in path: %s", fullPathToOrderByExpr)
			return nil
		}
		if query.layers[foundLayerIdx].nextBucketAggregation.name != path[0] {
			logger.ErrorWithCtx(t.ctx).Msgf("bucket aggregation mismatch in path: %s, expected: %s, was: %s",
				fullPathToOrderByExpr, path[0], query.layers[foundLayerIdx].nextBucketAggregation.name)
			return nil
		}
		foundLayerIdx += 1
		path = path[1:]
	}

	if foundLayerIdx >= len(query.layers) {
		logger.ErrorWithCtx(t.ctx).Msgf("out of layers in path: %s", fullPathToOrderByExpr)
		return nil
	}

	for _, metric := range query.layers[foundLayerIdx].currentMetricAggregations {
		columnIdx := 0 // when no multiple columns, it must be 0
		if multipleColumnsMetric, ok := metric.queryType.(metrics_aggregations.MultipleMetricColumnsInterface); ok {
			columnIdx = multipleColumnsMetric.ColumnIdx(submetricName)
		}

		if metric.name == path[0] {
			result := model.NewAliasedExpr(orderBy, metric.InternalNameForCol(columnIdx))
			return &result
		}
	}

	logger.ErrorWithCtx(t.ctx).Msgf("no metric found for path: %s", fullPathToOrderByExpr)
	return nil
}
