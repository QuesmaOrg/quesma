// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package queryparser

import (
	"context"
	"fmt"
	"github.com/k0kubun/pp"
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

// TODO: maybe the same logic needs to be applied to pipeline aggregations, needs checking.
func (t *pancakeOrderByTransformer) transformLayer(query *pancakeModel, layer *pancakeModelLayer) {
	if layer.nextBucketAggregation == nil {
		return
	}
	bucketAggr := layer.nextBucketAggregation
	for i, orderBy := range bucketAggr.orderBy {
		fmt.Println(orderBy.Expr)
		if pathToMetric, ok := orderBy.Expr.(model.LiteralExpr); ok {
			// fix. new base expr?
			pathToMetricStr := strings.Split(pathToMetric.Value.(string), ".")[0]
			for _, metric := range query.allMetricAggregations() {
				bucketAggrName := bucketAggr.InternalNameWithoutPrefix()
				metricAggrName := metric.InternalNameWithoutPrefix()
				fmt.Println("pathToMetricStr", pathToMetricStr, bucketAggrName, " X ", metricAggrName, metric.queryType.String())
				columnId := 0
				if multipleColumnsMetric, ok := metric.queryType.(metrics_aggregations.MultipleMetricColumnsInterface); ok {
					columnId = multipleColumnsMetric.ColumnId(strings.Split(pathToMetric.Value.(string), ".")[1])
				}
				if bucketAggrName+strings.ReplaceAll(pathToMetricStr, ">", "__") == metricAggrName {
					pp.Println("HOHOHO", metric)
					bucketAggr.orderBy[i].Expr = model.NewColumnRef(metric.InternalNameForCol(columnId))
				}
			}
		}
	}
	pp.Println("LAYER")
	pp.Println(layer)
}

func (t *pancakeOrderByTransformer) transform(query *pancakeModel) {
	pp.Println("TRANSFORMER")
	pp.Println(query)

	for _, layer := range query.layers {
		t.transformLayer(query, layer)
	}
}
