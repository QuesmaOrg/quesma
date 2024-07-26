// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package queryparser

import (
	"fmt"
	"quesma/model"
)

func pancakeTranslateMetricToFilling(metric *pancakeAggregationLevel) (filling *pancakeFillingMetricAggregation) {
	if metric == nil {
		panic("metric aggregation is nil in pancakeTranslateMetricToFilling")
	}
	if metric.queryType.AggregationType() != model.MetricsAggregation {
		panic(fmt.Sprintf("metric aggregation is not metrics aggregation in pancakeTranslateMetricToFilling, type: %s",
			metric.queryType.AggregationType().String()))
	}

	return &pancakeFillingMetricAggregation{
		name:            metric.name,
		queryType:       metric.queryType,
		selectedColumns: metric.selectedColumns,

		metadata: metric.metadata,
	}
}

func pancakeTranslateBucketToLayered(bucket *pancakeAggregationLevel) (layer *pancakeLayerBucketAggregation) {
	if bucket == nil {
		panic("bucket aggregation is nil in pancakeTranslateBucketToLayered")
	}
	if bucket.queryType.AggregationType() != model.BucketAggregation {
		panic(fmt.Sprintf("bucket aggregation is not bucket aggregation in pancakeTranslateBucketToLayered, type: %s",
			bucket.queryType.AggregationType().String()))
	}

	return &pancakeLayerBucketAggregation{
		name:            bucket.name,
		queryType:       bucket.queryType,
		selectedColumns: bucket.selectedColumns,

		//children: bucket.children,
		orderBy: bucket.orderBy,
		limit:   bucket.limit,
		isKeyed: bucket.isKeyed,

		metadata: bucket.metadata,
	}
}

func pancakeTranslateFromAggregationToLayered(topLevel pancakeAggregationTopLevel) (pancakeResult *pancakeAggregation, err error) {
	if topLevel.children == nil || len(topLevel.children) == 0 {
		// no aggregations found
		return
	}

	var nextBucketAggregation *pancakeAggregationLevel

	layers := make([]*pancakeAggregationLayer, 0)
	firstLayer, nextBucketAggregation, err := pancakeBakeLayer(topLevel.children)
	if err != nil {
		return nil, err
	}
	layers = append(layers, firstLayer)

	for nextBucketAggregation != nil {
		var layer *pancakeAggregationLayer
		layer, nextBucketAggregation, err = pancakeBakeLayer(nextBucketAggregation.children)
		if err != nil {
			return nil, err
		}
		if layer == nil {
			break
		}

		layers = append(layers, layer)
	}

	pancakeResult = &pancakeAggregation{
		layers:      layers,
		whereClause: topLevel.whereClause,
	}

	return
}

func pancakeBakeLayer(childAggregations []*pancakeAggregationLevel) (*pancakeAggregationLayer, *pancakeAggregationLevel, error) {

	if childAggregations == nil || len(childAggregations) == 0 {
		return nil, nil, nil
	}

	var nextBucketAggregation *pancakeAggregationLevel // this is the next bucket aggregation to process

	layer := &pancakeAggregationLayer{
		currentMetricAggregations: make([]*pancakeFillingMetricAggregation, 0),
	}

	for _, childAgg := range childAggregations {
		switch childAgg.queryType.AggregationType() {
		case model.MetricsAggregation:
			layer.currentMetricAggregations = append(layer.currentMetricAggregations, pancakeTranslateMetricToFilling(childAgg))

		case model.BucketAggregation:
			if layer.nextBucketAggregation != nil {
				return nil, nil, fmt.Errorf("two bucket aggregation on same level are not supported: %s, %s",
					layer.nextBucketAggregation.name, childAgg.name)
			}

			layer.nextBucketAggregation = pancakeTranslateBucketToLayered(childAgg)
			nextBucketAggregation = childAgg
		default:
			return nil, nil, fmt.Errorf("unsupported aggregation type in pancake, name: %s, type: %s",
				childAgg.name, childAgg.queryType.AggregationType().String())
		}
	}
	return layer, nextBucketAggregation, nil
}
