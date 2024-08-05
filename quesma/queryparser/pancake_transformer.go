// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package queryparser

import (
	"fmt"
	"quesma/model"
	"sort"
	"strings"
)

// 2. Translate aggregation tree into pancake model.
type pancakeTransformer struct {
}

func (a *pancakeTransformer) metricAggregationToFilling(previousAggrNames []string, metric *pancakeAggregationLevel) (filling *pancakeFillingMetricAggregation, err error) {
	if metric == nil {
		return nil, fmt.Errorf("metric aggregation is nil")

	}
	if metric.queryType.AggregationType() != model.MetricsAggregation {
		return nil, fmt.Errorf("metric %s aggregation is not metrics aggregation, type: %s", metric.name, metric.queryType.AggregationType().String())
	}

	return &pancakeFillingMetricAggregation{
		name: metric.name,
		// TODO: check for collisions
		internalName:    fmt.Sprintf("metric__%s", strings.Join(append(previousAggrNames, metric.name), "__")),
		queryType:       metric.queryType,
		selectedColumns: metric.selectedColumns,

		metadata: metric.metadata,
	}, nil
}

func (a *pancakeTransformer) bucketAggregationToLayer(previousAggrNames []string, bucket *pancakeAggregationLevel) (layer *pancakeLayerBucketAggregation, err error) {
	if bucket == nil {
		return nil, fmt.Errorf("bucket aggregation is nil")

	}
	if bucket.queryType.AggregationType() != model.BucketAggregation {
		return nil, fmt.Errorf("bucket aggregation %s is not bucket aggregation, type: %s", bucket.name, bucket.queryType.AggregationType().String())
	}

	return &pancakeLayerBucketAggregation{
		name: bucket.name,
		// TODO: check for collisions
		internalName:    fmt.Sprintf("aggr__%s__", strings.Join(append(previousAggrNames, bucket.name), "__")),
		queryType:       bucket.queryType,
		selectedColumns: bucket.selectedColumns,

		orderBy:                 bucket.orderBy,
		limit:                   bucket.limit,
		isKeyed:                 bucket.isKeyed,
		filterOurEmptyKeyBucket: bucket.filterOutEmptyKeyBucket,

		metadata: bucket.metadata,
	}, nil
}

func (a *pancakeTransformer) createLayer(previousAggrNames []string, childAggregations []*pancakeAggregationLevel) (layer *pancakeAggregationLayer, nextBucketAggregation *pancakeAggregationLevel, err error) {

	if len(childAggregations) == 0 {
		return nil, nil, nil
	}

	layer = &pancakeAggregationLayer{
		currentMetricAggregations: make([]*pancakeFillingMetricAggregation, 0),
	}

	for _, childAgg := range childAggregations {
		if childAgg.queryType == nil {
			return nil, nil, fmt.Errorf("query type is nil in createLayer")
		}
		switch childAgg.queryType.AggregationType() {
		case model.MetricsAggregation:
			metrics, err := a.metricAggregationToFilling(previousAggrNames, childAgg)
			if err != nil {
				return nil, nil, err
			}
			layer.currentMetricAggregations = append(layer.currentMetricAggregations, metrics)

		case model.BucketAggregation:
			if layer.nextBucketAggregation != nil {
				return nil, nil, fmt.Errorf("two bucket aggregation on same level are not supported: %s, %s",
					layer.nextBucketAggregation.name, childAgg.name)
			}

			bucket, err := a.bucketAggregationToLayer(previousAggrNames, childAgg)
			if err != nil {
				return nil, nil, err
			}

			layer.nextBucketAggregation = bucket
			nextBucketAggregation = childAgg
		default:
			return nil, nil, fmt.Errorf("unsupported aggregation type in pancake, name: %s, type: %s",
				childAgg.name, childAgg.queryType.AggregationType().String())
		}
	}
	return layer, nextBucketAggregation, nil
}

func (a *pancakeTransformer) aggregationTreeToPancake(topLevel pancakeAggregationTopLevel) (pancakeResult *pancakeAggregation, err error) {
	if topLevel.children == nil || len(topLevel.children) == 0 {
		return nil, fmt.Errorf("no top level aggregations found")
	}

	var nextBucketAggregation *pancakeAggregationLevel

	layers := make([]*pancakeAggregationLayer, 0)
	aggrNames := make([]string, 0)
	firstLayer, nextBucketAggregation, err := a.createLayer(aggrNames, topLevel.children)
	if err != nil {
		return nil, err
	}

	layers = append(layers, firstLayer)

	for nextBucketAggregation != nil {
		var layer *pancakeAggregationLayer
		aggrNames = append(aggrNames, nextBucketAggregation.name)
		layer, nextBucketAggregation, err = a.createLayer(aggrNames, nextBucketAggregation.children)
		if err != nil {
			return nil, err
		}
		if layer == nil {
			break
		}

		layers = append(layers, layer)
	}

	// we need sort metric aggregation to generate consistent results, otherwise:
	// - tests are flaky
	// - database might not use it query cache
	for _, layer := range layers {
		sort.Slice(layer.currentMetricAggregations, func(i, j int) bool {
			return layer.currentMetricAggregations[i].name < layer.currentMetricAggregations[j].name
		})
	}

	pancakeResult = &pancakeAggregation{
		layers:      layers,
		whereClause: topLevel.whereClause,
	}

	return
}
