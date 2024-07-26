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

		children: bucket.children,
		orderBy:  bucket.orderBy,
		limit:    bucket.limit,
		isKeyed:  bucket.isKeyed,

		metadata: bucket.metadata,
	}
}

func pancakeTranslateFromAggregationToLayered(aggregation pancakeAggregationTopLevel) (pancakeResult *pancakeAggregation, err error) {
	if aggregation.children == nil || len(aggregation.children) == 0 {
		// no aggregations found
		return
	}
	pancakeResult = &pancakeAggregation{
		bucketAggregations: make([]*pancakeLayerBucketAggregation, 0),
		metricAggregations: make([][]*pancakeFillingMetricAggregation, 1),
		whereClause:        aggregation.whereClause,
	}
	level := 0
	err = pancakeBakeLayer(aggregation.children, pancakeResult, level)
	if err != nil {
		return pancakeResult, err
	}
	for ; len(pancakeResult.bucketAggregations) > level; level += 1 {
		err = pancakeBakeLayer(pancakeResult.bucketAggregations[level].children, pancakeResult, level+1)
		if err != nil {
			return pancakeResult, err
		}
	}

	return
}

func pancakeBakeLayer(childAggregations []*pancakeAggregationLevel, pancakeResult *pancakeAggregation, level int) error {
	for _, childAgg := range childAggregations {
		switch childAgg.queryType.AggregationType() {
		case model.MetricsAggregation:
			pancakeResult.metricAggregations[level] = append(pancakeResult.metricAggregations[level],
				pancakeTranslateMetricToFilling(childAgg))
		case model.BucketAggregation:
			if len(pancakeResult.bucketAggregations) != level {
				return fmt.Errorf("two bucket aggregation on same level are not supported: %s, %s",
					pancakeResult.bucketAggregations[level-1].name, childAgg.name)
			}
			pancakeResult.bucketAggregations = append(pancakeResult.bucketAggregations,
				pancakeTranslateBucketToLayered(childAgg))
		default:
			return fmt.Errorf("unsupported aggregation type in pancake, name: %s, type: %s",
				childAgg.name, childAgg.queryType.AggregationType().String())
		}
	}
	return nil
}
