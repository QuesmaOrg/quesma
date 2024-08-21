// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package queryparser

import (
	"fmt"
	"quesma/model"
	"quesma/model/bucket_aggregations"
	"reflect"
	"sort"
	"strings"
)

// 2. Translate aggregation tree into pancake model.
type pancakeTransformer struct {
	usedNames map[string][]string
}

func newPancakeTransformer() pancakeTransformer {
	return pancakeTransformer{
		usedNames: make(map[string][]string),
	}
}

// Extremely rarely names may collide (e.g. aggregation named '1__2' and '1', '2').
// This adds number suffix to make sure they are always unique
func (a *pancakeTransformer) generateUniqueInternalName(origName string, aggrNames []string) string {
	proposedName := origName
	for counter := 2; true; counter += 1 {
		if prevAggr, isUsed := a.usedNames[proposedName]; !isUsed {
			a.usedNames[proposedName] = aggrNames
			return proposedName
		} else if reflect.DeepEqual(prevAggr, aggrNames) {
			return proposedName
		}
		proposedName = fmt.Sprintf("%s%d", origName, counter)
	}
	return origName
}

func (a *pancakeTransformer) generateMetricInternalName(aggrNames []string) string {
	origName := fmt.Sprintf("metric__%s", strings.Join(aggrNames, "__"))
	return a.generateUniqueInternalName(origName, aggrNames)
}

func (a *pancakeTransformer) generateBucketInternalName(aggrNames []string) string {
	origName := fmt.Sprintf("aggr__%s__", strings.Join(aggrNames, "__"))
	return a.generateUniqueInternalName(origName, aggrNames)
}

func (a *pancakeTransformer) metricAggregationTreeNodeToModel(previousAggrNames []string, metric *pancakeAggregationTreeNode) (metricModel *pancakeModelMetricAggregation, err error) {
	if metric == nil {
		return nil, fmt.Errorf("metric aggregation is nil")

	}
	if metric.queryType.AggregationType() != model.MetricsAggregation {
		return nil, fmt.Errorf("metric %s aggregation is not metrics aggregation, type: %s", metric.name, metric.queryType.AggregationType().String())
	}

	return &pancakeModelMetricAggregation{
		name:            metric.name,
		internalName:    a.generateMetricInternalName(append(previousAggrNames, metric.name)),
		queryType:       metric.queryType,
		selectedColumns: metric.selectedColumns,

		metadata: metric.metadata,
	}, nil
}

func (a *pancakeTransformer) bucketAggregationToLayer(previousAggrNames []string, bucket *pancakeAggregationTreeNode) (layer *pancakeModelBucketAggregation, err error) {
	if bucket == nil {
		return nil, fmt.Errorf("bucket aggregation is nil")

	}
	if bucket.queryType.AggregationType() != model.BucketAggregation {
		return nil, fmt.Errorf("bucket aggregation %s is not bucket aggregation, type: %s", bucket.name, bucket.queryType.AggregationType().String())
	}

	return &pancakeModelBucketAggregation{
		name:            bucket.name,
		internalName:    a.generateBucketInternalName(append(previousAggrNames, bucket.name)),
		queryType:       bucket.queryType,
		selectedColumns: bucket.selectedColumns,

		orderBy:                 bucket.orderBy,
		limit:                   bucket.limit,
		isKeyed:                 bucket.isKeyed,
		filterOurEmptyKeyBucket: bucket.filterOutEmptyKeyBucket,

		metadata: bucket.metadata,
	}, nil
}

type layerAndNextBucket struct {
	layer                 *pancakeModelLayer
	nextBucketAggregation *pancakeAggregationTreeNode
}

func (a *pancakeTransformer) createLayer(previousAggrNames []string, childAggregations []*pancakeAggregationTreeNode) (result []layerAndNextBucket, err error) {

	if len(childAggregations) == 0 {
		return nil, nil
	}

	result = make([]layerAndNextBucket, 1)

	result[0].layer = &pancakeModelLayer{
		currentMetricAggregations: make([]*pancakeModelMetricAggregation, 0),
	}

	for _, childAgg := range childAggregations {
		if childAgg.queryType == nil {
			return nil, fmt.Errorf("query type is nil in createLayer")
		}
		switch childAgg.queryType.AggregationType() {
		case model.MetricsAggregation:
			metrics, err := a.metricAggregationTreeNodeToModel(previousAggrNames, childAgg)
			if err != nil {
				return nil, err
			}
			result[0].layer.currentMetricAggregations = append(result[0].layer.currentMetricAggregations, metrics)

		case model.BucketAggregation:
			if result[0].nextBucketAggregation != nil {
				return nil, fmt.Errorf("two bucket aggregation on same level are not supported: %s, %s",
					result[0].nextBucketAggregation.name, childAgg.name)
			}

			bucket, err := a.bucketAggregationToLayer(previousAggrNames, childAgg)
			if err != nil {
				return nil, err
			}

			result[0].layer.nextBucketAggregation = bucket
			result[0].nextBucketAggregation = childAgg
		default:
			return nil, fmt.Errorf("unsupported aggregation type in pancake, name: %s, type: %s",
				childAgg.name, childAgg.queryType.AggregationType().String())
		}
	}
	return result, nil
}

func (a *pancakeTransformer) aggregationTreeToPancake(topLevel pancakeAggregationTree) (pancakeResult *pancakeModel, err error) {
	if topLevel.children == nil || len(topLevel.children) == 0 {
		return nil, fmt.Errorf("no top level aggregations found")
	}

	layers := make([]*pancakeModelLayer, 0)
	aggrNames := make([]string, 0)
	result, err := a.createLayer(aggrNames, topLevel.children)
	if err != nil {
		return nil, err
	}

	layers = append(layers, result[0].layer)

	for result[0].nextBucketAggregation != nil {
		aggrNames = append(aggrNames, result[0].nextBucketAggregation.name)
		result, err = a.createLayer(aggrNames, result[0].nextBucketAggregation.children)
		if err != nil {
			return nil, err
		}
		if len(result) == 0 || result[0].layer == nil {
			break // not sure if needed
		}

		layers = append(layers, result[0].layer)
	}

	// we need sort metric aggregation to generate consistent results, otherwise:
	// - tests are flaky
	// - database might not use it query cache
	for _, layer := range layers {
		sort.Slice(layer.currentMetricAggregations, func(i, j int) bool {
			return layer.currentMetricAggregations[i].name < layer.currentMetricAggregations[j].name
		})
	}

	sampleLimit := noSampleLimit
	if layers[0].nextBucketAggregation != nil {
		if sampler, ok := layers[0].nextBucketAggregation.queryType.(bucket_aggregations.SamplerInterface); ok {
			sampleLimit = sampler.GetSampleLimit()
		}
	}

	pancakeResult = &pancakeModel{
		layers:      layers,
		whereClause: topLevel.whereClause,
		sampleLimit: sampleLimit,
	}

	return
}
