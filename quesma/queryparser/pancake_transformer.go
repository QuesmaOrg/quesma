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
		// we can occasionally treat filter as metric if it has no childs
		if _, isFilter := metric.queryType.(bucket_aggregations.FilterAgg); !isFilter {
			return nil, fmt.Errorf("metric %s aggregation is not metrics aggregation, type: %s", metric.name, metric.queryType.AggregationType().String())
		}
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

func (a *pancakeTransformer) optimizeSimpleFilter(previousAggrNames []string, result *layerAndNextBucket, childAgg *pancakeAggregationTreeNode) bool {
	_, isFilter := result.nextBucketAggregation.queryType.(bucket_aggregations.FilterAgg)
	_, isFilter2 := childAgg.queryType.(bucket_aggregations.FilterAgg)

	if isFilter && isFilter2 && len(childAgg.children) == 0 {
		metrics, err := a.metricAggregationTreeNodeToModel(previousAggrNames, childAgg)
		if err != nil {
			return false // not a big deal, we can make two pancake queries instead or get error there
		}
		result.layer.currentMetricAggregations = append(result.layer.currentMetricAggregations, metrics)
		return true
	}
	return false
}

func (a *pancakeTransformer) createLayer(previousAggrNames []string, childAggregations []*pancakeAggregationTreeNode) (result []layerAndNextBucket, err error) {

	if len(childAggregations) == 0 {
		return nil, nil
	}

	result = make([]layerAndNextBucket, 1)

	result[0].layer = &pancakeModelLayer{
		currentMetricAggregations: make([]*pancakeModelMetricAggregation, 0),
	}

	// we need sort aggregation to generate consistent results, otherwise:
	// - tests are flaky
	// - database might not use it query cache
	sort.Slice(childAggregations, func(i, j int) bool {
		return childAggregations[i].name < childAggregations[j].name
	})

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
			bucket, err := a.bucketAggregationToLayer(previousAggrNames, childAgg)
			if err != nil {
				return nil, err
			}

			if result[0].nextBucketAggregation == nil {
				result[0].layer.nextBucketAggregation = bucket
				result[0].nextBucketAggregation = childAgg
			} else {
				// if both leaf optimizations are filter and second one doesn't have children we can treat second as metric
				if a.optimizeSimpleFilter(previousAggrNames, &result[0], childAgg) {
					continue
				}

				// we need more pancakes as we support just one bucket layer
				layer := &pancakeModelLayer{
					currentMetricAggregations: make([]*pancakeModelMetricAggregation, 0),
					nextBucketAggregation:     bucket,
				}
				result = append(result, layerAndNextBucket{layer: layer, nextBucketAggregation: childAgg})
			}
		default:
			return nil, fmt.Errorf("unsupported aggregation type in pancake, name: %s, type: %s",
				childAgg.name, childAgg.queryType.AggregationType().String())
		}
	}
	return result, nil
}

func (a *pancakeTransformer) aggregationChildrenToLayers(aggrNames []string, children []*pancakeAggregationTreeNode) (resultLayers [][]*pancakeModelLayer, err error) {
	results, err := a.createLayer(aggrNames, children)
	if err != nil {
		return nil, err
	}
	if len(results) == 0 {
		return nil, nil
	}
	resultLayers = make([][]*pancakeModelLayer, 0, len(results))
	for _, res := range results {
		if res.nextBucketAggregation != nil {
			childLayers, err := a.aggregationChildrenToLayers(append(aggrNames, res.nextBucketAggregation.name), res.nextBucketAggregation.children)
			if err != nil {
				return nil, err
			}
			if len(childLayers) == 0 {
				resultLayers = append(resultLayers, []*pancakeModelLayer{res.layer})
			} else {
				for i, childLayer := range childLayers {
					newLayer := res.layer
					if i > 0 { // remove metrics
						newLayer = &pancakeModelLayer{
							currentMetricAggregations: make([]*pancakeModelMetricAggregation, 0),
							nextBucketAggregation:     res.layer.nextBucketAggregation,
						}
					}

					resultLayers = append(resultLayers, append([]*pancakeModelLayer{newLayer}, childLayer...))
				}
			}
		} else {
			resultLayers = append(resultLayers, []*pancakeModelLayer{res.layer})
		}
	}
	return resultLayers, nil
}

func (a *pancakeTransformer) aggregationTreeToPancakes(topLevel pancakeAggregationTree) (pancakeResults []*pancakeModel, err error) {
	if topLevel.children == nil || len(topLevel.children) == 0 {
		return nil, fmt.Errorf("no top level aggregations found")
	}

	resultLayers, err := a.aggregationChildrenToLayers([]string{}, topLevel.children)

	if err != nil {
		return nil, err
	}

	for _, layers := range resultLayers {
		sampleLimit := noSampleLimit
		if layers[0].nextBucketAggregation != nil {
			if sampler, ok := layers[0].nextBucketAggregation.queryType.(bucket_aggregations.SamplerInterface); ok {
				sampleLimit = sampler.GetSampleLimit()
			}
		}

		// for now we support filter only as last bucket aggregation
		for layerIdx, layer := range layers {
			if layer.nextBucketAggregation != nil {
				switch layer.nextBucketAggregation.queryType.(type) {
				case bucket_aggregations.FilterAgg:
					if layerIdx+1 < len(layers) && layers[layerIdx+1].nextBucketAggregation != nil {
						return nil, fmt.Errorf("filter aggregation must be last bucket aggregation")
					}
				}
			}
		}

		pancakeResults = append(pancakeResults, &pancakeModel{
			layers:      layers,
			whereClause: topLevel.whereClause,
			sampleLimit: sampleLimit,
		})
	}

	return
}
