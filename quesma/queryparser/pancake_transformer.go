// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package queryparser

import (
	"context"
	"fmt"
	"quesma/logger"
	"quesma/model"
	"quesma/model/bucket_aggregations"
	"quesma/model/metrics_aggregations"
	"reflect"
	"sort"
	"strings"
)

// 2. Translate aggregation tree into pancake model.
type pancakeTransformer struct {
	ctx       context.Context
	usedNames map[string][]string
}

func newPancakeTransformer(ctx context.Context) pancakeTransformer {
	return pancakeTransformer{
		ctx:       ctx,
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

func (a *pancakeTransformer) generateMetricInternalName(aggrNames []string, queryType model.QueryType) string {
	prefix := "metric"
	if _, isTopHits := queryType.(metrics_aggregations.TopHits); isTopHits {
		prefix = "top_hits"
	}
	origName := fmt.Sprintf("%s__%s", prefix, strings.Join(aggrNames, "__"))
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
		internalName:    a.generateMetricInternalName(append(previousAggrNames, metric.name), metric.queryType),
		queryType:       metric.queryType,
		selectedColumns: metric.selectedColumns,

		metadata: metric.metadata,
	}, nil
}

func (a *pancakeTransformer) pipelineAggregationToLayer(previousAggrNames []string, pipeline *pancakeAggregationTreeNode) (layer *pancakeModelPipelineAggregation, err error) {
	if pipeline == nil {
		return nil, fmt.Errorf("pipeline aggregation is nil")
	}

	pipelineQueryType, ok := pipeline.queryType.(model.PipelineQueryType)
	if !ok {
		return nil, fmt.Errorf("pipeline aggregation %s is not pipeline aggregation, type: %s", pipeline.name, pipeline.queryType.AggregationType().String())
	}

	return newPancakeModelPipelineAggregation(pipeline.name, previousAggrNames, pipelineQueryType, pipeline.metadata), nil
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
	secondFilter, isFilter2 := childAgg.queryType.(bucket_aggregations.FilterAgg)

	if isFilter && isFilter2 && len(childAgg.children) == 0 {
		metrics, err := a.metricAggregationTreeNodeToModel(previousAggrNames, childAgg)
		if err != nil {
			return false // not a big deal, we can make two pancake queries instead or get error there
		}
		metrics.selectedColumns = []model.Expr{model.NewFunction("countIf", secondFilter.WhereClause)}
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

	result[0].layer = newPancakeModelLayer()

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

		case model.PipelineMetricsAggregation, model.PipelineBucketAggregation:
			pipeline, err := a.pipelineAggregationToLayer(previousAggrNames, childAgg)
			if err != nil {
				return nil, err
			}

			result[0].layer.currentPipelineAggregations = append(result[0].layer.currentPipelineAggregations, pipeline)

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
						newLayer = newPancakeModelLayer()
						newLayer.nextBucketAggregation = res.layer.nextBucketAggregation
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

func (a *pancakeTransformer) checkIfSupported(layers []*pancakeModelLayer) error {
	// for now we support filter only as last bucket aggregation
	for layerIdx, layer := range layers {
		if layer.nextBucketAggregation != nil {
			switch layer.nextBucketAggregation.queryType.(type) {
			case bucket_aggregations.CombinatorAggregationInterface:
				for _, followingLayer := range layers[layerIdx+1:] {
					bucket := followingLayer.nextBucketAggregation
					if bucket != nil {
						switch bucket.queryType.(type) {
						case *bucket_aggregations.DateHistogram:
							continue // histogram are fine
						case bucket_aggregations.CombinatorAggregationInterface:
							continue // we also support nested filters/range/dataRange
						default:
							return fmt.Errorf("filter(s)/range/dataRange aggregation must be the last bucket aggregation")
						}
					}
				}
			}
		}
	}
	return nil
}

func (a *pancakeTransformer) connectPipelineAggregations(layers []*pancakeModelLayer) {
	for i, layer := range layers {
		for _, pipeline := range layer.currentPipelineAggregations {
			parentBucketLayer, err := a.findParentBucketLayer(layers[i:], pipeline.queryType)
			if err != nil {
				logger.WarnWithCtx(a.ctx).Err(err).Msg("could not find parent bucket layer")
				continue
			}
			parentBucketLayer.childrenPipelineAggregations = append(parentBucketLayer.childrenPipelineAggregations, pipeline)
		}
	}
}

// returns nil if no parent bucket layer found
func (a *pancakeTransformer) findParentBucketLayer(layers []*pancakeModelLayer, queryType model.QueryType) (
	parentBucketLayer *pancakeModelLayer, err error) {

	pipeline, ok := queryType.(model.PipelineQueryType)
	if !ok {
		return nil, fmt.Errorf("query type is not pipeline aggregation")
	}

	layer := layers[0]
	for i, aggrName := range pipeline.GetPathToParent() {
		layer = layers[i]
		if layer.nextBucketAggregation == nil || layer.nextBucketAggregation.name != aggrName {
			return nil, fmt.Errorf("could not find parent bucket layer")
		}
	}
	return layer, nil
}

func (a *pancakeTransformer) createTopHitPancakes(pancake *pancakeModel) (result []*pancakeModel, err error) {
	for layerIdx, layer := range pancake.layers {
		metricsWithoutTopHits := make([]*pancakeModelMetricAggregation, 0, len(layer.currentMetricAggregations))
		for _, metric := range layer.currentMetricAggregations {
			if _, isTopHits := metric.queryType.(metrics_aggregations.TopHits); isTopHits {
				canOptimize := layerIdx == len(pancake.layers)-1 && len(layer.currentMetricAggregations) == 1
				for _, layer2 := range pancake.layers[:layerIdx] {
					if len(layer2.currentMetricAggregations) > 0 {
						canOptimize = false
					}
					if layer2.nextBucketAggregation != nil {
						switch layer2.nextBucketAggregation.queryType.(type) {
						case bucket_aggregations.CombinatorAggregationInterface:
							// TODO: possible to implement, by generating more queries, skipped as it is rare
							return nil, fmt.Errorf("top_hits can't be in filter(s)/range/dataRange aggregation")
						}
					}
				}

				if canOptimize { // if this is just one top_hits with buckets than we can optimize
					// and don't need to create new pancake
					metricsWithoutTopHits = append(metricsWithoutTopHits, metric)
				} else {
					newLayers := make([]*pancakeModelLayer, layerIdx)
					for i := range newLayers {
						newLayers[i] = &pancakeModelLayer{
							currentMetricAggregations: make([]*pancakeModelMetricAggregation, 0),
							nextBucketAggregation:     pancake.layers[i].nextBucketAggregation,
						}
					}
					newLayers = append(newLayers, &pancakeModelLayer{
						currentMetricAggregations: []*pancakeModelMetricAggregation{metric},
						nextBucketAggregation:     nil,
					})

					newPancake := pancakeModel{
						layers:      newLayers,
						whereClause: pancake.whereClause,
						sampleLimit: pancake.sampleLimit,
					}
					result = append(result, &newPancake)
				}
			} else {
				metricsWithoutTopHits = append(metricsWithoutTopHits, metric)
			}
		}
		layer.currentMetricAggregations = metricsWithoutTopHits
	}
	return
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

		if err := a.checkIfSupported(layers); err != nil {
			return nil, err
		}

		a.connectPipelineAggregations(layers)

		newPancake := pancakeModel{
			layers:      layers,
			whereClause: topLevel.whereClause,
			sampleLimit: sampleLimit,
		}

		pancakeResults = append(pancakeResults, &newPancake)

		additionalTopHitPancakes, err := a.createTopHitPancakes(&newPancake)
		if err != nil {
			return nil, err
		}

		pancakeResults = append(pancakeResults, additionalTopHitPancakes...)
	}

	return
}
