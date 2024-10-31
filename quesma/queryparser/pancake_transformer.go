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
	switch queryType.(type) {
	case *metrics_aggregations.TopMetrics:
		prefix = "top_metrics"
	case *metrics_aggregations.TopHits:
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

	result[0].layer = newPancakeModelLayer(nil)

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
				layer := newPancakeModelLayer(bucket)
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
						newLayer = newPancakeModelLayer(res.layer.nextBucketAggregation)
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
	// Let's say we support everything. That'll be true when I add support for filters/date_range/range in the middle of aggregation tree (@trzysiek)
	// Erase this function by then.
	return nil
}

func (a *pancakeTransformer) connectPipelineAggregations(layers []*pancakeModelLayer) {
	for i, layer := range layers {
		for _, pipeline := range layer.currentPipelineAggregations {
			parentBucketLayer, layerIdx, err := a.findParentBucketLayer(layers[i:], pipeline.queryType)
			if err != nil {
				logger.WarnWithCtx(a.ctx).Err(err).Msg("could not find parent bucket layer")
				continue
			}

			parentBucketLayerIdx := i + layerIdx
			if parentBucketLayerIdx > 0 {
				pipeline.queryType.SetParentBucketAggregation(layers[parentBucketLayerIdx-1].nextBucketAggregation.queryType)
			}
			parentBucketLayer.childrenPipelineAggregations = append(parentBucketLayer.childrenPipelineAggregations, pipeline)
		}
	}
}

// returns nil if no parent bucket layer found

func (a *pancakeTransformer) findParentBucketLayer(layers []*pancakeModelLayer, queryType model.QueryType) (
	parentBucketLayer *pancakeModelLayer, layerIdx int, err error) {

	pipeline, ok := queryType.(model.PipelineQueryType)
	if !ok {
		return nil, -1, fmt.Errorf("query type is not pipeline aggregation")
	}

	layer := layers[0]
	for i, aggrName := range pipeline.GetPathToParent() {
		layer = layers[i]
		if layer.nextBucketAggregation == nil || layer.nextBucketAggregation.name != aggrName {
			return nil, -1, fmt.Errorf("could not find parent bucket layer")
		}
	}

	return layer, len(pipeline.GetPathToParent()), nil
}

func (a *pancakeTransformer) createTopHitAndTopMetricsPancakes(pancake *pancakeModel) (result []*pancakeModel, err error) {
	for layerIdx, layer := range pancake.layers {
		metricsWithoutTopHits := make([]*pancakeModelMetricAggregation, 0, len(layer.currentMetricAggregations))
		for metricIdx, metric := range layer.currentMetricAggregations {
			switch metric.queryType.(type) {
			case *metrics_aggregations.TopMetrics, *metrics_aggregations.TopHits:
				isLastLayer := layerIdx == len(pancake.layers)-1
				isOnlyAggregationOnLayer := len(layer.currentMetricAggregations) == 1 ||
					// if we have several top_metrics at bottom layer we can still optimize last one
					(len(layer.currentMetricAggregations)-1 == metricIdx && len(metricsWithoutTopHits) == 0)
				canOptimize := isLastLayer && isOnlyAggregationOnLayer
				for _, layer2 := range pancake.layers[:layerIdx] {
					if len(layer2.currentMetricAggregations) > 0 {
						canOptimize = false
					}
					if layer2.nextBucketAggregation != nil {
						switch layer2.nextBucketAggregation.queryType.(type) {
						case bucket_aggregations.CombinatorAggregationInterface:
							canOptimize = false // might be changed, but not worth effort for now
						}
					}
				}

				if canOptimize { // if this is just one top_hits with buckets than we can optimize
					// and don't need to create new pancake
					metricsWithoutTopHits = append(metricsWithoutTopHits, metric)
				} else {
					arrayOfNewLayers := [][]*pancakeModelLayer{[]*pancakeModelLayer{}}
					//newLayers := make([]*pancakeModelLayer, layerIdx)
					for idx := range layerIdx {
						switch queryType := pancake.layers[idx].nextBucketAggregation.queryType.(type) {
						case bucket_aggregations.CombinatorAggregationInterface:
							newArrayOfNewLayers := make([][]*pancakeModelLayer, 0)
							for _, nextBucketAggregationQueryType := range queryType.CombinatorSplit() {
								for _, newLayers := range arrayOfNewLayers {
									nextBucketAggregation := pancake.layers[idx].nextBucketAggregation.ShallowClone()
									nextBucketAggregation.queryType = nextBucketAggregationQueryType
									newArrayOfNewLayers = append(newArrayOfNewLayers,
										append(newLayers, newPancakeModelLayer(&nextBucketAggregation)))
								}
							}
							arrayOfNewLayers = newArrayOfNewLayers
						default:
							for i := range arrayOfNewLayers {
								arrayOfNewLayers[i] = append(arrayOfNewLayers[i],
									newPancakeModelLayer(pancake.layers[idx].nextBucketAggregation))
							}
						}
					}
					for _, newLayers := range arrayOfNewLayers {
						newLayer := newPancakeModelLayer(nil)
						newLayer.currentMetricAggregations = append(newLayer.currentMetricAggregations, metric)
						newLayers = append(newLayers, newLayer)

						newPancake := pancakeModel{
							layers:      newLayers,
							whereClause: pancake.whereClause,
							sampleLimit: pancake.sampleLimit,
						}
						result = append(result, &newPancake)
					}
				}
			default:
				metricsWithoutTopHits = append(metricsWithoutTopHits, metric)
			}
		}
		layer.currentMetricAggregations = metricsWithoutTopHits
	}
	return
}

func (a *pancakeTransformer) aggregationTreeToPancakes(topLevel pancakeAggregationTree) (pancakeResults []*pancakeModel, err error) {
	if len(topLevel.children) == 0 {
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
		additionalTopHitPancakes, err := a.createTopHitAndTopMetricsPancakes(&newPancake)
		if err != nil {
			return nil, err
		}
		pancakeResults = append(pancakeResults, additionalTopHitPancakes...)
		pancakeResults = append(pancakeResults, a.createFiltersPancakes(&newPancake)...)
	}

	for _, pancake := range pancakeResults {
		fmt.Println("PANC", pancake.whereClause)
		fmt.Println()
	}

	return
}

func (a *pancakeTransformer) createFiltersPancakes(pancake *pancakeModel) (newPancakes []*pancakeModel) {
	if len(pancake.layers) == 0 || pancake.layers[0].nextBucketAggregation == nil {
		return
	}

	firstLayer := pancake.layers[0]
	filters, isFilters := firstLayer.nextBucketAggregation.queryType.(bucket_aggregations.Filters)
	if !isFilters {
		return
	}
	fmt.Println("WTF PRZECIEZ TUTAJ")
	if len(firstLayer.currentMetricAggregations) == 0 && len(firstLayer.currentPipelineAggregations) == 0 && len(pancake.layers) > 1 { // maybe secondLayer, not first?
		// If filter is in the first layer, we can just add it to the where clause
		fmt.Println("WTF PRZECIEZ TUTAJ 2", len(filters.Filters), filters.Filters)
		for i, filter := range filters.Filters[1:] {
			newPancake := pancake.Clone()
			// new (every) pancake has only 1 filter instead of all
			bucketAggr := newPancake.layers[0].nextBucketAggregation.ShallowClone()
			bucketAggr.queryType = filters.NewFiltersSingleFilter(i + 1)
			newPancake.layers[0] = newPancakeModelLayer(&bucketAggr)
			newPancake.whereClause = model.And([]model.Expr{newPancake.whereClause, filter.Sql.WhereClause})
			fmt.Println("WTF PRZECIEZ TUTAJ 3", newPancake.whereClause)
			newPancakes = append(newPancakes, newPancake)
		}
		pancake.layers[0].nextBucketAggregation.queryType = filters.NewFiltersSingleFilter(0)
	}
	return
}
