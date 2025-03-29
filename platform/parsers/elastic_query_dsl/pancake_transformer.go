// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package elastic_query_dsl

import (
	"context"
	"fmt"
	"github.com/QuesmaOrg/quesma/platform/logger"
	"github.com/QuesmaOrg/quesma/platform/model"
	"github.com/QuesmaOrg/quesma/platform/model/bucket_aggregations"
	"github.com/QuesmaOrg/quesma/platform/model/metrics_aggregations"
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

	metrics := make([]*pancakeModelMetricAggregation, 0)

	for _, childAgg := range childAggregations {
		if childAgg.queryType == nil {
			return nil, fmt.Errorf("query type is nil in createLayer")
		}
		switch childAgg.queryType.AggregationType() {
		case model.MetricsAggregation:
			metric, err := a.metricAggregationTreeNodeToModel(previousAggrNames, childAgg)
			if err != nil {
				return nil, err
			}
			metrics = append(metrics, metric)

		case model.BucketAggregation:
			filter, isFilter := childAgg.queryType.(bucket_aggregations.FilterAgg)
			if isFilter && len(childAgg.children) == 0 {
				childAgg.selectedColumns = append(childAgg.selectedColumns, model.NewFunction("countIf", filter.WhereClause))
				metric, err := a.metricAggregationTreeNodeToModel(previousAggrNames, childAgg)
				if err != nil {
					return nil, err
				}
				metrics = append(metrics, metric)
				break
			}

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
	for _, resultLayer := range result {
		resultLayer.layer.currentMetricAggregations = metrics
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
			if parentBucketLayerIdx > 0 && layers[parentBucketLayerIdx-1].nextBucketAggregation != nil {
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
		asBucket := layer.nextBucketAggregation != nil && layer.nextBucketAggregation.name == aggrName
		asMetric := false
		for _, metric := range layer.currentMetricAggregations {
			if metric.name == aggrName {
				asMetric = true
				break
			}
		}

		if !asBucket && !asMetric {
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

// Auto date histogram is a date histogram, that automatically creates buckets based on time range.
// To do that we need parse WHERE clause which happens in this method.
func (a *pancakeTransformer) transformAutoDateHistogram(layers []*pancakeModelLayer, whereClause model.Expr) {
	for _, layer := range layers {
		if layer.nextBucketAggregation != nil {
			if autoDateHistogram, ok := layer.nextBucketAggregation.queryType.(*bucket_aggregations.AutoDateHistogram); ok {
				if tsLowerBound, found := model.FindTimestampLowerBound(autoDateHistogram.GetField(), whereClause); found {
					autoDateHistogram.SetKey(tsLowerBound.UnixMilli())
				} else {
					logger.WarnWithCtx(a.ctx).Msgf("could not find timestamp lower bound (field: %v, where clause: %v)",
						autoDateHistogram.GetField(), whereClause)
				}
			}
		}
	}
}

// Auto date histogram is a date histogram, that automatically creates buckets based on time range.
// To do that we need parse WHERE clause which happens in this method.
func (a *pancakeTransformer) transformRate(layers []*pancakeModelLayer) {
	for i, layer := range layers[:len(layers)-1] {
		if layer.nextBucketAggregation == nil {
			continue
		}
		if dateHistogram, ok := layer.nextBucketAggregation.queryType.(*bucket_aggregations.DateHistogram); ok {
			if dhInterval, ok := dateHistogram.Interval(); ok {
				for _, metric := range layers[i+1].currentMetricAggregations {
					if rate, ok := metric.queryType.(*metrics_aggregations.Rate); ok {
						rate.CalcAndSetMultiplier(dhInterval)
					}
				}
			}
		}
	}
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
		a.transformAutoDateHistogram(layers, topLevel.whereClause)
		a.transformRate(layers)

		newPancake := pancakeModel{
			layers:      layers,
			whereClause: topLevel.whereClause,
			sampleLimit: sampleLimit,
		}
		pancakeResults = append(pancakeResults, &newPancake)

		// TODO: if both top_hits/top_metrics, and filters, it probably won't work...
		// Care: order of these two functions is unfortunately important.
		// Should be fixed after this TODO
		newCombinatorPancakes := a.createCombinatorPancakes(&newPancake)
		additionalTopHitPancakes, err := a.createTopHitAndTopMetricsPancakes(&newPancake)
		if err != nil {
			return nil, err
		}

		pancakeResults = append(pancakeResults, additionalTopHitPancakes...)
		pancakeResults = append(pancakeResults, newCombinatorPancakes...)
	}

	return
}

// createFiltersPancakes only does something, if first layer aggregation is Filters.
// It creates new pancakes for each filter in that aggregation, and updates `pancake` to have only first filter.
func (a *pancakeTransformer) createCombinatorPancakes(pancake *pancakeModel) (newPancakes []*pancakeModel) {
	if len(pancake.layers) == 0 || pancake.layers[0].nextBucketAggregation == nil {
		return
	}

	firstLayer := pancake.layers[0]
	combinator, isCombinator := firstLayer.nextBucketAggregation.queryType.(bucket_aggregations.CombinatorAggregationInterface)
	if !isCombinator {
		return
	}

	noMoreBucket := len(pancake.layers) <= 1 || (len(pancake.layers) == 2 && pancake.layers[1].nextBucketAggregation == nil)
	noMetricOnFirstLayer := len(firstLayer.currentMetricAggregations) == 0 && len(firstLayer.currentPipelineAggregations) == 0
	canSimplyAddCombinatorToWhereClause := noMoreBucket && noMetricOnFirstLayer
	if canSimplyAddCombinatorToWhereClause {
		return
	}

	areNewPancakesReallyNeeded := len(pancake.layers) > 1 // if there is only one layer above combinator, it easily can be done with 1 pancake, no need for more
	groups := combinator.CombinatorGroups()
	if !areNewPancakesReallyNeeded || len(groups) == 0 {
		return
	}

	combinatorSplit := combinator.CombinatorSplit()
	combinatorGroups := combinator.CombinatorGroups()
	// First create N-1 new pancakes [1...N), each with different filter
	// (important to update the first (0th) pancake at the end)
	for i := 1; i < len(groups); i++ {
		newPancake := pancake.Clone()
		bucketAggr := newPancake.layers[0].nextBucketAggregation.ShallowClone()
		bucketAggr.queryType = combinatorSplit[i]
		newPancake.layers[0] = newPancakeModelLayer(&bucketAggr)
		newPancake.whereClause = model.And([]model.Expr{newPancake.whereClause, combinatorGroups[i].WhereClause})
		newPancakes = append(newPancakes, newPancake)
	}

	// Update original
	pancake.layers[0].nextBucketAggregation.queryType = combinatorSplit[0]
	pancake.whereClause = model.And([]model.Expr{pancake.whereClause, combinatorGroups[0].WhereClause})

	return
}
