// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package queryparser

import (
	"fmt"
	"quesma/model"
)

type aggregationValidatorUnaWalker struct {
	currentAgg []string // it stores "path" to the current aggregation

	visitMetrics  func(*aggregationLevelVersionUna) (any, error)
	visitBucket   func(*aggregationLevelVersionUna) (any, error)
	visitPipeline func(*aggregationLevelVersionUna) (any, error)
	visitTypical  func(*aggregationLevelVersionUna) (any, error)
	visitUnknown  func(*aggregationLevelVersionUna) (any, error)
}

func (w *aggregationValidatorUnaWalker) visitChildren(aggs []*aggregationLevelVersionUna) (interface{}, error) {

	var res []interface{}

	for _, child := range aggs {
		_, err := w.walk(child)
		if err != nil {
			return nil, err
		}
		res = append(res, child)
	}
	return res, nil
}

func (w *aggregationValidatorUnaWalker) walk(agg *aggregationLevelVersionUna) (interface{}, error) {

	w.currentAgg = append(w.currentAgg, agg.name)
	defer func() {
		if len(w.currentAgg) > 0 {
			w.currentAgg = w.currentAgg[:len(w.currentAgg)-1]
		}
	}()

	fmt.Println("walking ", agg.name, " ", agg.queryType.AggregationType(), " ", w.currentAgg)

	switch agg.queryType.AggregationType() {
	case model.MetricsAggregation:
		if w.visitMetrics != nil {
			return w.visitMetrics(agg)
		}
		return agg, nil
	case model.BucketAggregation:
		if w.visitBucket != nil {
			return w.visitBucket(agg)
		}
		_, err := w.visitChildren(agg.children)
		if err != nil {
			return nil, err
		}
		return agg, nil
	case model.PipelineAggregation:
		if w.visitPipeline != nil {
			return w.visitPipeline(agg)
		}
		return agg, nil
	case model.TypicalAggregation:
		if w.visitTypical != nil {
			return w.visitTypical(agg)
		}

		return agg, nil
	default:
		return nil, fmt.Errorf("unexpected aggregation type: %v", agg.queryType.AggregationType())
	}
}

func (w *aggregationValidatorUnaWalker) walkTopLevel(agg *aggregationTopLevelVersionUna) (interface{}, error) {
	return w.visitChildren(agg.children)
}
