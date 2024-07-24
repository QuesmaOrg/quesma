// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package queryparser

import (
	"fmt"
	"quesma/model"
	"strings"
)

type aggregationValidatorUna struct {
}

func newAggregationValidatorUna() aggregationValidatorUna {
	return aggregationValidatorUna{}
}

// validate checks if the aggregation tree can be translated to a single SQL
func (v *aggregationValidatorUna) validate(agg *aggregationTopLevelVersionUna) error {

	if len(agg.children) == 0 {
		return fmt.Errorf("no aggregations found")
	}

	if len(agg.children) > 1 {
		return fmt.Errorf("top level aggregation can have at most 1 child but got: %d", len(agg.children))
	}

	walker := &aggregationValidatorUnaWalker{}

	describe := func(agg *aggregationLevelVersionUna) string {
		return fmt.Sprintf("'%s' (%s)", strings.Join(walker.currentAgg, "."), agg.queryType.AggregationType())
	}

	walker.visitMetrics = func(agg *aggregationLevelVersionUna) (any, error) {

		if len(agg.children) > 0 {
			return nil, fmt.Errorf("metrics aggregation can't have children: %s", describe(agg))
		}

		return agg, nil
	}

	walker.visitTypical = func(agg *aggregationLevelVersionUna) (any, error) {
		return nil, fmt.Errorf("unsupported aggregation type: %s", describe(agg))
	}

	walker.visitPipeline = func(agg *aggregationLevelVersionUna) (any, error) {
		return nil, fmt.Errorf("pipeline aggregations are not supported: %s", describe(agg))
	}

	walker.visitUnknown = func(agg *aggregationLevelVersionUna) (any, error) {
		return nil, fmt.Errorf("unknown aggregation are not supported at all: %s", describe(agg))
	}

	walker.visitBucket = func(agg *aggregationLevelVersionUna) (any, error) {

		var buckets int

		for _, child := range agg.children {
			if child.queryType.AggregationType() == model.BucketAggregation {
				buckets++
			}
		}

		if buckets > 1 {
			return nil, fmt.Errorf("bucket aggregation have to many sub buckets: %s - %d", describe(agg), buckets)
		}

		return walker.visitChildren(agg.children)
	}

	_, err := walker.walkTopLevel(agg)

	return err
}
