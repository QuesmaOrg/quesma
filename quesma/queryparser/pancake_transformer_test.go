// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package queryparser

import (
	"github.com/stretchr/testify/assert"
	"quesma/model"
	"quesma/model/bucket_aggregations"
	"quesma/model/metrics_aggregations"
	"testing"
)

func Test_pancakeTranslateFromAggregationToLayered(t *testing.T) {

	// DSL for testing

	// input

	agg := func(a string, q model.QueryType) *pancakeAggregationTreeNode {
		return &pancakeAggregationTreeNode{
			name:      a,
			queryType: q,
		}
	}

	metrics := func(a string) *pancakeAggregationTreeNode {
		return agg(a, metrics_aggregations.Avg{})
	}

	bucket := func(a string, children ...*pancakeAggregationTreeNode) *pancakeAggregationTreeNode {

		if children == nil {
			children = make([]*pancakeAggregationTreeNode, 0)
		}

		return &pancakeAggregationTreeNode{
			name:      a,
			queryType: bucket_aggregations.Range{},
			children:  children,
		}
	}

	top := func(a ...*pancakeAggregationTreeNode) *pancakeAggregationTree {
		return &pancakeAggregationTree{
			children: a,
		}
	}

	// output

	panBucket := func(a, b string) *pancakeModelBucketAggregation {
		return &pancakeModelBucketAggregation{
			name:         a,
			internalName: b,
			queryType:    bucket_aggregations.Range{},
		}
	}

	panMetric := func(a, b string) *pancakeModelMetricAggregation {
		return &pancakeModelMetricAggregation{
			name:         a,
			internalName: b,
			queryType:    metrics_aggregations.Avg{},
		}
	}

	layer := func(bucket *pancakeModelBucketAggregation, metrics ...*pancakeModelMetricAggregation) *pancakeModelLayer {
		return &pancakeModelLayer{
			nextBucketAggregation:     bucket,
			currentMetricAggregations: metrics,
		}
	}

	pancake := func(panLayers ...*pancakeModelLayer) *pancakeModel {
		return &pancakeModel{
			layers: panLayers,
		}
	}

	// test cases
	tests := []struct {
		name    string
		tree    *pancakeAggregationTree
		pancake *pancakeModel
	}{

		{"one bucket aggregation",
			top(bucket("bucket_1", metrics("metrics_1"), metrics("metrics_2"))),
			pancake(
				layer(panBucket("bucket_1", "aggr__bucket_1__")),
				layer(nil, panMetric("metrics_1", "metric__bucket_1__metrics_1"), panMetric("metrics_2", "metric__bucket_1__metrics_2"))),
		},

		{"bucket in bucket  ... ",
			top(bucket("bucket_1", bucket("bucket_2"))),
			pancake(
				layer(panBucket("bucket_1", "aggr__bucket_1__")),
				layer(panBucket("bucket_2", "aggr__bucket_1__bucket_2__"))),
		},

		{"one bucket aggregation with metrics aggregations ",
			top(bucket("bucket_1", metrics("metrics_1"), metrics("metrics_2"))),
			pancake(
				layer(panBucket("bucket_1", "aggr__bucket_1__")),
				layer(nil, panMetric("metrics_1", "metric__bucket_1__metrics_1"), panMetric("metrics_2", "metric__bucket_1__metrics_2"))),
		},

		{"one bucket aggregation with metrics aggregations and bucket aggregations",
			top(bucket("bucket_1", metrics("metrics_1"), metrics("metrics_2"), bucket("bucket_2", metrics("metrics_3")))),
			pancake(
				layer(panBucket("bucket_1", "aggr__bucket_1__")),
				layer(panBucket("bucket_2", "aggr__bucket_1__bucket_2__"), panMetric("metrics_1", "metric__bucket_1__metrics_1"), panMetric("metrics_2", "metric__bucket_1__metrics_2")),
				layer(nil, panMetric("metrics_3", "metric__bucket_1__bucket_2__metrics_3"))),
		},

		{"one bucket aggregation with metrics aggregations and bucket aggregations",
			top(bucket("bucket_1", metrics("metrics_1"), metrics("metrics_2"), bucket("bucket_2", bucket("bucket_3"), metrics("metrics_3")))),
			pancake(
				layer(panBucket("bucket_1", "aggr__bucket_1__")),
				layer(panBucket("bucket_2", "aggr__bucket_1__bucket_2__"), panMetric("metrics_1", "metric__bucket_1__metrics_1"), panMetric("metrics_2", "metric__bucket_1__metrics_2")),
				layer(panBucket("bucket_3", "aggr__bucket_1__bucket_2__bucket_3__"), panMetric("metrics_3", "metric__bucket_1__bucket_2__metrics_3"))),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			transformer := &pancakeTransformer{}

			pan, err := transformer.aggregationTreeToPancake(*tt.tree)

			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			/*
				fmt.Println("tree: ")
				pp.Println(tt.tree)
				fmt.Println("expected: ")
				pp.Println(tt.pancake)
				fmt.Println("pancake: ")
				pp.Println(pan)
			*/

			assert.Equal(t, len(tt.pancake.layers), len(pan.layers))

			// we compare only the structure not internal values

			for i, layer := range tt.pancake.layers {

				assert.Equal(t, layer.nextBucketAggregation, pan.layers[i].nextBucketAggregation)
				assert.Equal(t, len(layer.currentMetricAggregations), len(pan.layers[i].currentMetricAggregations))

				for j, metric := range layer.currentMetricAggregations {
					assert.Equal(t, metric, pan.layers[i].currentMetricAggregations[j])
				}
			}

		})
	}
}
