// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package queryparser

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"quesma/model"
	"quesma/model/bucket_aggregations"
	"quesma/model/metrics_aggregations"
	"quesma/model/pipeline_aggregations"
	"testing"
)

func Test_aggregation_validator_pancake(t *testing.T) {

	// DSL for testing
	agg := func(a string, q model.QueryType) *pancakeAggregationLevel {
		return &pancakeAggregationLevel{
			name:      a,
			queryType: q,
		}
	}

	metrics := func(a string) *pancakeAggregationLevel {
		return agg(a, metrics_aggregations.Avg{})
	}

	pipeline := func(a string) *pancakeAggregationLevel {
		return agg(a, pipeline_aggregations.AverageBucket{})
	}

	bucket := func(a string, children ...*pancakeAggregationLevel) *pancakeAggregationLevel {
		return &pancakeAggregationLevel{
			name:      a,
			queryType: bucket_aggregations.Range{},
			children:  children,
		}
	}

	top := func(a ...*pancakeAggregationLevel) *pancakeAggregationTopLevel {
		return &pancakeAggregationTopLevel{
			children: a,
		}
	}

	// test cases
	tests := []struct {
		name string
		agg  *pancakeAggregationTopLevel
		fail bool
	}{
		{"no aggregations", top(), true},
		{"one metrics aggregation", top(metrics("foo")), false},
		{"one bucket aggregation", top(bucket("foo")), false},
		{"bucket in bucket in bucket ... ", top(bucket("a", bucket("b", bucket("c", metrics("d"))))), false},
		{"one bucket aggregation with metrics aggregations ", top(bucket("foo", metrics("1"), metrics("2"))), false},
		{"one bucket aggregation with bucket aggregations ", top(bucket("foo", bucket("1"), bucket("2"))), true},
		{"one bucket aggregation with pipeline aggregations ", top(bucket("foo", pipeline("1"), pipeline("2"))), true},
		{"one bucket aggregation with pipeline and metrics aggregations ", top(bucket("foo", pipeline("1"), metrics("2"))), true},
		{"multiple top level aggregations", top(metrics("foo"), bucket("bar", metrics("1"), metrics("2"))), true},
		{"multiple top level aggregations 2", top(metrics("foo"), bucket("bar", metrics("1"), metrics("2"), bucket("3"), bucket("4"))), true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := newAggregationValidatorPancake()

			err := v.validate(tt.agg)
			fmt.Println("err: ", err)
			assert.Equal(t, tt.fail, err != nil, err)
		})
	}
}
