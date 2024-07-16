// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package queryparser

import (
	"context"
	"fmt"
	"quesma/logger"
	"quesma/model"
	"quesma/model/metrics_aggregations"
	"quesma/model/typical_queries"
)

// maybe move to separate package?

//type QueryOptimizationPipeline struct {
//	transformers []plugins.QueryTransformer
//}

type MergeMetricsAggsTransformer struct {
	ctx context.Context
}

func (t MergeMetricsAggsTransformer) Transform(queries []*model.Query) ([]*model.Query, error) {
	fmt.Println("queries len:", len(queries))
	for i, queryToMerge := range queries {
		a, ok := queryToMerge.Type.(metrics_aggregations.MetricsAggregation)
		if !ok {
			fmt.Println("Not metrics", queryToMerge, a)
			// if not metrics, skip. We merge only metrics.
			continue
		}

		// try to merge queryToMerge with some previous query
		for j, queryToMergeWith := range queries[:i] {
			fmt.Println(i, j)
			if t.mergeable(queryToMerge, queryToMergeWith) {
				fmt.Println("QQQQ", t.ctx == nil)
				logger.DebugWithCtx(t.ctx).Msgf("Merging query %d with %d", j, i)
				t.merge(queryToMerge, queryToMergeWith)
				break
			}
		}
	}
	return queries, nil
}

func (t MergeMetricsAggsTransformer) mergeable(query1, query2 *model.Query) bool {
	_, isMetrics1 := query1.Type.(metrics_aggregations.MetricsAggregation)
	_, isMetrics2 := query2.Type.(metrics_aggregations.MetricsAggregation)
	fmt.Println("isMetrics1", isMetrics1, "isMetrics2", isMetrics2)
	if !isMetrics1 || !isMetrics2 {
		return false
	}

	// special case: (count with no limit, aggregation with 1 aggregator) is also mergeable
	if t.isTypicalCount(query1) && len(query2.Aggregators) == 1 && query1.SelectCommand.SampleLimit == 0 {
		return true
	}
	if t.isTypicalCount(query2) && len(query1.Aggregators) == 1 && query2.SelectCommand.SampleLimit == 0 {
		return true
	}

	// queries need to have the same parents, so equal lengths + equal parents (N - 1 aggregators)
	fmt.Println("a", query1.Aggregators, query1.Aggregators[0])
	fmt.Println("b", query2.Aggregators)
	if len(query1.Aggregators) != len(query2.Aggregators) {
		return false
	}
	for i := 0; i < len(query1.Aggregators)-1; i++ {
		if query1.Aggregators[i] != query2.Aggregators[i] {
			return false
		}
		q1Filters := query1.Aggregators[i].Filters
		q2Filters := query2.Aggregators[i].Filters
		if (q1Filters && !q2Filters) || (!q1Filters && q2Filters) {
			return false
		}
	}

	return true
}

func (t MergeMetricsAggsTransformer) merge(queryToMerge, queryToMergeWith *model.Query) {
	colNr := len(queryToMerge.SelectCommand.Columns)
	if colNr == 0 {
		logger.WarnWithCtx(t.ctx).Msg("mergeMetricsAggsTransformer: no columns to merge")
		return
	}

	if _, ok := queryToMergeWith.Type.(*metrics_aggregations.MetricsWrapper); !ok {
		queryToMergeWith.Type = metrics_aggregations.NewMetricsWrapped(
			t.ctx,
			queryToMergeWith.Type.(metrics_aggregations.MetricsAggregation),
			len(queryToMergeWith.SelectCommand.Columns)-1,
			queryToMergeWith,
		)
	}

	fmt.Printf("before %s %T\n", queryToMergeWith.Type.String(), queryToMergeWith.Type)
	firstIdx := colNr - queryToMerge.Type.(metrics_aggregations.MetricsAggregation).ColumnsNr()
	if firstIdx < 0 {
		logger.ErrorWithCtx(t.ctx).Msgf("mergeMetricsAggsTransformer: firstIdx < 0: %d", firstIdx)
		return
	}
	queryToMergeWith.SelectCommand.Columns = append(queryToMergeWith.SelectCommand.Columns, queryToMerge.SelectCommand.Columns[firstIdx:]...)

	queryToMerge.Type = metrics_aggregations.NewMetricsWrapped(
		t.ctx,
		queryToMerge.Type.(metrics_aggregations.MetricsAggregation),
		len(queryToMergeWith.SelectCommand.Columns)-1,
		queryToMergeWith,
	)
	queryToMerge.NoDBQuery = true // change to isPipeline or sth
	fmt.Printf("after %s %T\n", queryToMergeWith.Type.String(), queryToMergeWith.Type)
}

func (t MergeMetricsAggsTransformer) isTypicalCount(query *model.Query) bool {
	if _, ok := query.Type.(typical_queries.Count); ok {
		return true
	}
	if wrapper, ok := query.Type.(*metrics_aggregations.MetricsWrapper); ok {
		_, ok = wrapper.GetWrapped().(typical_queries.Count)
		return ok
	}
	return false
}
