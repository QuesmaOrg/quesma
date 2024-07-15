// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package metrics_aggregations

import (
	"context"
	"fmt"
	"quesma/model"
)

type MetricsWrapper struct {
	ctx              context.Context
	wrapped          MetricsAggregation
	lastColIndex     int
	queryWithResults *model.Query
}

func NewMetricsWrapped(ctx context.Context, wrapped MetricsAggregation, lastColIndex int, queryWithResults *model.Query) *MetricsWrapper {
	return &MetricsWrapper{ctx: ctx, wrapped: wrapped, lastColIndex: lastColIndex, queryWithResults: queryWithResults}
}

func (query *MetricsWrapper) IsBucketAggregation() bool {
	return false
}

func (query *MetricsWrapper) TranslateSqlResponseToJson(rows []model.QueryResultRow, level int) model.JsonMap {
	fmt.Printf("kuku %s %d", query.String(), query.lastColIndex)
	return query.wrapped.TranslateSqlResponseToJson(rows, query.lastColIndex)
}

func (query *MetricsWrapper) String() string {
	return fmt.Sprintf("metrics_wrapper: (%s, lastColIndex: %d, dbQueryIndex: %d)",
		query.wrapped.String(), query.lastColIndex, query.queryWithResults.Type)
}

func (query *MetricsWrapper) PostprocessResults(rowsFromDB []model.QueryResultRow) []model.QueryResultRow {
	return rowsFromDB
}

func (query *MetricsWrapper) MetricsAggregation() {}

func (query *MetricsWrapper) GetQueryWithResults() *model.Query {
	return query.queryWithResults
}

func (query *MetricsWrapper) GetWrapped() MetricsAggregation {
	return query.wrapped
}
