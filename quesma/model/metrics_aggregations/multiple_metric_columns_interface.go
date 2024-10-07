// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package metrics_aggregations

// MultipleMetricColumnsInterface is an interface for metrics aggregations
// that have multiple columns in the response.
// It allows to get the index of the column by its name, e.g.
// "count", or "standard_deviation" for extended_stats, or "50" for quantile.
type MultipleMetricColumnsInterface interface {
	ColumnIdx(name string) int
}
