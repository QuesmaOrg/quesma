// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package bucket_aggregations

// NoGroupByInterface is a special case of bucket aggregation which does not add group by.
// Examples: sampler, filter, filters, range and dataRange.
type NoGroupByInterface interface {
	DoesNotHaveGroupBy() bool
}
