// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package bucket_aggregations

type SamplerInterface interface {
	GetSampleLimit() int
}
