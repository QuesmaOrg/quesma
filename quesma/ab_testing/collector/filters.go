// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package collector

import "math/rand"

type probabilisticSampler struct {
	ratio float64
}

func (t *probabilisticSampler) process(in EnrichedResults) (out EnrichedResults, drop bool, err error) {

	if rand.Float64() > t.ratio {
		return in, true, nil
	}

	return in, false, nil
}
