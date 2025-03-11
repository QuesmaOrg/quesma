// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package collector

import "math/rand"

type probabilisticSampler struct {
	ratio float64
}

func (*probabilisticSampler) name() string {
	return "probabilisticSampler"
}

func (t *probabilisticSampler) process(in EnrichedResults) (out EnrichedResults, drop bool, err error) {

	if rand.Float64() > t.ratio {
		return in, true, nil
	}

	return in, false, nil
}

// mismatchedOnlyFilter is a filter results that only allows mismatched results to pass
type mismatchedOnlyFilter struct {
}

func (t *mismatchedOnlyFilter) name() string {
	return "mismatchedOnlyFilter"
}

func (t *mismatchedOnlyFilter) process(in EnrichedResults) (out EnrichedResults, drop bool, err error) {

	if in.Mismatch.IsOK {
		return in, true, nil
	}

	return in, false, nil
}

// avoid unused struct error
var _ = &mismatchedOnlyFilter{}

type redactOkResults struct {
}

func (t *redactOkResults) name() string {
	return "redactOkResults"
}

func (t *redactOkResults) process(in EnrichedResults) (out EnrichedResults, drop bool, err error) {

	// we're not interested in the details of the request and responses if the mismatch is OK

	redactMsg := "***REDACTED***"
	if in.Mismatch.IsOK {
		in.Request.Body = redactMsg
		in.A.Body = redactMsg
		in.B.Body = redactMsg
		in.Mismatch.Message = "OK"
	}

	return in, false, nil
}

var _ = &redactOkResults{}
