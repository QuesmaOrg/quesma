// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package collector

type diffTransformer struct {
}

func (t *diffTransformer) process(in EnrichedResults) (out EnrichedResults, drop bool, err error) {

	// TODO add real diff logic here

	if in.A.Body != in.B.Body {
		in.Diff.BodyDiff = "Compute diff here"
		in.Diff.IsDiff = true
	}
	return in, false, nil
}
