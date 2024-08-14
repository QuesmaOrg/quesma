// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package collector

import (
	"encoding/json"
	"quesma/jsondiff"
	"quesma/quesma/types"
)

type diffTransformer struct {
}

func (t *diffTransformer) mostCommonMismatchType(mismatches []jsondiff.JSONMismatch) (string, int) {

	currentMax := 0
	maxType := ""
	m := make(map[string]int)

	for _, mismatch := range mismatches {
		m[mismatch.Type]++
		if m[mismatch.Type] > currentMax {
			currentMax = m[mismatch.Type]
			maxType = mismatch.Type
		}
	}

	return maxType, currentMax

}

func (t *diffTransformer) process(in EnrichedResults) (out EnrichedResults, drop bool, err error) {

	d, err := jsondiff.NewElasticResponseJSONDiff()
	if err != nil {
		return in, false, err
	}

	jsonA, err := types.ParseJSON(in.A.Body)
	if err != nil {
		return in, false, err
	}

	jsonB, err := types.ParseJSON(in.B.Body)
	if err != nil {
		return in, false, err
	}

	mismatches, err := d.Diff(jsonA, jsonB)

	if err != nil {
		return in, false, err
	}

	if len(mismatches) > 0 {
		b, err := json.MarshalIndent(mismatches, "", " ")

		if err != nil {
			return in, false, err
		}

		in.Mismatch.Mismatches = string(b)
		in.Mismatch.IsOK = false
		in.Mismatch.Count = len(mismatches)
		in.Mismatch.Message = mismatches.String()

		topMismatchType, _ := t.mostCommonMismatchType(mismatches)
		if topMismatchType != "" {
			in.Mismatch.TopMismatchType = topMismatchType
		}

	} else {
		in.Mismatch.Mismatches = "[]"
		in.Mismatch.IsOK = true
	}

	return in, false, nil
}
