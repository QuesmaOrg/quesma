// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package collector

import (
	"encoding/json"
	"fmt"
	"quesma/jsondiff"
	"quesma/quesma/types"
)

type diffTransformer struct {
}

func (t *diffTransformer) name() string {
	return "diffTransformer"
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
		in.Mismatch.IsOK = false
		in.Mismatch.Message = fmt.Sprintf("failed to parse A response: %v", err)
		err = fmt.Errorf("failed to parse A response: %w", err)
		in.Errors = append(in.Errors, err.Error())
		return in, false, nil
	}

	jsonB, err := types.ParseJSON(in.B.Body)
	if err != nil {
		in.Mismatch.IsOK = false
		in.Mismatch.Message = fmt.Sprintf("failed to parse B response: %v", err)
		err = fmt.Errorf("failed to parse B response: %w", err)
		in.Errors = append(in.Errors, err.Error())
		return in, false, nil
	}

	mismatches, err := d.Diff(jsonA, jsonB)

	if err != nil {
		return in, false, err
	}

	if len(mismatches) > 0 {

		in.Mismatch.IsOK = false
		in.Mismatch.Count = len(mismatches)

		topMismatchType, _ := t.mostCommonMismatchType(mismatches)
		if topMismatchType != "" {
			in.Mismatch.TopMismatchType = topMismatchType
		}

		// if there are too many mismatches, we only show the first 20
		// this is to avoid overwhelming the user with too much information
		const mismatchesSize = 20

		if len(mismatches) > mismatchesSize {
			mismatches = mismatches[:mismatchesSize]
		}

		b, err := json.MarshalIndent(mismatches, "", " ")

		if err != nil {
			return in, false, fmt.Errorf("failed to marshal mismatches: %w", err)
		}
		in.Mismatch.Mismatches = string(b)
		in.Mismatch.Message = mismatches.String()

	} else {
		in.Mismatch.Mismatches = "[]"
		in.Mismatch.IsOK = true
	}

	return in, false, nil
}
