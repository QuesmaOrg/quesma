// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package collector

import (
	"crypto/sha1"
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

	mismatches := jsondiff.Mismatches{}

	d, err := jsondiff.NewElasticResponseJSONDiff()
	if err != nil {
		return in, false, err
	}

	if in.A.Error != "" || in.B.Error != "" {

		in.Mismatch.IsOK = false
		in.Mismatch.Message = "one of the responses has an error"

		if in.A.Error != "" {
			in.Mismatch.Message = in.Mismatch.Message + fmt.Sprintf("\nA response has an error: %s", in.A.Error)

			mismatches = append(mismatches, jsondiff.JSONMismatch{
				Type:     "error",
				Message:  fmt.Sprintf("\nA response has an error: %s", in.A.Error),
				Path:     "n/a",
				Expected: "n/a",
				Actual:   "n/a",
			})

		}

		if in.B.Error != "" {

			mismatches = append(mismatches, jsondiff.JSONMismatch{
				Type:     "error",
				Message:  fmt.Sprintf("\nB response has an error: %s", in.B.Error),
				Path:     "n/a",
				Expected: "n/a",
				Actual:   "n/a",
			})
		}

	} else {

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

		mismatches, err = d.Diff(jsonA, jsonB)
		if err != nil {
			return in, false, err
		}

	}

	if len(mismatches) > 0 {

		b, err := json.Marshal(mismatches)

		if err != nil {
			return in, false, fmt.Errorf("failed to marshal mismatches: %w", err)
		}

		in.Mismatch.Mismatches = string(b)
		hash := sha1.Sum(b)
		in.Mismatch.SHA1 = fmt.Sprintf("%x", hash)
		in.Mismatch.IsOK = false
		in.Mismatch.Count = len(mismatches)

		topMismatchType, _ := t.mostCommonMismatchType(mismatches)
		if topMismatchType != "" {
			in.Mismatch.TopMismatchType = topMismatchType
		}

		size := len(mismatches)

		// if there are too many mismatches, we only show the first 20
		// this is to avoid overwhelming the user with too much information
		const mismatchesSize = 20

		if len(mismatches) > mismatchesSize {
			mismatches = mismatches[:mismatchesSize]
			mismatches = append(mismatches, jsondiff.JSONMismatch{
				Type:    "info",
				Message: fmt.Sprintf("only first %d mismatches, total %d", mismatchesSize, size),
			})
		}

		in.Mismatch.Message = mismatches.String()

	} else {
		in.Mismatch.Mismatches = "[]"
		in.Mismatch.IsOK = true
	}

	return in, false, nil
}
