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

func (t *diffTransformer) process(in EnrichedResults) (out EnrichedResults, drop bool, err error) {

	d := jsondiff.NewElasticResponseJSONDiff()

	jsonA, err := types.ParseJSON(in.A.Body)
	if err != nil {
		return in, false, err
	}

	jsonB, err := types.ParseJSON(in.B.Body)
	if err != nil {
		return in, false, err
	}

	problems, err := d.Diff(jsonA, jsonB)

	if err != nil {
		return in, false, err
	}

	if len(problems) > 0 {

		b, err := json.MarshalIndent(problems, "", " ")

		if err != nil {
			return in, false, err

		}

		in.Mismatch.Mismatches = string(b)
		in.Mismatch.IsMismatch = true

	} else {
		in.Mismatch.Mismatches = "[]"
		in.Mismatch.IsMismatch = false
	}

	return in, false, nil
}
