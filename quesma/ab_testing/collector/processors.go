// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package collector

import (
	"encoding/json"
	"quesma/quesma/types"
)

// deAsyncResponse is a processor that processes that removes async "wrapper" from the response
type deAsyncResponse struct {
}

func (t *deAsyncResponse) process(in EnrichedResults) (out EnrichedResults, drop bool, err error) {

	deAsync := func(elasticResponse string) (string, error) {

		asJson, err := types.ParseJSON(elasticResponse)

		if asJson == nil {
			return "", err
		}

		if asJson["response"] == nil {
			return elasticResponse, nil
		}

		res := asJson["response"]

		b, err := json.Marshal(res)

		if err != nil {
			return "", nil
		}

		return string(b), nil
	}

	respA, err := deAsync(in.A.Body)
	if err != nil {
		return in, false, err
	}

	respB, err := deAsync(in.B.Body)
	if err != nil {
		return in, false, err
	}

	in.A.Body = respA
	in.B.Body = respB

	return in, false, nil
}
