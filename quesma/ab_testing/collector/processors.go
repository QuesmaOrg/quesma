// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package collector

import (
	"encoding/json"
	"quesma/quesma/types"
)

// unifySyncAsyncResponse is a processor that processes that removes async "wrapper" from the response
type unifySyncAsyncResponse struct {
}

func (t *unifySyncAsyncResponse) process(in EnrichedResults) (out EnrichedResults, drop bool, err error) {

	deAsync := func(elasticResponse string) (string, error) {

		asJson, err := types.ParseJSON(elasticResponse)

		if err != nil {
			return "", err
		}

		if res, ok := asJson["response"]; ok {
			b, err := json.Marshal(res)

			if err != nil {
				return "", err
			}

			return string(b), nil
		}

		return elasticResponse, nil
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
