package collector

import (
	"encoding/json"
	"quesma/quesma/types"
)

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
