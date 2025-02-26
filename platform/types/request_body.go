// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package types

import (
	"fmt"
	"github.com/QuesmaOrg/quesma/platform/end_user_errors"
	quesma_api "github.com/QuesmaOrg/quesma/platform/v2/core"
	"strings"
)

// There we can add methods to iterate over NDJSON

type Unknown struct {
	Body             string
	JSONParseError   error
	NDJSONParseError error
}

func (u *Unknown) String() string {

	return fmt.Sprintf("Unknown{Body: %s, JSONParseError: %v, NDJSONParseError: %v}", u.Body, u.JSONParseError, u.NDJSONParseError)

}

func (j JSON) IsParsedRequestBody()     {}
func (n NDJSON) IsParsedRequestBody()   {}
func (u *Unknown) IsParsedRequestBody() {}

func ParseRequestBody(body string) quesma_api.RequestBody {

	unknow := &Unknown{}
	unknow.Body = body

	body = strings.TrimSpace(body)

	// json
	if len(body) > 1 && body[0] == '{' {
		parsedBody, err := ParseJSON(body)
		if err != nil {
			unknow.JSONParseError = err
		} else {
			return parsedBody
		}
	}

	// ndjson
	if len(body) > 1 && body[0] == '{' {

		parsedBody, err := ParseNDJSON(body)
		if err != nil {
			unknow.NDJSONParseError = err
		} else {
			return parsedBody
		}

		// if nothing else, it's unknown
	}

	return unknow
}

func ExpectJSON(body quesma_api.RequestBody) (JSON, error) {

	switch b := body.(type) {
	case JSON:
		return b, nil
	default:
		return nil, end_user_errors.ErrExpectedJSON.New(fmt.Errorf("expecting JSON . Got: %T", body))
	}
}

func ExpectNDJSON(body quesma_api.RequestBody) (NDJSON, error) {

	switch b := body.(type) {
	case JSON:
		return NDJSON{b}, nil
	case NDJSON:
		return b, nil
	default:
		return nil, end_user_errors.ErrExpectedNDJSON.New(fmt.Errorf("expecting NDJSON . Got: %T", body))
	}
}
