// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package types

import (
	"fmt"
	"quesma/end_user_errors"
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

type RequestBody interface {
	//isParsedRequestBody() // this is a marker method
}

//func (j JSON) isParsedRequestBody()     {}
//func (n NDJSON) isParsedRequestBody()   {}
//func (u *Unknown) isParsedRequestBody() {}

func ParseRequestBody(body string) RequestBody {

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

func ExpectJSON(body RequestBody) (JSON, error) {

	switch b := body.(type) {
	case JSON:
		return b, nil
	default:
		return nil, end_user_errors.ErrExpectedJSON.New(fmt.Errorf("expecting JSON . Got: %T", body))
	}
}

func ExpectNDJSON(body RequestBody) (NDJSON, error) {

	switch b := body.(type) {
	case NDJSON:
		return b, nil
	default:
		return nil, end_user_errors.ErrExpectedNDJSON.New(fmt.Errorf("expecting NDJSON . Got: %T", body))
	}
}
