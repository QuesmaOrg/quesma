package types

import (
	"fmt"
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
	isParsedRequestBody() // this is a marker method
}

func (j JSON) isParsedRequestBody()     {}
func (n NDJSON) isParsedRequestBody()   {}
func (u *Unknown) isParsedRequestBody() {}

func ParseRequestBody(body string) RequestBody {

	unknow := &Unknown{}
	unknow.Body = body

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
