package mux

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

type JSON map[string]interface{}

func (j JSON) Bytes() ([]byte, error) {
	return json.Marshal(j)
}

func (j JSON) Remarshal(v interface{}) error {
	b, err := json.Marshal(j)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, v)
}

func (j JSON) ShortString() string {

	var asString string
	asBytes, err := json.Marshal(j)

	if err != nil {
		asString = fmt.Sprintf("Error marshalling JSON: %v, json: %v", err, j)
	} else {
		asString = string(asBytes)
	}

	if len(asString) < 70 {
		return asString
	}
	return asString[:70]

}

type NDJSON []JSON

// There we can add methods to iterate over NDJSON

type Unknown []error

type RequestBody interface {
	isParsedRequestBody() // this is a marker method
}


func (j JSON) isParsedRequestBody()    {}
func (n NDJSON) isParsedRequestBody()  {}
func (e Unknown) isParsedRequestBody() {}

func ParseRequestBody(ctx context.Context, req *Request) RequestBody {

	var errors []error

	switch {
	// json
	case len(req.Body) > 1 && req.Body[0] == '{':
		parsedBody := make(JSON)
		if err := json.Unmarshal([]byte(req.Body), &parsedBody); err != nil {
			errors = append(errors, fmt.Errorf("error while parsing JSON %s", err))
		} else {
			return parsedBody
		}

	// ndjson
	case len(req.Body) > 1 && req.Body[0] == '{':

		var ndjson NDJSON

		var err error
		for _, line := range strings.Split(req.Body, "\n") {

			parsedLine := make(JSON)

			err = json.Unmarshal([]byte(line), &parsedLine)
			if err != nil {
				errors = append(errors, fmt.Errorf("error while parsing NDJSON %s", err))
				break
			}

			ndjson = append(ndjson, parsedLine)
		}
		if err == nil {
			return ndjson
		}

	// if nothing else, it's unknown
	default:
		return Unknown(errors)
	}
	return Unknown(errors)
}
