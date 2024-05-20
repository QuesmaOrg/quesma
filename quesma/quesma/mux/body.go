package mux

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

type JSON map[string]interface{}
type NDJSON []JSON
type Unknown []error

type RequestBody interface {
	isParsedRequestBody() // this is a marker method
}

func (j JSON) isParsedRequestBody()    {}
func (n NDJSON) isParsedRequestBody()  {}
func (e Unknown) isParsedRequestBody() {}

func ParseRequestBody(ctx context.Context, req *Request) {

	var errors []error

	if len(req.Body) > 1 && req.Body[0] == '{' {
		parsedBody := make(JSON)
		if err := json.Unmarshal([]byte(req.Body), &parsedBody); err != nil {
			errors = append(errors, fmt.Errorf("error while parsing JSON %s", err))
		} else {
			req.ParsedBody = JSON(parsedBody)
			return
		}
	}

	if len(req.Body) > 1 && req.Body[0] == '{' {

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
			req.ParsedBody = ndjson
			return
		}
	}

	req.ParsedBody = Unknown(errors)

}
