package mux

import (
	"encoding/json"
	"fmt"
	"strings"
)

//
// These types are generic.
// TODO move them to a separate package `types`
//

type JSON map[string]interface{}

func ParseJSON(body string) (JSON, error) {

	var res JSON
	err := json.Unmarshal([]byte(body), &res)

	return res, err
}

func MustJSON(s string) JSON {

	res, err := ParseJSON(s)
	if err != nil {
		panic(fmt.Sprintf("Failed to parse JSON: %v", err))
	}

	return res
}

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

func ParseNDJSON(body string) (NDJSON, error) {
	var ndjson NDJSON

	var err error
	var errors []error
	for x, line := range strings.Split(body, "\n") {

		if line == "" {
			continue
		}

		parsedLine := make(JSON)

		err = json.Unmarshal([]byte(line), &parsedLine)
		if err != nil {
			errors = append(errors, fmt.Errorf("error while parsing line %d: %s: %s", x, line, err))
			break
		}

		ndjson = append(ndjson, parsedLine)
	}

	if len(errors) > 0 {
		err = fmt.Errorf("errors while parsing NDJSON: %v", errors)
	}

	return ndjson, err
}

type DocumentTarget struct {
	Index *string `json:"_index"`
	Id    *string `json:"_id"` // document's target id in Elasticsearch, we ignore it when writing to Clickhouse.
}

type BulkOperation map[string]DocumentTarget

func (op BulkOperation) GetIndex() string {
	for _, target := range op { // this map contains only 1 element though
		if target.Index != nil {
			return *target.Index
		}
	}

	return ""
}

func (op BulkOperation) GetOperation() string {
	for operation := range op {
		return operation
	}
	return ""
}

func (n NDJSON) BulkForEach(f func(operation BulkOperation, doc JSON)) error {

	for i := 0; i+1 < len(n); i += 2 {
		operation := n[i]  // {"create":{"_index":"kibana_sample_data_flights", "_id": 1}}
		document := n[i+1] // {"FlightNum":"9HY9SWR","DestCountry":"AU","OriginWeather":"Sunny","OriginCityName":"Frankfurt am Main" }

		var operationParsed BulkOperation // operationName (create, index, update, delete) -> DocumentTarget

		err := operation.Remarshal(&operationParsed)
		if err != nil {
			return err
		}

		f(operationParsed, document)
	}

	return nil

}

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
