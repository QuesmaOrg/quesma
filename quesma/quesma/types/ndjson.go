// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package types

import (
	"fmt"
	"github.com/goccy/go-json"
	"strings"
)

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

func (n NDJSON) BulkForEach(f func(entryNumber int, operationParsed BulkOperation, operation JSON, doc JSON) error) error {

	for i := 0; i+1 < len(n); i += 2 {
		operation := n[i]  // {"create":{"_index":"kibana_sample_data_flights", "_id": 1}}
		document := n[i+1] // {"FlightNum":"9HY9SWR","DestCountry":"AU","OriginWeather":"Sunny","OriginCityName":"Frankfurt am Main" }

		var operationParsed BulkOperation // operationName (create, index, update, delete) -> DocumentTarget

		operationParsed = make(map[string]DocumentTarget)

		for k, v := range operation {
			d := DocumentTarget{}
			if op, ok := v.(map[string]interface{}); ok {
				if index, ok := op["_index"].(string); ok {
					d.Index = &index
				}

				if id, ok := op["_id"].(string); ok {
					d.Id = &id
				}
			}

			operationParsed[k] = d
		}

		err := f(i/2, operationParsed, operation, document)
		if err != nil {
			return err
		}
	}
	return nil
}
