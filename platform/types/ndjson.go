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

// BulkForEach iterates over the NDJSON lines and calls the supplied function for each **ENTRY**.
//
// Example bulk payload looks like following:
// {"create":{"_index":"kibana_sample_data_flights", "_id": 1}}
// {"FlightNum":"9HY9SWR","DestCountry":"AU","OriginWeather":"Sunny","OriginCityName":"Frankfurt am Main" }
// {"delete":{"_index":"my_index","_id":"1"}}
//
// ENTRY is usually 2 lines: action/metadata and the actual document. However, it is possible to have only action/metadata line (for DELETE operation).
// In that case the callback function will be invoked with an empty JSON document.
// Ref: https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html#docs-bulk-api-desc
func (n NDJSON) BulkForEach(f func(entryNumber int, operationParsed BulkOperation, operation JSON, doc JSON) error) error {
	for lineNum, entryNum := 0, 0; lineNum < len(n); lineNum++ {
		actionAndMetadata := n[lineNum]

		actionAndMetadataParsed := make(BulkOperation)
		for opType, opDetails := range actionAndMetadata {
			if detailsMap, ok := opDetails.(map[string]interface{}); ok {
				docTarget := DocumentTarget{}

				if index, ok := detailsMap["_index"].(string); ok {
					docTarget.Index = &index
				}
				if id, ok := detailsMap["_id"].(string); ok {
					docTarget.Id = &id
				}

				actionAndMetadataParsed[opType] = docTarget
			} else {
				return fmt.Errorf("invalid metadata format for operation at index %d: %v", lineNum, actionAndMetadata)
			}
		}

		if operationRequiresDocument(actionAndMetadata) {
			if lineNum+1 >= len(n) {
				return fmt.Errorf("missing document for metadata at index %d", lineNum)
			}
			optionalDocumentSource := n[lineNum+1]
			err := f(entryNum, actionAndMetadataParsed, actionAndMetadata, optionalDocumentSource)
			entryNum++
			if err != nil {
				return err
			}
			lineNum++ // Skip the document line
		} else { // Call the callback without a document
			err := f(entryNum, actionAndMetadataParsed, actionAndMetadata, JSON{})
			entryNum++
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func operationRequiresDocument(metadata JSON) bool {
	for opType := range metadata {
		if opType == "delete" {
			return false
		}
	}
	return true
}
