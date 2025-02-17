// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package main

import (
	"encoding/json"
	"fmt"
	"strings"
)

type KibanaRecord struct {
	Attributes struct {
		KibanaSavedObjectMeta struct {
			SearchSourceJSON string `json:"searchSourceJSON"`
		} `json:"kibanaSavedObjectMeta"`
	} `json:"attributes"`
}

// SearchSource represents the extracted query structure
type SearchSource struct {
	Query  map[string]interface{}   `json:"query"`
	Filter []map[string]interface{} `json:"filter"`
}

func extractFieldValues(query string, fieldValues map[string]map[string]bool) {
	parts := strings.Split(query, ":")
	if len(parts) == 2 {
		field := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])
		if fieldValues[field] == nil {
			fieldValues[field] = make(map[string]bool)
		}
		fieldValues[field][value] = true
	}
}

// Converts a map of values to a slice of keys
func getKeys(m map[string]bool) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

func guessValues(panel []byte) map[string]map[string]bool {

	fieldValues := make(map[string]map[string]bool)

	var record KibanaRecord
	if err := json.Unmarshal(panel, &record); err != nil {
		fmt.Println("Error parsing JSON:", err)
		return fieldValues
	}

	// Parse SearchSourceJSON
	var searchSource SearchSource
	if err := json.Unmarshal([]byte(record.Attributes.KibanaSavedObjectMeta.SearchSourceJSON), &searchSource); err != nil {
		return fieldValues
	}

	// Extract fields from queries
	if query, ok := searchSource.Query["query"].(string); ok {
		extractFieldValues(query, fieldValues)
	}

	// Extract fields from filters
	for _, filter := range searchSource.Filter {
		if meta, exists := filter["meta"].(map[string]interface{}); exists {
			if key, ok := meta["key"].(string); ok {
				if value, ok := meta["value"].(string); ok {
					if fieldValues[key] == nil {
						fieldValues[key] = make(map[string]bool)
					}
					fieldValues[key][value] = true
				}
			}
		}
	}

	return fieldValues
}
