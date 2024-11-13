// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package jsondiff

import "fmt"

// NewElasticResponseJSONDiff creates a JSONDiff instance that is tailored to compare Elasticsearch response JSONs.
func NewElasticResponseJSONDiff() (*JSONDiff, error) {

	var ignorePaths []string

	// quesma specific fields that we want to ignore
	ignorePaths = append(ignorePaths, ".*Quesma_key_.*", ".*__quesma_total_count", ".*\\.__quesma_originalKey")

	// well known fields that we want to ignore
	ignorePaths = append(ignorePaths, "^id$", "^took$", ".*\\._id", "^_shards.*", ".*\\._score", ".*\\._source", ".*\\._version$")

	// elastic has some fields that are suffixed with ".keyword" that we want to ignore
	ignorePaths = append(ignorePaths, ".*\\.keyword$")

	// ignore some fields that are related to location, just for now (we  want to compare them in the future)
	ignorePaths = append(ignorePaths, ".*Location$", ".*\\.lat$", ".*\\.lon$")

	d, err := NewJSONDiff(ignorePaths...)

	if err != nil {
		return nil, fmt.Errorf("could not create JSONDiff: %v", err)
	}

	// here we enable comparing the buckets by the key field
	// this will show higher level differences in the JSON (e.g. sorting differences)

	anyToKey := func(element any) string {

		switch val := element.(type) {

		case float64:

			if val == float64(int(val)) {
				return fmt.Sprintf("%d", int(val))
			}
			return fmt.Sprintf("%f", val)

		case string:
			return val
		default:
			return fmt.Sprintf("%v", val)
		}
	}

	err = d.AddKeyExtractor("buckets", func(element any) (string, error) {
		switch v := element.(type) {
		case map[string]interface{}:
			if val, ok := v["key"]; ok {
				return anyToKey(val), nil
			}
		}
		return "", fmt.Errorf("could not extract key from element: %v", element)
	})

	if err != nil {
		return nil, fmt.Errorf("could not add key extractor: %v", err)
	}

	return d, nil
}
