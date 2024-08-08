// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package jsondiff

import "fmt"

// NewElasticResponseJSONDiff creates a JSONDiff instance that is tailored to compare Elasticsearch response JSONs.
func NewElasticResponseJSONDiff() (*JSONDiff, error) {
	d, err := NewJSONDiff("^id$", ".*Quesma_key_.*")

	if err != nil {
		return nil, fmt.Errorf("could not create JSONDiff: %v", err)
	}

	// here we enable comparing the buckets by the key field
	// this will show higher level differences in the JSON (e.g. sorting differences)
	err = d.AddKeyExtractor("buckets", func(element any) (string, error) {
		switch v := element.(type) {
		case map[string]interface{}:
			if val, ok := v["key"]; ok {
				return fmt.Sprintf("%v", val), nil
			}
		}
		return "", fmt.Errorf("could not extract key from element: %v", element)
	})

	if err != nil {
		return nil, fmt.Errorf("could not add key extractor: %v", err)
	}

	return d, nil
}
