// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package testcases

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/assert"
	"sort"
	"testing"
)

type CommonTableAndRegularTable struct {
	IntegrationTestcaseBase
}

func NewCommonTableAndRegularTable() *CommonTableAndRegularTable {
	return &CommonTableAndRegularTable{
		IntegrationTestcaseBase: IntegrationTestcaseBase{
			ConfigTemplate: "quesma-common-table-and-regular-table.yml.template",
		},
	}
}

func (a *CommonTableAndRegularTable) SetupContainers(ctx context.Context) error {
	containers, err := setupAllContainersWithCh(ctx, a.ConfigTemplate)
	a.Containers = containers
	return err
}

func (a *CommonTableAndRegularTable) RunTests(ctx context.Context, t *testing.T) error {

	t.Run("all mappings", func(t *testing.T) { a.mappingsAll(ctx, t) })

	return nil
}

func (a *CommonTableAndRegularTable) mappingsAll(ctx context.Context, t *testing.T) {

	fetchStarFieldCaps := func() map[string]any {

		resp, body := a.RequestToQuesma(ctx, t, "GET", "/*/_field_caps", nil)
		if resp.StatusCode != 200 {
			t.Fatalf("Failed to fetch mappings: %s", body)
		}

		var fieldCaps map[string]any
		if err := json.Unmarshal(body, &fieldCaps); err != nil {
			t.Fatalf("Failed to unmarshal mappings: %v", err)
		}

		return fieldCaps
	}

	fetchFieldCaps := func(index string) {

		resp, body := a.RequestToQuesma(ctx, t, "GET", fmt.Sprintf("/%s/_field_caps", index), nil)
		if resp.StatusCode != 200 {
			t.Fatalf("Failed to fetch mappings: %s", body)
		}

		var fieldCaps map[string]any
		if err := json.Unmarshal(body, &fieldCaps); err != nil {
			t.Fatalf("Failed to unmarshal mappings: %v", err)
		}
		if len(fieldCaps) == 0 {
			t.Fatalf("Expected field caps for index %s, got empty response", index)
		}
	}

	checkFieldCaps := func(fieldCaps map[string]any, expectedIndexes []string) {

		indicesAny, ok := fieldCaps["indices"].([]any)
		if !ok {
			t.Fatalf("Expected 'indices' to be a slice of strings, got: %T", fieldCaps["indices"])
		}

		indices := make([]string, len(indicesAny))
		for i, index := range indicesAny {
			indexStr, ok := index.(string)
			if !ok {
				t.Fatalf("Expected index to be a string, got: %T", index)
			}
			indices[i] = indexStr
		}

		assert.Equal(t, len(expectedIndexes), len(indices))

		sort.Strings(indices)
		sort.Strings(expectedIndexes)

		for i, index := range expectedIndexes {
			assert.Equal(t, index, indices[i], fmt.Sprintf("Index %s should exist in field caps", index))
		}

	}

	fetchStarMapping := func() map[string]any {

		resp, body := a.RequestToQuesma(ctx, t, "GET", "/*/_mapping", nil)
		if resp.StatusCode != 200 {
			t.Fatalf("Failed to fetch mappings: %s", body)
		}

		var mappings map[string]any
		if err := json.Unmarshal(body, &mappings); err != nil {
			t.Fatalf("Failed to unmarshal mappings: %v", err)
		}
		return mappings
	}

	fetchMappings := func(index string) {
		resp, body := a.RequestToQuesma(ctx, t, "GET", fmt.Sprintf("/%s/_mapping", index), nil)
		if resp.StatusCode != 200 {
			t.Fatalf("Failed to fetch mappings: %s", body)
		}

		var mappings map[string]any
		if err := json.Unmarshal(body, &mappings); err != nil {
			t.Fatalf("Failed to unmarshal mappings: %v", err)
		}
		if len(mappings) == 0 {
			t.Fatalf("Expected mappings for index %s, got empty response", index)
		}
	}

	checkMappings := func(mappings map[string]any, expectedIndexes []string) {

		assert.Equal(t, len(expectedIndexes), len(mappings))

		for _, index := range expectedIndexes {
			_, exists := mappings[index]
			assert.True(t, exists, fmt.Sprintf("Index %s should exist in mappings", index))
		}
	}

	expectedIndexes := []string{"first", "second", "third"} // explicitly defined indexes in the config

	mappings := fetchStarMapping()
	checkMappings(mappings, expectedIndexes)

	fieldCaps := fetchStarFieldCaps()
	checkFieldCaps(fieldCaps, expectedIndexes)

	for _, index := range expectedIndexes {
		fetchFieldCaps(index)
		fetchMappings(index)
	}

	// add a new index (common table)

	a.RequestToQuesma(ctx, t, "POST", "/go_to_common_table/_doc", []byte(`{"name": "Przemyslaw", "age": 31337}`))

	expectedIndexes = append(expectedIndexes, "go_to_common_table")
	mappings = fetchStarMapping()
	checkMappings(mappings, expectedIndexes)

	fieldCaps = fetchStarFieldCaps()
	checkFieldCaps(fieldCaps, expectedIndexes)

	for _, index := range expectedIndexes {
		fetchFieldCaps(index)
		fetchMappings(index)
	}
}
