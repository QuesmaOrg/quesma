// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

// This file contains integration tests for different ingest functionalities.
// This is a good place to add regression tests for ingest bugs.

package testcases

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/assert"
	"net/http"
	"testing"
)

type DefaultSchemaOverrideTestcase struct {
	IntegrationTestcaseBase
}

func NewDefaultSchemaOverrideTestcase() *DefaultSchemaOverrideTestcase {
	return &DefaultSchemaOverrideTestcase{
		IntegrationTestcaseBase: IntegrationTestcaseBase{
			ConfigTemplate: "quesma-default-schema-override.yml.template",
		},
	}
}

func (a *DefaultSchemaOverrideTestcase) SetupContainers(ctx context.Context) error {
	containers, err := setupAllContainersWithCh(ctx, a.ConfigTemplate)
	a.Containers = containers
	return err
}

func (a *DefaultSchemaOverrideTestcase) RunTests(ctx context.Context, t *testing.T) error {
	t.Run("test basic request", func(t *testing.T) { a.testBasicRequest(ctx, t) })

	return nil
}

func (a *DefaultSchemaOverrideTestcase) testBasicRequest(ctx context.Context, t *testing.T) {

	testCases := []struct {
		TestCaseName string `json:"name"`

		// ingest
		IndexName string `json:"index_name"`

		// value of the doc
		Message string `json:"message"`

		// query
		QueryIndex   string `json:"query_index"`
		Pattern      string `json:"pattern"`
		TotalResults int    `json:"total_results"`
	}{
		{
			TestCaseName: "1. plain index name",
			IndexName:    "foo",
			QueryIndex:   "foo",
			Message:      "This is first",
			Pattern:      "first",
			TotalResults: 1,
		},
		{
			TestCaseName: "2. plain index name with date",
			IndexName:    "foo.2023-10-01",
			QueryIndex:   "foo",
			Message:      "This is second",
			Pattern:      "second",
			TotalResults: 1,
		},
		{
			TestCaseName: "3. plain index name with date not matching ",
			IndexName:    "foo.2023-10-01",
			QueryIndex:   "foo",
			Message:      "This is third",
			Pattern:      "notmatching",
			TotalResults: 0,
		},
		{
			TestCaseName: "4. another index name with date",
			IndexName:    "anotherindex.2023-10-01",
			QueryIndex:   "anotherindex",
			Message:      "This is third",
			Pattern:      "third",
			TotalResults: 1,
		},
		{
			TestCaseName: "5. query all",
			IndexName:    "foo.2023-01",
			QueryIndex:   "foo,anotherindex",
			Message:      "This is fifth",
			Pattern:      "This",
			TotalResults: 5,
		},
	}

	for n, d := range testCases {

		data, err := json.Marshal(d)
		if err != nil {
			t.Fatalf("Failed to marshal test case %d: %s", n, err)
		}

		resp, bodyBytes := a.RequestToQuesma(ctx, t,
			"POST", fmt.Sprintf("/%s/_doc", d.IndexName), data)

		assert.Contains(t, string(bodyBytes), "created")
		assert.Equal(t, http.StatusOK, resp.StatusCode)
		assert.Equal(t, "Clickhouse", resp.Header.Get("X-Quesma-Source"))
		assert.Equal(t, "Elasticsearch", resp.Header.Get("X-Elastic-Product"))
	}

	for _, d := range testCases {
		t.Run(d.TestCaseName, func(t *testing.T) {
			// check field caps
			fmt.Println("Testing: ", d.QueryIndex, d.TestCaseName)
			q := `{
			"fields": [
		             "*"
            ]
		}`

			_, bodyBytes := a.RequestToQuesma(ctx, t, "POST", fmt.Sprintf("/%s/_field_caps", d.QueryIndex), []byte(q))
			assert.Contains(t, string(bodyBytes), `"message":{"text"`)

			// perform full-text search

			fullTextQuery := fmt.Sprintf(`{"query": {"match": {"message": "%s"}}}`, d.Pattern)

			_, bodyBytes = a.RequestToQuesma(ctx, t, "POST", fmt.Sprintf("/%s/_search", d.QueryIndex), []byte(fullTextQuery))

			fmt.Println(string(bodyBytes))

			type Total struct {
				Value    int    `json:"value"`
				Relation string `json:"relation"`
			}

			type Hits struct {
				Total Total `json:"total"`
			}

			type ElasticsearchResponse struct {
				Hits Hits `json:"hits"`
			}

			var esResponse ElasticsearchResponse
			if err := json.Unmarshal(bodyBytes, &esResponse); err != nil {
				t.Fatalf("Failed to unmarshal response body: %s", err)
			}

			if esResponse.Hits.Total.Value != d.TotalResults {
				t.Fatalf("Expected %d results, got %d for test case %s", d.TotalResults, esResponse.Hits.Total, d.TestCaseName)
			}
		})
	}

}
