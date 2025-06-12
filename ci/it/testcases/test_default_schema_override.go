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
		testCaseName string

		// ingest
		indexName string

		hasDefaultField bool

		// value of the doc
		message string

		// query
		queryIndex   string
		pattern      string
		totalResults int
	}{
		{
			testCaseName:    "1. plain index name",
			indexName:       "foo",
			hasDefaultField: true,
			queryIndex:      "foo",
			message:         "This is first",
			pattern:         "first",
			totalResults:    1,
		},
		{
			testCaseName:    "2. plain index name with date",
			indexName:       "foo.2023-10-01",
			hasDefaultField: true,
			queryIndex:      "foo",
			message:         "This is second",
			pattern:         "second",
			totalResults:    1,
		},
		{
			testCaseName:    "3. plain index name with date not matching ",
			indexName:       "foo.2023-10-01",
			hasDefaultField: true,
			queryIndex:      "foo",
			message:         "This is third",
			pattern:         "notmatching",
			totalResults:    0,
		},
		{
			testCaseName:    "4. another index name with date",
			indexName:       "anotherindex.2023-10-01",
			hasDefaultField: true,
			queryIndex:      "anotherindex",
			message:         "This is third",
			pattern:         "third",
			totalResults:    1,
		},
		{
			testCaseName:    "5. query all",
			indexName:       "foo.2023-01",
			hasDefaultField: true,
			queryIndex:      "foo,anotherindex",
			message:         "This is fifth",
			pattern:         "This",
			totalResults:    5,
		},
		{
			testCaseName:    "6. no message index",
			indexName:       "no-message-index",
			hasDefaultField: false,
			queryIndex:      "no-message-index",
			message:         "",
			pattern:         "",
			totalResults:    0,
		},
	}

	type Doc struct {
		IndexName string  `json:"index_name"`
		Message   *string `json:"message,omitempty"`
	}

	// ingest all test cases
	for n, d := range testCases {

		var doc Doc

		doc.IndexName = d.indexName
		if d.message != "" {
			doc.Message = &d.message
		} else {
			doc.Message = nil // explicitly set to nil if no message
		}

		data, err := json.Marshal(doc)
		if err != nil {
			t.Fatalf("Failed to marshal test case %d: %s", n, err)
		}

		resp, bodyBytes := a.RequestToQuesma(ctx, t,
			"POST", fmt.Sprintf("/%s/_doc", d.indexName), data)

		assert.Contains(t, string(bodyBytes), "created")
		assert.Equal(t, http.StatusOK, resp.StatusCode)
		assert.Equal(t, "Clickhouse", resp.Header.Get("X-Quesma-Source"))
		assert.Equal(t, "Elasticsearch", resp.Header.Get("X-Elastic-Product"))
	}

	// query all test cases

	for _, d := range testCases {
		t.Run(d.testCaseName, func(t *testing.T) {
			// check field caps
			fmt.Println("Testing: ", d.queryIndex, d.testCaseName)
			q := `{
			"fields": [
		             "*"
            ]
		}`

			_, bodyBytes := a.RequestToQuesma(ctx, t, "POST", fmt.Sprintf("/%s/_field_caps", d.queryIndex), []byte(q))

			defaulfFieldForNotConfiguredIndex := `default_field_for_not_configured_index`

			if d.hasDefaultField {
				assert.Contains(t, string(bodyBytes), defaulfFieldForNotConfiguredIndex, "Index %s should have a default_field_for_not_configured_index field", d.queryIndex)
			} else {
				assert.NotContains(t, string(bodyBytes), defaulfFieldForNotConfiguredIndex, "Index %s should not have a default_field_for_not_configured_index field", d.queryIndex)
			}

			if d.message == "" {
				assert.NotContains(t, string(bodyBytes), `"message":{"text"`, "Index %s should not have a message field", d.queryIndex)
				// don't check the rest of the fields if message is not present
				return
			} else {
				assert.Contains(t, string(bodyBytes), `"message":{"text"`, "Index %s should have a message field", d.queryIndex)
			}

			// perform full-text search

			fullTextQuery := fmt.Sprintf(`{"query": {"match": {"message": "%s"} },  "fields": ["message"],  "_source": false }`, d.pattern)

			_, bodyBytes = a.RequestToQuesma(ctx, t, "POST", fmt.Sprintf("/%s/_search", d.queryIndex), []byte(fullTextQuery))

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

			if esResponse.Hits.Total.Value != d.totalResults {
				t.Fatalf("Expected %d results, got %d for test case %s", d.totalResults, esResponse.Hits.Total, d.testCaseName)
			}
		})
	}

}
