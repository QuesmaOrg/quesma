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

type IndexNameRewriteTestcase struct {
	IntegrationTestcaseBase
}

func NewIndexNameRewriteTestcase() *IndexNameRewriteTestcase {
	return &IndexNameRewriteTestcase{
		IntegrationTestcaseBase: IntegrationTestcaseBase{
			ConfigTemplate: "quesma-index-name-rewrite.yml.template",
		},
	}
}

func (a *IndexNameRewriteTestcase) SetupContainers(ctx context.Context) error {
	containers, err := setupAllContainersWithCh(ctx, a.ConfigTemplate)
	a.Containers = containers
	return err
}

func (a *IndexNameRewriteTestcase) RunTests(ctx context.Context, t *testing.T) error {
	t.Run("test basic request", func(t *testing.T) { a.testBasicRequest(ctx, t) })

	return nil
}

func (a *IndexNameRewriteTestcase) testBasicRequest(ctx context.Context, t *testing.T) {

	testCases := []struct {
		TestCaseName      string `json:"name"`
		IndexName         string `json:"index_name"`
		ExpectedIndexName string `json:"expected_index_name"`
	}{
		{
			TestCaseName:      "1. plain index name",
			IndexName:         "foo",
			ExpectedIndexName: "foo",
		},
		{
			TestCaseName:      "2. index name with date",
			IndexName:         "foo.2001-01-01",
			ExpectedIndexName: "foo",
		},
		{
			TestCaseName:      "3. index name  and month",
			IndexName:         "foo.2001-01",
			ExpectedIndexName: "foo",
		},
		{
			TestCaseName:      "3. index name with date and dashes",
			IndexName:         "foo-2001.01",
			ExpectedIndexName: "foo",
		},
		{
			TestCaseName:      "4. index name with date and dashes",
			IndexName:         "foo-2001.01.01",
			ExpectedIndexName: "foo",
		},
		{
			TestCaseName:      "5. index name not matching",
			IndexName:         "foo-not-matching",
			ExpectedIndexName: "foo-not-matching",
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

	rows, err := a.ExecuteClickHouseQuery(ctx, "select name, __quesma_index_name, expected_index_name from quesma_common_table where expected_index_name <> __quesma_index_name")

	defer rows.Close()
	if err != nil {
		t.Fatalf("Failed to execute ClickHouse query: %s", err)
	}

	if rows.Next() {
		var name *string
		var expectedIndexName *string
		var actualIndexName *string

		if err := rows.Scan(&name, &actualIndexName, &expectedIndexName); err != nil {
			t.Fatalf("Failed to scan row: %s", err)
		}
		t.Fatalf("Expected index name does not match actual index. Test case: %s, actual index name: %s, expected index name: %s", *name, *actualIndexName, *expectedIndexName)
	}

}
