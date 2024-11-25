// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package testcases

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/assert"
	"io"
	"net/http"
	"testing"
)

type ABTestcase struct {
	IntegrationTestcaseBase
}

func NewABTestcase() *ABTestcase {
	return &ABTestcase{
		IntegrationTestcaseBase: IntegrationTestcaseBase{
			ConfigTemplate: "quesma-ab.yml.template",
		},
	}
}

func (a *ABTestcase) SetupContainers(ctx context.Context) error {
	containers, err := setupAllContainersWithCh(ctx, a.ConfigTemplate)
	if err != nil {
		return err
	}
	a.Containers = containers
	return nil
}

func (a *ABTestcase) RunTests(ctx context.Context, t *testing.T) error {
	t.Run("test basic request", func(t *testing.T) { a.testBasicRequest(ctx, t) })
	t.Run("test ingest to both connectors", func(t *testing.T) { a.testIngestToClickHouseWorks(ctx, t) })
	t.Run("test A/B queries", func(t *testing.T) { a.testQueries(ctx, t) })
	return nil
}

func (a *ABTestcase) testBasicRequest(ctx context.Context, t *testing.T) {
	resp, _ := a.RequestToQuesma(ctx, t, "GET", "/", nil)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func (a *ABTestcase) testIngestToClickHouseWorks(ctx context.Context, t *testing.T) {

	for i := 0; i < 100; i++ {

		m := make(map[string]interface{})
		m["foo"] = fmt.Sprintf("bar-%d", i)
		m["count"] = i

		doc, err := json.Marshal(m)
		if err != nil {
			t.Fatalf("Failed to marshal json: %s", err)
		}

		resp, body := a.RequestToQuesma(ctx, t, "POST", "/test_index/_doc", doc)

		if resp.StatusCode != http.StatusOK {

			fmt.Println("XXX", string(body))

		}
		resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode)
	}

	chQuery := "SELECT count(*) FROM test_index"
	rows, err := a.ExecuteClickHouseQuery(ctx, chQuery)
	if err != nil {
		t.Fatalf("Failed to execute query: %s", err)
	}

	var count int
	for rows.Next() {
		if err := rows.Scan(&count); err != nil {
			t.Fatalf("Failed to scan row: %s", err)
		}
	}

	if rows.Err() != nil {
		t.Fatalf("Failed to scan row: %s", rows.Err())
	}

	assert.Equal(t, 100, count)

	_, _ = a.RequestToElasticsearch(ctx, "GET", "/test_index/_refresh", nil)

	// Also make sure no such index got created in Elasticsearch
	resp, err := a.RequestToElasticsearch(ctx, "GET", "/test_index/_count", nil)
	if err != nil {
		t.Fatalf("Failed to make GET request: %s", err)
	}
	defer resp.Body.Close()
	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Failed to read response body: %s", err)
	}

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	elasticResult := make(map[string]interface{})
	if err := json.Unmarshal(bodyBytes, &elasticResult); err != nil {
		t.Fatalf("Failed to unmarshal response: %s", err)
	}

	assert.Equal(t, 100, int(elasticResult["count"].(float64)))

}

var queries []string = []string{
	`{}`,
}

func (a *ABTestcase) testQueries(ctx context.Context, t *testing.T) {

	for _, query := range queries {
		resp, _ := a.RequestToQuesma(ctx, t, "POST", "/test_index/_search", []byte(query))
		assert.Equal(t, http.StatusOK, resp.StatusCode)
	}

	chQuery := "SELECT count(*) FROM ab_testing_logs where  response_mismatch_is_ok = false"
	rows, err := a.ExecuteClickHouseQuery(ctx, chQuery)
	if err != nil {
		t.Fatalf("Failed to execute query: %s", err)
	}

	var count int
	for rows.Next() {
		if err := rows.Scan(&count); err != nil {
			t.Fatalf("Failed to scan row: %s", err)
		}
	}

	if rows.Err() != nil {
		t.Fatalf("Failed to scan row: %s", rows.Err())
	}

	assert.Equal(t, 0, count, "response_mismatch_is_ok should be false for all queries")

}
