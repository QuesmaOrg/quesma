// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package testcases

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/assert"
	"net/http"
	"testing"
)

type ReadingClickHouseTablesIntegrationTestcase struct {
	IntegrationTestcaseBase
}

func NewReadingClickHouseTablesIntegrationTestcase() *ReadingClickHouseTablesIntegrationTestcase {
	return &ReadingClickHouseTablesIntegrationTestcase{
		IntegrationTestcaseBase: IntegrationTestcaseBase{
			ConfigTemplate: "quesma-now-reads-clickhouse-tables.yml.template",
		},
	}
}

func (a *ReadingClickHouseTablesIntegrationTestcase) SetupContainers(ctx context.Context) error {
	containers, err := setupAllContainersWithCh(ctx, a.ConfigTemplate)
	a.Containers = containers
	return err
}

func (a *ReadingClickHouseTablesIntegrationTestcase) RunTests(ctx context.Context, t *testing.T) error {
	t.Run("test basic request", func(t *testing.T) { a.testBasicRequest(ctx, t) })
	t.Run("test random thing", func(t *testing.T) { a.testRandomThing(ctx, t) })
	t.Run("test wildcard goes to elastic", func(t *testing.T) { a.testWildcardGoesToElastic(ctx, t) })
	t.Run("test ingest is disabled", func(t *testing.T) { a.testIngestIsDisabled(ctx, t) })
	return nil
}

func (a *ReadingClickHouseTablesIntegrationTestcase) testBasicRequest(ctx context.Context, t *testing.T) {
	resp, _ := a.RequestToQuesma(ctx, t, "GET", "/", nil)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func (a *ReadingClickHouseTablesIntegrationTestcase) testRandomThing(ctx context.Context, t *testing.T) {
	createTableQuery := "CREATE TABLE IF NOT EXISTS test_table (id UInt32, name String) ENGINE = Memory"
	if _, err := a.ExecuteClickHouseStatement(ctx, createTableQuery); err != nil {
		t.Fatalf("Failed to create table: %s", err)
	}

	insertRowsQuery := "INSERT INTO test_table (id, name) VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')"
	if _, err := a.ExecuteClickHouseStatement(ctx, insertRowsQuery); err != nil {
		t.Fatalf("Failed to insert rows: %s", err)
	}

	// This returns 500 Internal Server Error, but will be tackled in separate PR.
	// (The table has not yet been discovered by Quesma )
	// ERR quesma/quesma/quesma.go:198 > quesma request failed: Q2002: Missing table. Table: test_table: can't load test_table table opaque_id= path=/test_table/_search reason="Missing table." request_id=01926654-b214-7e1d-944a-a7545cd7d419
	resp, _ := a.RequestToQuesma(ctx, t, "GET", "/test_table/_search", []byte(`{"query": {"match_all": {}}}`))
	assert.Equal(t, "Clickhouse", resp.Header.Get("X-Quesma-Source"))
}

func (a *ReadingClickHouseTablesIntegrationTestcase) testWildcardGoesToElastic(ctx context.Context, t *testing.T) {
	// Given an index in Elasticsearch which falls under `*` in the configuration
	var err error
	if _, err = a.RequestToElasticsearch(ctx, "PUT", "/extra_index", nil); err != nil {
		t.Fatalf("Failed to create index: %s", err)
	}
	if _, err = a.RequestToElasticsearch(ctx, "POST", "/extra_index/_doc/1", []byte(`{"name": "Alice"}`)); err != nil {
		t.Fatalf("Failed to insert document: %s", err)
	}
	if _, err = a.RequestToElasticsearch(ctx, "POST", "/extra_index/_refresh", nil); err != nil {
		t.Fatalf("Failed to refresh index: %s", err)
	}
	// When Quesma searches for that document
	resp, bodyBytes := a.RequestToQuesma(ctx, t, "POST", "/extra_index/_search", []byte(`{"query": {"match_all": {}}}`))
	var jsonResponse map[string]interface{}
	if err := json.Unmarshal(bodyBytes, &jsonResponse); err != nil {
		t.Fatalf("Failed to unmarshal response body: %s", err)
	}
	hits, _ := jsonResponse["hits"].(map[string]interface{})
	// We should get proper search result from Elasticsearch
	hit := hits["total"]
	hitValue := hit.(map[string]interface{})["value"]
	assert.Equal(t, float64(1), hitValue)
	assert.Contains(t, string(bodyBytes), "Alice")
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "Elasticsearch", resp.Header.Get("X-Quesma-Source"))
	assert.Equal(t, "Elasticsearch", resp.Header.Get("X-Elastic-Product"))
}

func (a *ReadingClickHouseTablesIntegrationTestcase) testIngestIsDisabled(ctx context.Context, t *testing.T) {
	// There is no ingest pipeline, so Quesma should reject all ingest requests
	for _, tt := range []string{"test_table", "extra_index"} {
		t.Run(tt, func(t *testing.T) {
			resp, bodyBytes := a.RequestToQuesma(ctx, t, "POST", fmt.Sprintf("/%s/_doc", tt), []byte(`{"name": "Piotr", "age": 11111}`))
			assert.Contains(t, string(bodyBytes), "index_closed_exception")
			assert.Equal(t, http.StatusOK, resp.StatusCode)
			assert.Equal(t, "Clickhouse", resp.Header.Get("X-Quesma-Source"))
			assert.Equal(t, "Elasticsearch", resp.Header.Get("X-Elastic-Product"))
		})
	}
}
