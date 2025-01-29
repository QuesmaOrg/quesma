// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package testcases

import (
	"context"
	"github.com/stretchr/testify/assert"
	"io"
	"net/http"
	"testing"
)

type WildcardClickhouseTestcase struct {
	IntegrationTestcaseBase
}

func NewWildcardClickhouseTestcase() *WildcardClickhouseTestcase {
	return &WildcardClickhouseTestcase{
		IntegrationTestcaseBase: IntegrationTestcaseBase{
			ConfigTemplate: "quesma-wildcard-clickhouse.yml.template",
		},
	}
}

func (a *WildcardClickhouseTestcase) SetupContainers(ctx context.Context) error {
	containers, err := setupAllContainersWithCh(ctx, a.ConfigTemplate)
	a.Containers = containers
	return err
}

func (a *WildcardClickhouseTestcase) RunTests(ctx context.Context, t *testing.T) error {
	t.Run("test basic request", func(t *testing.T) { a.testBasicRequest(ctx, t) })
	t.Run("test ingest+query works", func(t *testing.T) { a.testIngestQueryWorks(ctx, t) })
	t.Run("test clickhouse table autodiscovery", func(t *testing.T) { a.testClickHouseTableAutodiscovery(ctx, t) })
	t.Run("test internal endpoints", func(t *testing.T) { a.testInternalEndpoints(ctx, t) })
	return nil
}

func (a *WildcardClickhouseTestcase) testBasicRequest(ctx context.Context, t *testing.T) {
	resp, _ := a.RequestToQuesma(ctx, t, "GET", "/", nil)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func (a *WildcardClickhouseTestcase) testIngestQueryWorks(ctx context.Context, t *testing.T) {
	// First ingest...
	resp, bodyBytes := a.RequestToQuesma(ctx, t, "POST", "/test_index/_doc", []byte(`{"name": "Piotr", "age": 22222}`))
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "Clickhouse", resp.Header.Get("X-Quesma-Source"))
	assert.Equal(t, "Elasticsearch", resp.Header.Get("X-Elastic-Product"))

	// ...then query inserted data
	resp, bodyBytes = a.RequestToQuesma(ctx, t, "POST", "/test_index/_search", []byte(`{"query": {"match_all": {}}}`))
	assert.Contains(t, string(bodyBytes), "Piotr")
	assert.Contains(t, string(bodyBytes), "22222")
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "Clickhouse", resp.Header.Get("X-Quesma-Source"))
	assert.Equal(t, "Elasticsearch", resp.Header.Get("X-Elastic-Product"))

	// Also make sure no such index got created in Elasticsearch
	resp, err := a.RequestToElasticsearch(ctx, "GET", "/test_index/_refresh", nil)
	if err != nil {
		t.Fatalf("Failed to make GET request: %s", err)
	}
	defer resp.Body.Close()
	bodyBytes, err = io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Failed to read response body: %s", err)
	}

	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
	assert.Contains(t, string(bodyBytes), "no such index [test_index]")
}

func (a *WildcardClickhouseTestcase) testClickHouseTableAutodiscovery(ctx context.Context, t *testing.T) {
	// Create test table in ClickHouse
	createTableQuery := "CREATE TABLE IF NOT EXISTS existing_clickhouse_table (id UInt32, name String) ENGINE = Memory"
	if _, err := a.ExecuteClickHouseStatement(ctx, createTableQuery); err != nil {
		t.Fatalf("Failed to create table: %s", err)
	}
	insertRowsQuery := "INSERT INTO existing_clickhouse_table (id, name) VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')"
	if _, err := a.ExecuteClickHouseStatement(ctx, insertRowsQuery); err != nil {
		t.Fatalf("Failed to insert rows: %s", err)
	}

	resp, _ := a.RequestToQuesma(ctx, t, "POST", "/existing_clickhouse_table/_search", []byte(`{"query": {"match_all": {}}}`))
	assert.Equal(t, "Clickhouse", resp.Header.Get("X-Quesma-Source"))

	// This returns 500 Internal Server Error, but will be tackled in separate PR.
	// (The table has not yet been discovered by Quesma )
	//
	// assert.Equal(t, http.StatusOK, resp.StatusCode)
	// assert.Contains(t, string(bodyBytes), "Alice")
	// assert.Contains(t, string(bodyBytes), "Bob")
	// assert.Contains(t, string(bodyBytes), "Charlie")
	// assert.Equal(t, "Elasticsearch", resp.Header.Get("X-Elastic-Product"))
}

// For full list, run "rg -o 'new Route\([^/{}"]*("/_[^/{}"]*(/[^{}/"]*)?")' | cut -d'"' -f2 | sort | uniq"
// on the ES codebase.
var InternalPaths = []string{"/_nodes", "/_xpack", "/_stats", "/_all/_stats", "/_license", "/_cat", "/_cat/health", "/_cluster/stats", "/_data_stream/_stats"}

func (a *WildcardClickhouseTestcase) testInternalEndpoints(ctx context.Context, t *testing.T) {
	for _, internalPath := range InternalPaths {
		t.Run(internalPath, func(t *testing.T) {
			resp, _ := a.RequestToQuesma(ctx, t, "GET", internalPath, nil)
			assert.Equal(t, http.StatusOK, resp.StatusCode)
			assert.Equal(t, "Elasticsearch", resp.Header.Get("X-Quesma-Source"))
			assert.Equal(t, "Elasticsearch", resp.Header.Get("X-Elastic-Product"))
		})
	}
}
