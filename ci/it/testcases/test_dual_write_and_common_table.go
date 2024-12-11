// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package testcases

import (
	"context"
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"io"
	"net/http"
	"testing"
)

type DualWriteAndCommonTableTestcase struct {
	IntegrationTestcaseBase
}

func NewDualWriteAndCommonTableTestcase() *DualWriteAndCommonTableTestcase {
	return &DualWriteAndCommonTableTestcase{
		IntegrationTestcaseBase: IntegrationTestcaseBase{
			ConfigTemplate: "quesma-with-dual-writes-and-common-table.yml.template",
		},
	}
}

func (a *DualWriteAndCommonTableTestcase) SetupContainers(ctx context.Context) error {
	containers, err := setupAllContainersWithCh(ctx, a.ConfigTemplate)
	if err != nil {
		return err
	}
	a.Containers = containers
	return nil
}

func (a *DualWriteAndCommonTableTestcase) RunTests(ctx context.Context, t *testing.T) error {
	t.Run("test basic request", func(t *testing.T) { a.testBasicRequest(ctx, t) })
	t.Run("test ingest to clickhouse works", func(t *testing.T) { a.testIngestToClickHouseWorks(ctx, t) })
	t.Run("test ingest to common table works", func(t *testing.T) { a.testIngestToCommonTableWorks(ctx, t) })
	t.Run("test dual query returns data from clickhouse", func(t *testing.T) { a.testDualQueryReturnsDataFromClickHouse(ctx, t) })
	t.Run("test dual writes work", func(t *testing.T) { a.testDualWritesWork(ctx, t) })
	t.Run("test wildcard goes to elastic", func(t *testing.T) { a.testWildcardGoesToElastic(ctx, t) })
	t.Run("test _resolve/index/* works properly", func(t *testing.T) { a.testResolveEndpointInQuesma(ctx, t) })
	return nil
}

func (a *DualWriteAndCommonTableTestcase) testBasicRequest(ctx context.Context, t *testing.T) {
	resp, _ := a.RequestToQuesma(ctx, t, "GET", "/", nil)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}
func (a *DualWriteAndCommonTableTestcase) testIngestToCommonTableWorks(ctx context.Context, t *testing.T) {
	resp, _ := a.RequestToQuesma(ctx, t, "POST", "/logs-4/_doc", []byte(`{"name": "Przemyslaw", "age": 31337}`))
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	chQuery := "SELECT * FROM 'quesma_common_table'"
	rows, err := a.ExecuteClickHouseQuery(ctx, chQuery)
	if err != nil {
		t.Fatalf("Failed to execute query: %s", err)
	}
	columnTypes, err := rows.ColumnTypes()
	values := make([]interface{}, len(columnTypes))
	valuePtrs := make([]interface{}, len(columnTypes))
	for i := range values {
		valuePtrs[i] = &values[i]
	}
	var name string
	var age int
	var quesmaIndexName string
	for rows.Next() {
		if err := rows.Scan(valuePtrs...); err != nil {
			t.Fatalf("Failed to scan row: %s", err)
		}
		for i, col := range values {
			switch columnTypes[i].Name() {
			case "__quesma_index_name":
				if v, ok := col.(string); ok {
					quesmaIndexName = v
				}
			case "name":
				if v, ok := col.(*string); ok {
					name = *v
				}
			case "age":
				if v, ok := col.(*int64); ok {
					age = int(*v)
				}
			}
		}
		if name == "Przemyslaw" && age == 31337 && quesmaIndexName == "logs-4" {
			break
		}
	}
	assert.Equal(t, "Przemyslaw", name)
	assert.Equal(t, 31337, age)
	assert.Equal(t, "logs-4", quesmaIndexName)

	resp, bodyBytes := a.RequestToQuesma(ctx, t, "GET", "/logs-4/_search", []byte(`{"query": {"match_all": {}}}`))
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, string(bodyBytes), "Przemyslaw")
	assert.Contains(t, "Clickhouse", resp.Header.Get("X-Quesma-Source"))
}

func (a *DualWriteAndCommonTableTestcase) testDualQueryReturnsDataFromClickHouse(ctx context.Context, t *testing.T) {
	resp, _ := a.RequestToQuesma(ctx, t, "POST", "/logs-dual_query/_doc", []byte(`{"name": "Przemyslaw", "age": 31337}`))
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	chQuery := "SELECT * FROM 'logs-dual_query'"
	rows, err := a.ExecuteClickHouseQuery(ctx, chQuery)
	if err != nil {
		t.Fatalf("Failed to execute query: %s", err)
	}
	columnTypes, err := rows.ColumnTypes()
	values := make([]interface{}, len(columnTypes))
	valuePtrs := make([]interface{}, len(columnTypes))
	for i := range values {
		valuePtrs[i] = &values[i]
	}
	var name string
	var age int
	for rows.Next() {
		if err := rows.Scan(valuePtrs...); err != nil {
			t.Fatalf("Failed to scan row: %s", err)
		}
		for i, col := range values {
			switch columnTypes[i].Name() {
			case "name":
				if v, ok := col.(*string); ok {
					name = *v
				}
			case "age":
				if v, ok := col.(*int64); ok {
					age = int(*v)
				}
			}
		}
		if name == "Przemyslaw" && age == 31337 {
			break
		}
	}
	assert.Equal(t, "Przemyslaw", name)
	assert.Equal(t, 31337, age)

	// In the meantime let's delete the index from Elasticsearch
	resp, _ = a.RequestToElasticsearch(ctx, "DELETE", "/logs-dual_query", nil)
	if err != nil {
		t.Fatalf("Failed to make DELETE request: %s", err)
	}
	// FINAL TEST - WHETHER QUESMA RETURNS DATA FROM CLICKHOUSE
	resp, bodyBytes := a.RequestToQuesma(ctx, t, "GET", "/logs-dual_query/_search", []byte(`{"query": {"match_all": {}}}`))
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, string(bodyBytes), "Przemyslaw")
	assert.Contains(t, "Clickhouse", resp.Header.Get("X-Quesma-Source"))
}

func (a *DualWriteAndCommonTableTestcase) testIngestToClickHouseWorks(ctx context.Context, t *testing.T) {
	resp, _ := a.RequestToQuesma(ctx, t, "POST", "/logs-2/_doc", []byte(`{"name": "Przemyslaw", "age": 31337}`))
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	chQuery := "SELECT * FROM 'logs-2'"
	rows, err := a.ExecuteClickHouseQuery(ctx, chQuery)
	if err != nil {
		t.Fatalf("Failed to execute query: %s", err)
	}
	columnTypes, err := rows.ColumnTypes()
	values := make([]interface{}, len(columnTypes))
	valuePtrs := make([]interface{}, len(columnTypes))
	for i := range values {
		valuePtrs[i] = &values[i]
	}
	var name string
	var age int
	for rows.Next() {
		if err := rows.Scan(valuePtrs...); err != nil {
			t.Fatalf("Failed to scan row: %s", err)
		}
		for i, col := range values {
			switch columnTypes[i].Name() {
			case "name":
				if v, ok := col.(*string); ok {
					name = *v
				}
			case "age":
				if v, ok := col.(*int64); ok {
					age = int(*v)
				}
			}
		}
		if name == "Przemyslaw" && age == 31337 {
			break
		}
	}
	assert.Equal(t, "Przemyslaw", name)
	assert.Equal(t, 31337, age)

	// Also make sure no such index got created in Elasticsearch
	resp, err = a.RequestToElasticsearch(ctx, "GET", "/logs-2/_refresh", nil)
	if err != nil {
		t.Fatalf("Failed to make GET request: %s", err)
	}
	defer resp.Body.Close()
	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Failed to read response body: %s", err)
	}

	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
	assert.Contains(t, string(bodyBytes), "no such index [logs-2]")
}

func (a *DualWriteAndCommonTableTestcase) testDualWritesWork(ctx context.Context, t *testing.T) {
	resp, _ := a.RequestToQuesma(ctx, t, "POST", "/logs-3/_doc", []byte(`{"name": "Przemyslaw", "age": 31337}`))
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	chQuery := "SELECT * FROM 'logs-3'"
	rows, err := a.ExecuteClickHouseQuery(ctx, chQuery)
	if err != nil {
		t.Fatalf("Failed to execute query: %s", err)
	}
	columnTypes, err := rows.ColumnTypes()
	values := make([]interface{}, len(columnTypes))
	valuePtrs := make([]interface{}, len(columnTypes))
	for i := range values {
		valuePtrs[i] = &values[i]
	}
	var name string
	var age int
	for rows.Next() {
		if err := rows.Scan(valuePtrs...); err != nil {
			t.Fatalf("Failed to scan row: %s", err)
		}
		for i, col := range values {
			switch columnTypes[i].Name() {
			case "name":
				if v, ok := col.(*string); ok {
					name = *v
				}
			case "age":
				if v, ok := col.(*int64); ok {
					age = int(*v)
				}
			}
		}
		if name == "Przemyslaw" && age == 31337 {
			break
		}
	}
	assert.Equal(t, "Przemyslaw", name)
	assert.Equal(t, 31337, age)

	// Also make sure no such index got created in Elasticsearch
	_, _ = a.RequestToElasticsearch(ctx, "GET", "/logs-3/_refresh", nil)
	resp, err = a.RequestToElasticsearch(ctx, "GET", "/logs-3/_search", []byte(`{"query": {"match_all": {}}}`))
	if err != nil {
		t.Fatalf("Failed to make GET request: %s", err)
	}
	defer resp.Body.Close()
	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Failed to read response body: %s", err)
	}

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, string(bodyBytes), "Przemyslaw")
}

func (a *DualWriteAndCommonTableTestcase) testWildcardGoesToElastic(ctx context.Context, t *testing.T) {
	// Given an index in Elasticsearch which falls under `*` in the configuration
	var err error
	if _, err = a.RequestToElasticsearch(ctx, "PUT", "/unmentioned_index", nil); err != nil {
		t.Fatalf("Failed to create index: %s", err)
	}
	if _, err = a.RequestToElasticsearch(ctx, "POST", "/unmentioned_index/_doc/1", []byte(`{"name": "Alice"}`)); err != nil {
		t.Fatalf("Failed to insert document: %s", err)
	}
	if _, err = a.RequestToElasticsearch(ctx, "POST", "/unmentioned_index/_refresh", nil); err != nil {
		t.Fatalf("Failed to refresh index: %s", err)
	}
	// When Quesma searches for that document
	resp, bodyBytes := a.RequestToQuesma(ctx, t, "POST", "/unmentioned_index/_search", []byte(`{"query": {"match_all": {}}}`))

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

func (a *DualWriteAndCommonTableTestcase) testResolveEndpointInQuesma(ctx context.Context, t *testing.T) {
	// When Quesma searches for that document
	resp, bodyBytes := a.RequestToQuesma(ctx, t, "GET", "/_resolve/index/*", nil)

	var jsonResponse map[string]interface{}
	if err := json.Unmarshal(bodyBytes, &jsonResponse); err != nil {
		t.Fatalf("Failed to unmarshal response body: %s", err)
	}

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "Clickhouse", resp.Header.Get("X-Quesma-Source"))
	expectedResponse := map[string]interface{}{
		"indices": []interface{}{
			map[string]interface{}{
				"name":       "logs-3",
				"attributes": []interface{}{"open"},
			},
			map[string]interface{}{
				"name":       "quesma_virtual_tables",
				"attributes": []interface{}{"open"},
			},
			map[string]interface{}{
				"name":       "unmentioned_index",
				"attributes": []interface{}{"open"},
			},
		},
		"aliases": []interface{}{},
		"data_streams": []interface{}{
			map[string]interface{}{
				"name":            "logs-2",
				"backing_indices": []interface{}{"logs-2"},
				"timestamp_field": "@timestamp",
			},
			map[string]interface{}{
				"name":            "logs-3",
				"backing_indices": []interface{}{"logs-3"},
				"timestamp_field": "@timestamp",
			},
			map[string]interface{}{
				"name":            "logs-4",
				"backing_indices": []interface{}{"logs-4"},
				"timestamp_field": "@timestamp",
			},
			map[string]interface{}{
				"name":            "logs-dual_query",
				"backing_indices": []interface{}{"logs-dual_query"},
				"timestamp_field": "@timestamp",
			},
		},
	}
	assert.ElementsMatchf(t, expectedResponse["indices"], jsonResponse["indices"], "indices do not match")
	assert.ElementsMatchf(t, expectedResponse["aliases"], jsonResponse["aliases"], "aliases do not match")
	assert.ElementsMatchf(t, expectedResponse["data_streams"], jsonResponse["data_streams"], "data_streams do not match")
}
