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
	a.testBasicRequest(ctx, t)
	a.testIngestToClickHouseWorks(ctx, t)
	//a.testIngestToCommonTableWorks(ctx, t)
	//a.testDualQueryReturnsDataFromClickHouse(ctx, t)
	//a.testDualWritesWork(ctx, t)
	a.testWildcardGoesToElastic(ctx, t)
	return nil
}

func (a *DualWriteAndCommonTableTestcase) testBasicRequest(ctx context.Context, t *testing.T) {
	resp, err := a.RequestToQuesma(ctx, "GET", "/", nil)
	if err != nil {
		t.Fatalf("Failed to make GET request: %s", err)
	}
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func (a *DualWriteAndCommonTableTestcase) testIngestToClickHouseWorks(ctx context.Context, t *testing.T) {
	resp, err := a.RequestToQuesma(ctx, "POST", "/logs-2/_doc", []byte(`{"name": "Przemyslaw", "age": 31337}`))
	if err != nil {
		t.Fatalf("Failed to insert document: %s", err)
	}
	defer resp.Body.Close()
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
	resp, err = a.RequestToElasticsearch(ctx, "GET", "/_cat/indices", nil)
	if err != nil {
		t.Fatalf("Failed to make GET request: %s", err)
	}
	defer resp.Body.Close()
	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Failed to read response body: %s", err)
	}
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Greater(t, len(bodyBytes), 0)
	assert.Contains(t, string(bodyBytes), "green") // at least one index should be there
	assert.NotContains(t, string(bodyBytes), "logs-2")
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
	resp, err := a.RequestToQuesma(ctx, "POST", "/unmentioned_index/_search", []byte(`{"query": {"match_all": {}}}`))
	if err != nil {
		t.Fatalf("Failed to make GET request: %s", err)
	}
	defer resp.Body.Close()
	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Failed to read response body: %s", err)
	}
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
