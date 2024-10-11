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
	//a.testBasicRequest(ctx, t)
	//a.testWildcardGoesToElastic(ctx, t)
	//a.testEmptyTargetDoc(ctx, t)
	//a.testEmptyTargetBulk(ctx, t)
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

func (a *DualWriteAndCommonTableTestcase) testEmptyTargetDoc(ctx context.Context, t *testing.T) {
	resp, err := a.RequestToQuesma(ctx, "POST", "/logs_disabled/_doc", []byte(`{"name": "Alice"}`))
	if err != nil {
		t.Fatalf("Error sending POST request: %s", err)
	}
	defer resp.Body.Close()
	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Failed to read response body: %s", err)
	}

	assert.Contains(t, string(bodyBytes), "index_closed_exception")
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "Clickhouse", resp.Header.Get("X-Quesma-Source"))
	assert.Equal(t, "Elasticsearch", resp.Header.Get("X-Elastic-Product"))
}

func (a *DualWriteAndCommonTableTestcase) testEmptyTargetBulk(ctx context.Context, t *testing.T) {
	bulkPayload := []byte(`
		{ "index": { "_index": "logs_disabled", "_id": "1" } }
		{ "name": "Alice", "age": 30 }
		{ "index": { "_index": "logs_disabled", "_id": "2" } }
		{ "name": "Bob", "age": 25 }
	
`)
	resp, err := a.RequestToQuesma(ctx, "POST", "/_bulk", bulkPayload)
	if err != nil {
		t.Fatalf("Error sending POST request: %s", err)
	}
	defer resp.Body.Close()
	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Failed to read response body: %s", err)
	}

	assert.Contains(t, string(bodyBytes), "index_closed_exception")
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "Clickhouse", resp.Header.Get("X-Quesma-Source"))
	assert.Equal(t, "Elasticsearch", resp.Header.Get("X-Elastic-Product"))
}

// TODO: A POST to /logs_disabled/_doc/:id is going to be routed to Elasticsearch and will return result in writing to the index.
