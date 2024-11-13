// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package testcases

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"net/http"
	"testing"
)

type WildcardDisabledTestcase struct {
	IntegrationTestcaseBase
}

func NewWildcardDisabledTestcase() *WildcardDisabledTestcase {
	return &WildcardDisabledTestcase{
		IntegrationTestcaseBase: IntegrationTestcaseBase{
			ConfigTemplate: "quesma-wildcard-disabled.yml.template",
		},
	}
}

func (a *WildcardDisabledTestcase) SetupContainers(ctx context.Context) error {
	containers, err := setupAllContainersWithCh(ctx, a.ConfigTemplate)
	if err != nil {
		return err
	}
	a.Containers = containers
	return nil
}

func (a *WildcardDisabledTestcase) RunTests(ctx context.Context, t *testing.T) error {
	t.Run("test basic request", func(t *testing.T) { a.testBasicRequest(ctx, t) })
	t.Run("test query is disabled", func(t *testing.T) { a.testQueryIsDisabled(ctx, t) })
	t.Run("test ingest is disabled", func(t *testing.T) { a.testIngestIsDisabled(ctx, t) })
	t.Run("test explicit index query enabled", func(t *testing.T) { a.testExplicitIndexQueryIsEnabled(ctx, t) })
	t.Run("test explicit index ingest enabled", func(t *testing.T) { a.testExplicitIndexIngestIsEnabled(ctx, t) })
	return nil
}

func (a *WildcardDisabledTestcase) testBasicRequest(ctx context.Context, t *testing.T) {
	resp, _ := a.RequestToQuesma(ctx, t, "GET", "/", nil)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func (a *WildcardDisabledTestcase) testQueryIsDisabled(ctx context.Context, t *testing.T) {
	if _, err := a.RequestToElasticsearch(ctx, "PUT", "/elastic_index1", nil); err != nil {
		t.Fatalf("Failed to create index: %s", err)
	}
	if _, err := a.RequestToElasticsearch(ctx, "POST", "/elastic_index1/_refresh", nil); err != nil {
		t.Fatalf("Failed to refresh index: %s", err)
	}

	// Quesma should reject all queries
	for _, tt := range []string{"test_table", "extra_index", "explicitly_disabled1", "explicitly_disabled2", "explicitly_disabled3", "ingest_enabled", "elastic_index1"} {
		t.Run(tt, func(t *testing.T) {
			resp, bodyBytes := a.RequestToQuesma(ctx, t, "POST", fmt.Sprintf("/%s/_search", tt), []byte(`{"query": {"match_all": {}}}`))
			assert.Contains(t, string(bodyBytes), "index_closed_exception")
			assert.Equal(t, http.StatusOK, resp.StatusCode)
			assert.Equal(t, "Clickhouse", resp.Header.Get("X-Quesma-Source"))
			assert.Equal(t, "Elasticsearch", resp.Header.Get("X-Elastic-Product"))
		})
	}
}

func (a *WildcardDisabledTestcase) testIngestIsDisabled(ctx context.Context, t *testing.T) {
	if _, err := a.RequestToElasticsearch(ctx, "PUT", "/elastic_index2", nil); err != nil {
		t.Fatalf("Failed to create index: %s", err)
	}
	if _, err := a.RequestToElasticsearch(ctx, "POST", "/elastic_index2/_refresh", nil); err != nil {
		t.Fatalf("Failed to refresh index: %s", err)
	}

	// Quesma should reject all ingest requests
	for _, tt := range []string{"test_table", "extra_index", "explicitly_disabled1", "explicitly_disabled2", "explicitly_disabled3", "query_enabled", "elastic_index2"} {
		t.Run(tt, func(t *testing.T) {
			resp, bodyBytes := a.RequestToQuesma(ctx, t, "POST", fmt.Sprintf("/%s/_doc", tt), []byte(`{"name": "Piotr", "age": 22222}`))
			assert.Contains(t, string(bodyBytes), "index_closed_exception")
			assert.Equal(t, http.StatusOK, resp.StatusCode)
			assert.Equal(t, "Clickhouse", resp.Header.Get("X-Quesma-Source"))
			assert.Equal(t, "Elasticsearch", resp.Header.Get("X-Elastic-Product"))
		})
	}
}

func (a *WildcardDisabledTestcase) testExplicitIndexQueryIsEnabled(ctx context.Context, t *testing.T) {
	// query_enabled is the only index with query enabled
	resp, bodyBytes := a.RequestToQuesma(ctx, t, "POST", "/query_enabled/_search", []byte(`{"query": {"match_all": {}}}`))
	assert.NotContains(t, string(bodyBytes), "index_closed_exception")
	assert.Equal(t, "Clickhouse", resp.Header.Get("X-Quesma-Source"))
	// TODO: the actual request currently fails since there's no such table in ClickHouse
}

func (a *WildcardDisabledTestcase) testExplicitIndexIngestIsEnabled(ctx context.Context, t *testing.T) {
	// ingest_enabled is the only index with ingest enabled
	resp, bodyBytes := a.RequestToQuesma(ctx, t, "POST", "/ingest_enabled/_doc", []byte(`{"name": "Piotr", "age": 22222}`))
	assert.NotContains(t, string(bodyBytes), "index_closed_exception")
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "Clickhouse", resp.Header.Get("X-Quesma-Source"))
	assert.Equal(t, "Elasticsearch", resp.Header.Get("X-Elastic-Product"))
}
