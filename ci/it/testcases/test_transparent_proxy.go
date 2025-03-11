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

type TransparentProxyIntegrationTestcase struct {
	IntegrationTestcaseBase
}

func NewTransparentProxyIntegrationTestcase() *TransparentProxyIntegrationTestcase {
	return &TransparentProxyIntegrationTestcase{
		IntegrationTestcaseBase: IntegrationTestcaseBase{
			ConfigTemplate: "quesma-as-transparent-proxy.yml.template",
		},
	}
}

func (a *TransparentProxyIntegrationTestcase) SetupContainers(ctx context.Context) error {
	containers, err := setupContainersForTransparentProxy(ctx, a.ConfigTemplate)
	a.Containers = containers
	return err
}

func (a *TransparentProxyIntegrationTestcase) RunTests(ctx context.Context, t *testing.T) error {
	t.Run("test basic request", func(t *testing.T) { a.testBasicRequest(ctx, t) })
	t.Run("test if cat health request reaches elasticsearch", func(t *testing.T) { a.testIfCatHealthRequestReachesElasticsearch(ctx, t) })
	t.Run("test if index creation works", func(t *testing.T) { a.testIfIndexCreationWorks(ctx, t) })
	t.Run("test internal endpoints", func(t *testing.T) { a.testInternalEndpoints(ctx, t) })
	return nil
}

func (a *TransparentProxyIntegrationTestcase) testBasicRequest(ctx context.Context, t *testing.T) {
	resp, _ := a.RequestToQuesma(ctx, t, "GET", "/", nil)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func (a *TransparentProxyIntegrationTestcase) testIfCatHealthRequestReachesElasticsearch(ctx context.Context, t *testing.T) {
	resp, bodyBytes := a.RequestToQuesma(ctx, t, "GET", "/_cat/health", nil)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "Elasticsearch", resp.Header.Get("X-elastic-product"))
	assert.Contains(t, string(bodyBytes), "green")
}

func (a *TransparentProxyIntegrationTestcase) testIfIndexCreationWorks(ctx context.Context, t *testing.T) {
	_, _ = a.RequestToQuesma(ctx, t, "PUT", "/index_1", nil)
	_, _ = a.RequestToQuesma(ctx, t, "PUT", "/index_2", nil)

	resp, err := a.RequestToElasticsearch(ctx, "GET", "/_cat/indices", nil)
	if err != nil {
		t.Fatalf("Failed to make GET request: %s", err)
	}
	defer resp.Body.Close()
	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Failed to read response body: %s", err)
	}
	assert.Contains(t, string(bodyBytes), "index_1")
	assert.Contains(t, string(bodyBytes), "index_2")
}

func (a *TransparentProxyIntegrationTestcase) testInternalEndpoints(ctx context.Context, t *testing.T) {
	for _, internalPath := range InternalPaths {
		t.Run(internalPath, func(t *testing.T) {
			resp, _ := a.RequestToQuesma(ctx, t, "GET", internalPath, nil)
			assert.Equal(t, http.StatusOK, resp.StatusCode)
			assert.Equal(t, "Elasticsearch", resp.Header.Get("X-Elastic-Product"))
		})
	}
}
