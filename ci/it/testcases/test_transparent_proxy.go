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
	if err != nil {
		return err
	}
	a.Containers = containers
	return nil
}

func (a *TransparentProxyIntegrationTestcase) RunTests(ctx context.Context, t *testing.T) error {
	a.testBasicRequest(ctx, t)
	a.testIfCatHealthRequestReachesElasticsearch(ctx, t)
	a.testIfIndexCreationWorks(ctx, t)
	return nil
}

func (a *TransparentProxyIntegrationTestcase) testBasicRequest(ctx context.Context, t *testing.T) {
	resp, err := a.RequestToQuesma(ctx, "GET", "/", nil)
	if err != nil {
		t.Fatalf("Failed to make GET request: %s", err)
	}
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func (a *TransparentProxyIntegrationTestcase) testIfCatHealthRequestReachesElasticsearch(ctx context.Context, t *testing.T) {
	resp, err := a.RequestToQuesma(ctx, "GET", "/_cat/health", nil)
	if err != nil {
		t.Fatalf("Failed to make GET request: %s", err)
	}
	defer resp.Body.Close()
	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Failed to read response body: %s", err)
	}
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "Elasticsearch", resp.Header.Get("X-elastic-product"))
	assert.Contains(t, string(bodyBytes), "green")
}

func (a *TransparentProxyIntegrationTestcase) testIfIndexCreationWorks(ctx context.Context, t *testing.T) {
	_, err := a.RequestToQuesma(ctx, "PUT", "/index_1", nil)
	_, err = a.RequestToQuesma(ctx, "PUT", "/index_2", nil)

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
