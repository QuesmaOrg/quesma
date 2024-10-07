package testcases

import (
	"context"
	"github.com/stretchr/testify/assert"
	"io"
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
	resp, err := a.RequestToQuesma(ctx, "GET", "/", nil)
	if err != nil {
		t.Fatalf("Failed to make GET request: %s", err)
	}
	defer resp.Body.Close()
	assert.Equal(t, 200, resp.StatusCode)
	/* --------------------------- */
	resp, err = a.RequestToQuesma(ctx, "GET", "/_cat/health", nil)
	if err != nil {
		t.Fatalf("Failed to make GET request: %s", err)
	}
	defer resp.Body.Close()
	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Failed to read response body: %s", err)
	}
	assert.Equal(t, 200, resp.StatusCode)
	assert.Equal(t, "Elasticsearch", resp.Header.Get("X-elastic-product"))
	assert.Contains(t, string(bodyBytes), "green")
	/* --------------------------- */
	_, err = a.RequestToQuesma(ctx, "PUT", "/index_1", nil)
	_, err = a.RequestToQuesma(ctx, "PUT", "/index_2", nil)

	resp, err = a.RequestToElasticsearch(ctx, "GET", "/_cat/indices", nil)
	if err != nil {
		t.Fatalf("Failed to make GET request: %s", err)
	}
	defer resp.Body.Close()
	bodyBytes, err = io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Failed to read response body: %s", err)
	}
	assert.Contains(t, string(bodyBytes), "index_1")
	assert.Contains(t, string(bodyBytes), "index_2")
	return nil
}
