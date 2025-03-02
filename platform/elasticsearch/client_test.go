// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package elasticsearch

import (
	"context"
	"github.com/QuesmaOrg/quesma/platform/config"
	"github.com/stretchr/testify/assert"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
)

const testPayload = "{'test': 'test'}"

func getURL(urlStr string) *config.Url {
	u, _ := url.Parse(urlStr)
	newUrl := config.Url(*u)
	return &newUrl
}

func TestSimpleClient_Request_AddsContentTypeAndDoesntAuthenticateWhenNotConfigured(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "application/json", r.Header.Get("Content-Type"))
		assert.Equal(t, "", r.Header.Get("Authorization"))
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	esClient := &SimpleClient{
		client: &http.Client{},
		config: &config.ElasticsearchConfiguration{
			Url: getURL(server.URL), // No user and password configured for Elasticsearch
		},
	}

	resp, err := esClient.Request(context.Background(), "POST", "test-endpoint", []byte(testPayload))
	assert.NoError(t, err)
	assert.Equal(t, resp.StatusCode, http.StatusOK)
}

func TestSimpleClient_Request_AddsAuthHeadersIfElasticsearchAuthConfigured(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authHeader := r.Header.Get("Authorization")
		assert.Equal(t, "Basic dGVzdHVzZXI6dGVzdHBhc3N3b3Jk", authHeader)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	esClient := &SimpleClient{
		client: &http.Client{},
		config: &config.ElasticsearchConfiguration{
			Url:      getURL(server.URL),
			User:     "testuser",
			Password: "testpassword",
		},
	}

	resp, err := esClient.Request(context.Background(), "POST", "test-endpoint", []byte(testPayload))
	assert.NoError(t, err)
	assert.Equal(t, resp.StatusCode, http.StatusOK)
}

func TestSimpleClient_Authenticate_UsesAuthHeader(t *testing.T) {
	// Even if Elasticsearch auth is configured, Authenticate should always send the Authorization header as is
	const testAuthHeader = "Basic testtoken"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authHeader := r.Header.Get("Authorization")
		assert.Equal(t, testAuthHeader, authHeader)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	esClient := &SimpleClient{
		client: &http.Client{},
		config: &config.ElasticsearchConfiguration{
			Url:      getURL(server.URL),
			User:     "testuser",
			Password: "testpassword",
		},
	}

	result := esClient.Authenticate(context.Background(), testAuthHeader)
	assert.True(t, result)
}

func TestSimpleClient_RequestWithHeaders_OverwritesContentType(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		contentType := r.Header.Get("Content-Type")
		assert.Equal(t, "application/x-ndjson", contentType)
		body, err := io.ReadAll(r.Body)
		assert.NoError(t, err)
		assert.Equal(t, testPayload, string(body))
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	esClient := &SimpleClient{
		client: &http.Client{},
		config: &config.ElasticsearchConfiguration{
			Url: getURL(server.URL),
		},
	}

	headers := http.Header{"Content-Type": {"application/x-ndjson"}}

	resp, err := esClient.RequestWithHeaders(context.Background(), "POST", "test-endpoint", []byte(testPayload), headers)
	assert.NoError(t, err)
	assert.Equal(t, resp.StatusCode, http.StatusOK)
}
