// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package elasticsearch

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"github.com/QuesmaOrg/quesma/platform/config"
	"github.com/stretchr/testify/assert"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"testing"
	"time"
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

func writeTempPEM(t *testing.T, prefix string, pemBytes []byte) string {
	t.Helper()

	tmpFile, err := os.CreateTemp("", prefix+"-*.pem")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer tmpFile.Close()

	if _, err := tmpFile.Write(pemBytes); err != nil {
		t.Fatalf("failed to write to temp file: %v", err)
	}

	t.Cleanup(func() {
		os.Remove(tmpFile.Name())
	})

	return tmpFile.Name()
}

func generateTestCertAndKey(t *testing.T) (certPath, keyPath string) {
	t.Helper()

	private, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("failed to generate private key: %v", err)
	}

	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization: []string{"Test Certificate"},
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(365 * 24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &private.PublicKey, private)
	if err != nil {
		t.Fatalf("failed to create certificate: %v", err)
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(private)})

	certPath = writeTempPEM(t, "test-cert", certPEM)
	keyPath = writeTempPEM(t, "test-key", keyPEM)
	return certPath, keyPath
}

func TestNewHttpsClient_NoCerts(t *testing.T) {
	conf := &config.ElasticsearchConfiguration{}
	client := NewHttpsClient(conf, 5*time.Second)

	tlsConfig := client.Transport.(*http.Transport).TLSClientConfig
	if tlsConfig == nil {
		t.Fatal("expected TLSClientConfig to be set")
	}
	if !tlsConfig.InsecureSkipVerify {
		t.Error("expected InsecureSkipVerify to be true")
	}
}

func TestNewHttpsClient_WithCACert(t *testing.T) {
	caPath, _ := generateTestCertAndKey(t)

	conf := &config.ElasticsearchConfiguration{
		CACertPath: caPath,
	}
	client := NewHttpsClient(conf, 5*time.Second)

	tlsConfig := client.Transport.(*http.Transport).TLSClientConfig
	if tlsConfig.RootCAs == nil {
		t.Error("expected RootCAs to be set")
	}
	if tlsConfig.InsecureSkipVerify {
		t.Error("expected InsecureSkipVerify to be false")
	}
}

func TestNewHttpsClient_WithClientCert(t *testing.T) {
	certPath, keyPath := generateTestCertAndKey(t) // real cert and key

	conf := &config.ElasticsearchConfiguration{
		ClientCertPath: certPath,
		ClientKeyPath:  keyPath,
	}
	client := NewHttpsClient(conf, 5*time.Second)

	tlsConfig := client.Transport.(*http.Transport).TLSClientConfig
	if len(tlsConfig.Certificates) == 0 {
		t.Error("expected client certificate to be set")
	}
}

func TestNewHttpsClient_InvalidCAPath(t *testing.T) {
	conf := &config.ElasticsearchConfiguration{
		CACertPath: "/nonexistent/file.pem",
	}
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic for invalid CA path")
		}
	}()
	NewHttpsClient(conf, 5*time.Second)
}

func TestNewHttpsClient_InvalidClientCertPath(t *testing.T) {
	conf := &config.ElasticsearchConfiguration{
		ClientCertPath: "/invalid/cert.pem",
		ClientKeyPath:  "/invalid/key.pem",
	}
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic for invalid client cert path")
		}
	}()
	NewHttpsClient(conf, 5*time.Second)
}
