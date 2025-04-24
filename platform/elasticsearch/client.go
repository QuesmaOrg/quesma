// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package elasticsearch

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"github.com/QuesmaOrg/quesma/platform/config"
	"github.com/QuesmaOrg/quesma/platform/logger"
	"net/http"
	"os"
	"time"
)

const (
	esRequestTimeout              = 5 * time.Second
	elasticsearchSecurityEndpoint = "_security/_authenticate"
	openSearchSecurityEndpoint    = "_plugins/_security/api/account"

	tlsCertFile = "TLS_CERT_FILE"
	tlsKeyFile  = "TLS_KEY_FILE"
)

type SimpleClient struct {
	client *http.Client
	config *config.ElasticsearchConfiguration
}

func NewHttpsClient(configuration *config.ElasticsearchConfiguration, timeout time.Duration) *http.Client {
	var cert = tls.Certificate{}
	if configuration.ClientCertPath != "" && configuration.ClientKeyPath != "" {
		var err error
		cert, err = tls.LoadX509KeyPair(configuration.ClientCertPath, configuration.ClientKeyPath)
		if err != nil {
			panic(fmt.Sprintf("failed to load client certificate/key: %v", err))
		}
	}

	var caCertPool *x509.CertPool
	var insecureSkipVerify = true
	if configuration.CACertPath != "" {
		caCert, err := os.ReadFile(configuration.CACertPath)
		if err != nil {
			panic(fmt.Sprintf("failed to read CA certificate: %v", err))
		}
		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)
		insecureSkipVerify = false
	}

	tlsConfig := &tls.Config{
		Certificates:       []tls.Certificate{cert},
		RootCAs:            caCertPool,
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: insecureSkipVerify,
	}

	// Create HTTP client with custom transport
	return &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: tlsConfig,
		},
		Timeout: timeout,
	}
}

func NewSimpleClient(configuration *config.ElasticsearchConfiguration) *SimpleClient {
	client := NewHttpsClient(configuration, esRequestTimeout)
	return &SimpleClient{
		client: client,
		config: configuration,
	}
}

func (es *SimpleClient) Request(ctx context.Context, method, endpoint string, body []byte) (*http.Response, error) {
	return es.doRequest(ctx, method, endpoint, body, nil)
}

func (es *SimpleClient) RequestWithHeaders(ctx context.Context, method, endpoint string, body []byte, headers http.Header) (*http.Response, error) {
	return es.doRequest(ctx, method, endpoint, body, headers)
}

func (es *SimpleClient) Authenticate(ctx context.Context, authHeader string) bool {
	var authEndpoint string
	// This is really suboptimal, and we should find a better way to set this systematically (config perhaps?)
	// OTOH, since we have auth cache in place, I am not concerned about this additional backend call - at least for the time being.
	r, err := es.doRequest(ctx, "GET", "/", nil, http.Header{"Authorization": {authHeader}})
	if err != nil {
		logger.ErrorWithCtx(ctx).Msgf("error sending request: %v", err)
		return false
	}
	defer r.Body.Close()

	if isResponseFromElasticsearch(r) {
		authEndpoint = elasticsearchSecurityEndpoint
	} else {
		authEndpoint = openSearchSecurityEndpoint
	}
	resp, err := es.doRequest(ctx, "GET", authEndpoint, nil, http.Header{"Authorization": {authHeader}})
	if err != nil {
		logger.ErrorWithCtx(ctx).Msgf("error sending request: %v", err)
		return false
	}
	defer resp.Body.Close()

	return resp.StatusCode == http.StatusOK
}

// doRequest can override auth headers specified in the config, use with care!
func (es *SimpleClient) doRequest(ctx context.Context, method, endpoint string, body []byte, headers http.Header) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, fmt.Sprintf("%s/%s", es.config.Url, endpoint), bytes.NewBuffer(body))
	if err != nil {
		return nil, err
	}
	req = AddBasicAuthIfNeeded(req, es.config.User, es.config.Password)
	req.Header.Set("Content-Type", "application/json")
	for key, values := range headers {
		for _, value := range values {
			req.Header.Set(key, value)
		}
	}
	return es.client.Do(req)
}

func isResponseFromElasticsearch(resp *http.Response) bool {
	return resp.Header.Get("X-Elastic-Product") != ""
}
