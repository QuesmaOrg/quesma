// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package elasticsearch

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/http"
	"quesma/logger"
	"quesma/quesma/config"
	"time"
)

const esRequestTimeout = 5 * time.Second

type SimpleClient struct {
	client *http.Client
	config *config.ElasticsearchConfiguration
}

func NewSimpleClient(configuration *config.ElasticsearchConfiguration) *SimpleClient {
	client := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
		Timeout: esRequestTimeout,
	}
	return &SimpleClient{
		client: client,
		config: configuration,
	}
}
func (es *SimpleClient) Request(ctx context.Context, method, endpoint string, payload interface{}) (*http.Response, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	return es.doRequest(ctx, method, endpoint, body, nil)
}

func (es *SimpleClient) RequestWithHeaders(ctx context.Context, method, endpoint string, payload interface{}, headers http.Header) (*http.Response, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	return es.doRequest(ctx, method, endpoint, body, headers)
}

func (es *SimpleClient) Authenticate(ctx context.Context, authHeader string) bool {
	resp, err := es.doRequest(ctx, "GET", "_security/_authenticate", nil, http.Header{"Authorization": {authHeader}})
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
	if es.config.User != "" && es.config.Password != "" {
		req.SetBasicAuth(es.config.User, es.config.Password)
	}
	req.Header.Set("Content-Type", "application/json")
	if headers != nil {
		for key, values := range headers {
			for _, value := range values {
				req.Header.Set(key, value)
			}
		}
	}
	return es.client.Do(req)
}
