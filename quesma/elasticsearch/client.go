// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package elasticsearch

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"quesma/logger"
	"quesma/quesma/config"
	"time"
)

const (
	esRequestTimeout              = 5 * time.Second
	elasticsearchSecurityEndpoint = "_security/_authenticate"
	openSearchSecurityEndpoint    = "_plugins/_security/api/account"
)

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
func (es *SimpleClient) Request(ctx context.Context, method, endpoint string, body []byte) (*http.Response, error) {
	return es.doRequest(ctx, method, endpoint, body, nil)
}

func (es *SimpleClient) RequestWithHeaders(ctx context.Context, method, endpoint string, body []byte, headers http.Header) (*http.Response, error) {
	return es.doRequest(ctx, method, endpoint, body, headers)
}

func (es *SimpleClient) DoRequestCheckResponseStatusOK(ctx context.Context, method, endpoint string, body []byte) (resp *http.Response, err error) {
	resp, err = es.doRequest(ctx, method, endpoint, body, nil)
	if err != nil {
		return
	}

	if resp.StatusCode != http.StatusOK {
		return resp, fmt.Errorf("response code from Elastic is not 200 OK, but %s", resp.Status)
	}
	return resp, nil
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
