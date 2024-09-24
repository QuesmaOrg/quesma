// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package elasticsearch

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/http"
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
func (es *SimpleClient) Request(method, endpoint string, payload interface{}) (*http.Response, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	return es.doRequest(method, endpoint, body, nil)
}

func (es *SimpleClient) Authenticate(authHeader string) bool {
	resp, err := es.doRequest("GET", "_security/_authenticate", nil, http.Header{"Authorization": {authHeader}})
	if err != nil {
		fmt.Println("Error sending request:", err)
		return false
	}
	defer resp.Body.Close()

	return resp.StatusCode == http.StatusOK
}

// doRequest can override auth headers specified in the config, use with care!
func (es *SimpleClient) doRequest(method, endpoint string, body []byte, headers http.Header) (*http.Response, error) {
	req, err := http.NewRequest(method, fmt.Sprintf("%s/%s", es.config.Url, endpoint), bytes.NewBuffer(body))
	if err != nil {
		return nil, err
	}
	if es.config.User != "" && es.config.Password != "" {
		req.SetBasicAuth(es.config.User, es.config.Password)
	}
	if headers != nil {
		req.Header = headers
	}
	req.Header.Set("Content-Type", "application/json")
	return es.client.Do(req)
}
