// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package backend_connectors

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"quesma/elasticsearch"
	"quesma/quesma/config"
	quesma_api "quesma_v2/core"
	"time"
)

const esRequestTimeout = 5 * time.Second

type Rows struct {
	Hits []map[string]interface{}
}

// ElasticsearchBackendConnector is just a test impl -
// TODO: THIS IS A TRUE QUESTION MARK WHETHER IT IS GOING TO STAY LIKE THIS
type ElasticsearchBackendConnector struct {
	client *http.Client
	config config.ElasticsearchConfiguration
}

func NewElasticsearchBackendConnector(cfg config.ElasticsearchConfiguration) *ElasticsearchBackendConnector {
	conn := &ElasticsearchBackendConnector{
		config: cfg,
		client: &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
			},
			Timeout: esRequestTimeout,
		},
	}
	return conn
}

func (e *ElasticsearchBackendConnector) InstanceName() string {
	return "elasticsearch" // TODO return name from config
}

func (e *ElasticsearchBackendConnector) GetConfig() config.ElasticsearchConfiguration {
	return e.config
}

func (e *ElasticsearchBackendConnector) RequestWithHeaders(ctx context.Context, method, endpoint string, body []byte, headers http.Header) (*http.Response, error) {
	return e.doRequest(ctx, method, endpoint, body, headers)
}

func (e *ElasticsearchBackendConnector) doRequest(ctx context.Context, method, endpoint string, body []byte, headers http.Header) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, fmt.Sprintf("%s/%s", e.config.Url, endpoint), bytes.NewBuffer(body))
	if err != nil {
		return nil, err
	}
	if req.Header.Get("Content-Type") == "" {
		req.Header.Set("Content-Type", "application/json")
	}
	req = elasticsearch.AddBasicAuthIfNeeded(req, e.config.User, e.config.Password)
	for key, values := range headers {
		for _, value := range values {
			req.Header.Set(key, value)
		}
	}
	return e.client.Do(req)
}

// HttpBackendConnector is a base interface for sending http requests, for now
type HttpBackendConnector interface {
	Send(r *http.Request) *http.Response
}

func (e *ElasticsearchBackendConnector) Send(r *http.Request) *http.Response {
	r.Host = e.config.Url.Host
	r.URL.Host = e.config.Url.Host
	r.URL.Scheme = e.config.Url.Scheme
	r.RequestURI = "" // this is important for the request to be sent correctly to a different host
	maybeAuthdReq := elasticsearch.AddBasicAuthIfNeeded(r, e.config.User, e.config.Password)
	if resp, err := e.client.Do(maybeAuthdReq); err != nil {
		fmt.Printf("Error: %v\n", err)
		panic(err)
	} else {
		return resp
	}
}

func (e *ElasticsearchBackendConnector) GetId() quesma_api.BackendConnectorType {
	return quesma_api.ElasticsearchBackend
}

func (e *ElasticsearchBackendConnector) Open() error {
	return nil
}

func (e *ElasticsearchBackendConnector) Query(ctx context.Context, query string, args ...interface{}) (quesma_api.Rows, error) {
	panic("not implemented")
}

func (e *ElasticsearchBackendConnector) QueryRow(ctx context.Context, query string, args ...interface{}) quesma_api.Row {
	panic("not implemented")
}

func (e *ElasticsearchBackendConnector) Stats() quesma_api.DBStats {
	return quesma_api.DBStats{}
}

func (e *ElasticsearchBackendConnector) Exec(ctx context.Context, query string, args ...interface{}) error {
	panic("not implemented")
}

func (e *ElasticsearchBackendConnector) Close() error {
	return nil
}

func (e *ElasticsearchBackendConnector) Ping() error {
	return nil
}
