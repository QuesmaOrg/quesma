// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package backend_connectors

import (
	"context"
	"crypto/tls"
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

// HttpBackendConnector is a base interface for sending http requests, for now
type HttpBackendConnector interface {
	Send(r http.Request) *http.Response
}

func (e *ElasticsearchBackendConnector) Send(r http.Request) *http.Response {
	e.client = &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
		Timeout: esRequestTimeout,
	}
	e.config = config.ElasticsearchConfiguration{Url: &config.Url{
		Host: "localhost:9200",
	}}
	r.Host = e.config.Url.Host
	maybeAuthdReq := elasticsearch.AddBasicAuthIfNeeded(&r, e.config.User, e.config.Password)
	if resp, err := e.client.Do(maybeAuthdReq); err != nil {
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

func (e *ElasticsearchBackendConnector) Exec(ctx context.Context, query string, args ...interface{}) error {
	panic("not implemented")
}

func (e *ElasticsearchBackendConnector) Close() error {
	return nil
}
