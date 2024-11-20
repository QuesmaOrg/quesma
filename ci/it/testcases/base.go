// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package testcases

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	"io"
	"net/http"
	"testing"
	"time"
)

// Base structs/interfaces for integration tests

type TestCase interface {
	SetupContainers(ctx context.Context) error
	RunTests(ctx context.Context, t *testing.T) error
	Cleanup(ctx context.Context)
}

type IntegrationTestcaseBase struct {
	ConfigTemplate string
	Containers     *Containers
}

func (tc *IntegrationTestcaseBase) SetupContainers(ctx context.Context) error {
	return nil
}

func (tc *IntegrationTestcaseBase) RunTests(ctx context.Context, t *testing.T) error {
	return nil
}

func (tc *IntegrationTestcaseBase) Cleanup(ctx context.Context) {
	tc.Containers.Cleanup(ctx)
}

func (tc *IntegrationTestcaseBase) getQuesmaEndpoint() string {
	ctx := context.Background()
	q := *tc.Containers.Quesma
	p, _ := q.MappedPort(ctx, "8080/tcp")
	h, _ := q.Host(ctx)
	return fmt.Sprintf("http://%s:%s", h, p.Port())
}

func (tc *IntegrationTestcaseBase) getElasticsearchEndpoint() string {
	ctx := context.Background()
	q := *tc.Containers.Elasticsearch
	p, _ := q.MappedPort(ctx, "9200/tcp")
	h, _ := q.Host(ctx)
	return fmt.Sprintf("http://%s:%s", h, p.Port())
}

func (tc *IntegrationTestcaseBase) getClickHouseClient() (*sql.DB, error) {
	ctx := context.Background()
	q := *tc.Containers.ClickHouse
	p, _ := q.MappedPort(ctx, "9000/tcp")
	h, _ := q.Host(ctx)
	options := clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%s", h, p.Port())},
		TLS:  nil,
		Auth: clickhouse.Auth{
			Username: "default", // Replace with your ClickHouse username
			Password: "",        // Replace with your ClickHouse password, if any
		},
	}
	db := clickhouse.OpenDB(&options)
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping ClickHouse: %w", err)
	}
	db.SetMaxIdleConns(20)
	db.SetMaxOpenConns(30)
	db.SetConnMaxLifetime(15 * time.Minute)
	db.SetConnMaxIdleTime(5 * time.Minute)
	return db, nil
}

func (tc *IntegrationTestcaseBase) ExecuteClickHouseQuery(ctx context.Context, query string) (*sql.Rows, error) {
	db, err := tc.getClickHouseClient()
	if err != nil {
		return nil, err
	}
	if errP := db.Ping(); errP != nil {
		return nil, fmt.Errorf("failed to ping ClickHouse: %w", errP)
	}
	rows, errQ := db.QueryContext(ctx, query)

	if errQ != nil {
		return nil, errQ
	}
	defer db.Close()
	return rows, nil
}

func (tc *IntegrationTestcaseBase) ExecuteClickHouseStatement(ctx context.Context, stmt string) (sql.Result, error) {
	db, err := tc.getClickHouseClient()
	if err != nil {
		return nil, err
	}
	if errP := db.Ping(); errP != nil {
		return nil, fmt.Errorf("failed to ping ClickHouse: %w", errP)
	}
	res, errQ := db.ExecContext(ctx, stmt)

	if errQ != nil {
		return nil, errQ
	}
	defer db.Close()
	return res, nil
}

func (tc *IntegrationTestcaseBase) FetchClickHouseColumns(ctx context.Context, tableName string) (map[string]string, error) {
	rows, err := tc.ExecuteClickHouseQuery(ctx, fmt.Sprintf("SELECT name, type FROM system.columns WHERE table = '%s'", tableName))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[string]string)
	for rows.Next() {
		var name, colType string
		if err := rows.Scan(&name, &colType); err != nil {
			return nil, err
		}
		result[name] = colType
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func (tc *IntegrationTestcaseBase) RequestToQuesma(ctx context.Context, t *testing.T, method, uri string, requestBody []byte) (*http.Response, []byte) {
	endpoint := tc.getQuesmaEndpoint()
	resp, err := tc.doRequest(ctx, method, endpoint+uri, requestBody, nil)
	if err != nil {
		t.Fatalf("Error sending %s request to the endpoint '%s': %s", method, uri, err)
	}
	defer resp.Body.Close()
	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Failed to read response body of %s request to the endpoint '%s': %s", method, uri, err)
	}
	return resp, responseBody
}

func (tc *IntegrationTestcaseBase) RequestToElasticsearch(ctx context.Context, method, uri string, body []byte) (*http.Response, error) {
	endpoint := tc.getElasticsearchEndpoint()
	return tc.doRequest(ctx, method, endpoint+uri, body, nil)
}

func (tc *IntegrationTestcaseBase) doRequest(ctx context.Context, method, endpoint string, body []byte, headers http.Header) (*http.Response, error) {
	client := &http.Client{}
	req, err := http.NewRequestWithContext(ctx, method, endpoint, bytes.NewBuffer(body))
	if err != nil {
		return nil, err
	}
	req.SetBasicAuth("elastic", "quesmaquesma")
	req.Header.Set("Content-Type", "application/json")
	return client.Do(req)
}
