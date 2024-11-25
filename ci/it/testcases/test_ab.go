// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package testcases

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/assert"
	"io"
	"net/http"
	"sort"
	"testing"
	"time"
)

//go:embed queries/*
var queries embed.FS

type ABTestcase struct {
	IntegrationTestcaseBase
}

func NewABTestcase() *ABTestcase {
	return &ABTestcase{
		IntegrationTestcaseBase: IntegrationTestcaseBase{
			ConfigTemplate: "quesma-ab.yml.template",
		},
	}
}

func (a *ABTestcase) SetupContainers(ctx context.Context) error {
	containers, err := setupAllContainersWithCh(ctx, a.ConfigTemplate)
	if err != nil {
		return err
	}
	a.Containers = containers
	return nil
}

func (a *ABTestcase) RunTests(ctx context.Context, t *testing.T) error {
	t.Run("test basic request", func(t *testing.T) { a.testBasicRequest(ctx, t) })
	t.Run("test ingest to both connectors", func(t *testing.T) { a.testIngest(ctx, t) })
	t.Run("test A/B queries", func(t *testing.T) { a.testQueries(ctx, t) })
	return nil
}

func (a *ABTestcase) testBasicRequest(ctx context.Context, t *testing.T) {
	resp, _ := a.RequestToQuesma(ctx, t, "GET", "/", nil)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func (a *ABTestcase) testIngest(ctx context.Context, t *testing.T) {

	// this is very deterministic log

	// Fixed start time
	start, err := time.Parse(time.RFC3339, "2019-01-22T03:56:14+03:30")
	if err != nil {
		t.Fatalf("Failed to parse time: %s", err)
	}

	for i := 0; i < 100; i++ {

		m := make(map[string]interface{})

		m["remote_addr"] = "127.0.0.1"
		m["request"] = fmt.Sprintf("GET /test-%d", i)
		m["status"] = ((i % 4) + 2) * 100 // 200,300,400,500
		m["body_bytes_sent"] = i * 1000
		m["http_referer"] = "-"

		if i%2 == 0 {
			m["http_user_agent"] = "Mozilla/5.0"
		} else {
			m["http_user_agent"] = "curl/7.68"
		}
		m["@timestamp"] = start.Add(time.Duration(i) * time.Second).Format(time.RFC3339)

		doc, err := json.Marshal(m)
		if err != nil {
			t.Fatalf("Failed to marshal json: %s", err)
		}

		resp, body := a.RequestToQuesma(ctx, t, "POST", "/test_index/_doc", doc)

		if resp.StatusCode != http.StatusOK {
			fmt.Println("XXX", string(body))
		}
		resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode)
	}

	chQuery := "SELECT count(*) FROM test_index"
	rows, err := a.ExecuteClickHouseQuery(ctx, chQuery)
	if err != nil {
		t.Fatalf("Failed to execute query: %s", err)
	}

	var count int
	for rows.Next() {
		if err := rows.Scan(&count); err != nil {
			t.Fatalf("Failed to scan row: %s", err)
		}
	}

	if rows.Err() != nil {
		t.Fatalf("Failed to scan row: %s", rows.Err())
	}

	assert.Equal(t, 100, count)

	_, _ = a.RequestToElasticsearch(ctx, "GET", "/test_index/_refresh", nil)

	// Also make sure no such index got created in Elasticsearch
	resp, err := a.RequestToElasticsearch(ctx, "GET", "/test_index/_count", nil)
	if err != nil {
		t.Fatalf("Failed to make GET request: %s", err)
	}
	defer resp.Body.Close()
	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Failed to read response body: %s", err)
	}

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	elasticResult := make(map[string]interface{})
	if err := json.Unmarshal(bodyBytes, &elasticResult); err != nil {
		t.Fatalf("Failed to unmarshal response: %s", err)
	}

	assert.Equal(t, 100, int(elasticResult["count"].(float64)))

}

func (a *ABTestcase) testQueries(ctx context.Context, t *testing.T) {

	_, _ = a.RequestToElasticsearch(ctx, "GET", "/test_index/_refresh", nil)

	files, err := queries.ReadDir("queries")
	if err != nil {
		t.Fatalf("Failed to read queries: %s", err)
	}

	skip := map[string]bool{}

	// here we skip some queries that are known to fail
	// TODO fix them
	skip["01.json"] = true
	skip["02.json"] = false
	skip["03.json"] = true
	skip["04.json"] = true
	skip["05.json"] = true
	skip["06.json"] = true
	skip["07.json"] = false
	skip["08.json"] = false

	for _, file := range files {

		if skip[file.Name()] {
			fmt.Println("Skipping", file.Name())
			continue
		}

		query, err := queries.ReadFile("queries/" + file.Name())
		if err != nil {
			t.Fatalf("Failed to read file: %s", err)
		}

		resp, _ := a.RequestToQuesma(ctx, t, "POST", "/test_index/_search", query)
		assert.Equal(t, http.StatusOK, resp.StatusCode)
	}

	time.Sleep(10 * time.Second)

	mismatches := []map[string]any{}

	chQuery := "SELECT * FROM ab_testing_logs where  response_mismatch_is_ok = false"
	rows, err := a.ExecuteClickHouseQuery(ctx, chQuery)
	if err != nil {
		t.Fatalf("Failed to execute query: %s", err)
	}

	cols, err := rows.Columns()
	if err != nil {
		t.Fatalf("Failed to get columns: %s", err)
	}

	for rows.Next() {

		r := make([]interface{}, len(cols))
		valPtrs := make([]interface{}, len(cols))
		for i := range r {
			valPtrs[i] = &r[i]
		}
		if err := rows.Scan(valPtrs...); err != nil {
			t.Fatalf("Failed to scan row: %s", err)
		}

		rm := make(map[string]any)
		for i, c := range cols {
			switch v := r[i].(type) {
			case *bool:
				rm[c] = *v
			case string:
				rm[c] = v
			case *string:
				rm[c] = *v
			case *int64:
				rm[c] = *v
			case *float64:
				rm[c] = *v
			default:
				rm[c] = fmt.Sprintf("%v", v)
			}
		}

		mismatches = append(mismatches, rm)
	}

	if rows.Err() != nil {
		t.Fatalf("Failed to scan row: %s", rows.Err())
	}

	sort.Strings(cols)

	if len(mismatches) > 0 {
		for n, m := range mismatches {
			fmt.Printf("Mismatch: %d\n", n+1)
			for _, name := range cols {
				fmt.Printf("   %s=%v\n", name, m[name])
			}
		}

		t.Fatalf("Found %d mismatches", len(mismatches))
	}
}
