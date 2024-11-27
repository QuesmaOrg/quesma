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
	"strings"
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
			fmt.Println("Failed POST request: ", string(body))
			t.Fatalf("Failed to make POST request: %s", resp.Status)
		}
		_ = resp.Body.Close()

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

func (a ABTestcase) waitForAbResults(ctx context.Context, t *testing.T) {

	timeout := 10

	for i := 0; i <= timeout; i++ {
		time.Sleep(1 * time.Second)

		if i == timeout {
			t.Fatalf("Didn't find any results in ab_testing_logs table. Failing the test.")
		}

		count := "SELECT count(*) FROM ab_testing_logs"
		rows, err := a.ExecuteClickHouseQuery(ctx, count)

		if err != nil {
			if strings.Contains(err.Error(), "code: 60") { // 60 is the code for table not found
				continue
			}

			t.Fatalf("Failed to execute query: %s", err)
		}
		for rows.Next() {
			var c int
			if err := rows.Scan(&c); err != nil {
				t.Fatalf("Failed to scan row: %s", err)
			}
			if c > 0 { // we have some results,
				return
			}
		}

		if rows.Err() != nil {
			t.Fatalf("Failed to scan row: %s", rows.Err())
		}
		_ = rows.Close()
	}
}

func (a *ABTestcase) readABResults(ctx context.Context, t *testing.T) ([]map[string]any, []string) {
	var mismatches []map[string]any

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

	return mismatches, cols
}

func (a *ABTestcase) testQueries(ctx context.Context, t *testing.T) {

	_, _ = a.RequestToElasticsearch(ctx, "GET", "/test_index/_refresh", nil)

	files, err := queries.ReadDir("queries")
	if err != nil {
		t.Fatalf("Failed to read queries: %s", err)
	}

	skip := map[string]bool{}

	// here we skip some queries that are known to fail
	//
	//
	// These are the queries that are known to fail
	//
	// https://github.com/QuesmaOrg/quesma/issues/1044
	skip["04.json"] = true // date_histogram aggregation is used here, quesma output differs from ES
	skip["06.json"] = true // it contains histogram aggregation, quesma returns different results than ES

	for _, file := range files {
		t.Run(file.Name(), func(t *testing.T) {
			_, _ = a.ExecuteClickHouseStatement(ctx, "delete from ab_testing_logs where true")

			if skip[file.Name()] {
				fmt.Println("Skipping", file.Name())
				return
			}

			query, err := queries.ReadFile("queries/" + file.Name())
			if err != nil {
				t.Fatalf("Failed to read file: %s", err)
			}

			resp, _ := a.RequestToQuesma(ctx, t, "POST", "/test_index/_search", query)
			assert.Equal(t, http.StatusOK, resp.StatusCode)

			a.waitForAbResults(ctx, t)

			mismatches, cols := a.readABResults(ctx, t)

			if len(mismatches) > 0 {
				for n, m := range mismatches {
					fmt.Printf("Mismatch: %d\n", n+1)
					for _, name := range cols {
						fmt.Printf("   %s=%v\n", name, m[name])
					}
				}

				t.Fatalf("Found %d mismatches", len(mismatches))
			}
		})
	}
}
