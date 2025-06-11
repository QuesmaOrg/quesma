// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package testcases

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

type SplitTimeRangeTestcase struct {
	IntegrationTestcaseBase
}

func NewSplitTimeRangeTestcase() *SplitTimeRangeTestcase {
	return &SplitTimeRangeTestcase{
		IntegrationTestcaseBase: IntegrationTestcaseBase{
			ConfigTemplate: "quesma-split-time-range.yml.template",
		},
	}
}

func (a *SplitTimeRangeTestcase) SetupContainers(ctx context.Context) error {
	containers, err := setupAllContainersWithCh(ctx, a.ConfigTemplate)
	a.Containers = containers
	return err
}

const sortByTimestamp = `
"sort": [
	{
		"@timestamp": {
			"format": "strict_date_optional_time",
			"order": "desc",
			"unmapped_type": "boolean"
		}
	},
	{
		"_doc": {
			"order": "desc",
			"unmapped_type": "boolean"
		}
	}
],
`

func (a *SplitTimeRangeTestcase) RunTests(ctx context.Context, t *testing.T) error {
	t.Run("test basic request", func(t *testing.T) { a.testBasicRequest(ctx, t) })
	t.Run("test timeranges (handwritten)", func(t *testing.T) { a.testTimerangesHandwritten(ctx, t) })
	t.Run("test timeranges (random)", func(t *testing.T) { a.testTimerangesRandom(ctx, t) })
	return nil
}

func (a *SplitTimeRangeTestcase) testBasicRequest(ctx context.Context, t *testing.T) {
	resp, _ := a.RequestToQuesma(ctx, t, "GET", "/", nil)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

// Handwritten queries
func (a *SplitTimeRangeTestcase) testTimerangesHandwritten(ctx context.Context, t *testing.T) {
	// Insert 2 documents for each minute between 2024-01-01 and 2024-01-02
	var bulkBody []byte
	var elasticTimestamps []string
	for minute := 0; minute < 24*60; minute++ {
		jsonTimestamp := fmt.Sprintf("2024-01-01T%02d:%02d:00Z", minute/60, minute%60)
		elasticTimestamps = append(elasticTimestamps, fmt.Sprintf("2024-01-01 %02d:%02d:00 +0000 UTC", minute/60, minute%60))

		bulkBody = append(bulkBody, []byte(fmt.Sprintf(`{ "index": { "_index": "testtable1" } }%s`, "\n"))...)
		bulkBody = append(bulkBody, []byte(fmt.Sprintf(`{"name": "Przemyslaw", "age": %d, "@timestamp": "%s"}%s`, 31337+minute, jsonTimestamp, "\n"))...)

		bulkBody = append(bulkBody, []byte(fmt.Sprintf(`{ "index": { "_index": "testtable1" } }%s`, "\n"))...)
		bulkBody = append(bulkBody, []byte(fmt.Sprintf(`{"name": "Piotr", "age": %d, "@timestamp": "%s"}%s`, 131337+minute, jsonTimestamp, "\n"))...)
	}
	resp, _ := a.RequestToQuesma(ctx, t, "POST", "/_bulk", bulkBody)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// Whole range, should return all documents
	rows := a.sendSearchQuery(ctx, t, fmt.Sprintf(`{
		"size": 10000,
		"query": {
			"range": {
				"@timestamp": {
					"gte": "2024-01-01T00:00:00Z",
					"lte": "2024-01-02T00:00:00Z"
				}
			}
		},
		%s
	}`, sortByTimestamp), 24*60, 24*60)

	// Outside range, should return no documents
	rows = a.sendSearchQuery(ctx, t, fmt.Sprintf(`{
		"size": 10000,
		"query": {
			"range": {
				"@timestamp": {
					"gte": "2024-02-01T00:00:00Z",
					"lte": "2024-02-02T00:00:00Z"
				}
			}
		},
		%s
	}`, sortByTimestamp), 0, 0)

	// Whole range, only "Piotr", should return half of documents
	rows = a.sendSearchQuery(ctx, t, fmt.Sprintf(`{
		"size": 10000,
		"query": {
			"bool": {
				"must": [
					{
						"range": {
							"@timestamp": {
								"gte": "2024-01-01T00:00:00Z",
								"lte": "2024-01-02T00:00:00Z"
							}
						}
					},
					{
						"term": {
							"name": "Piotr"
						}
					}
				]
			}
		},
		%s
	}`, sortByTimestamp), 24*60, 0)

	// Whole range, LIMIT 100, should return exactly 100 documents (equal number of "Przemyslaw" and "Piotr")
	rows = a.sendSearchQuery(ctx, t, fmt.Sprintf(`{
		"size": 100,
		"query": {
			"range": {
				"@timestamp": {
					"gte": "2024-01-01T00:00:00Z",
					"lte": "2024-01-02T00:00:00Z"
				}
			}
		},
		%s
	}`, sortByTimestamp), 50, 50)

	// "Przemyslaw" and "Piotr" share the same 50 timestamps (first 50 timestamps of the overall range)
	for _, timestamp := range elasticTimestamps[len(elasticTimestamps)-50:] {
		assert.Contains(t, rows, timestamp, "should contain latest 50 timestamps")
	}

	// Single timepoint span, LIMIT 100, should return exactly 2 documents (Przemyslaw + Piotr)
	rows = a.sendSearchQuery(ctx, t, fmt.Sprintf(`{
		"size": 2,
		"query": {
			"range": {
				"@timestamp": {
					"gte": "2024-01-01T00:00:00Z",
					"lte": "2024-01-01T00:00:00Z",
				}
			}
		},
		%s
	}`, sortByTimestamp), 1, 1)

	// 30 minute span (exclusive of end), LIMIT 100, should return exactly 60 documents (Przemyslaw every minute + Piotr every minute)
	rows = a.sendSearchQuery(ctx, t, fmt.Sprintf(`{
		"size": 100,
		"query": {
			"range": {
				"@timestamp": {
					"gte": "2024-01-01T00:00:00Z",
					"lt": "2024-01-01T00:30:00Z"
				}
			}
		},
		%s
	}`, sortByTimestamp), 30, 30)
	// "Przemyslaw" and "Piotr" share the same 30 timestamps (first 30 timestamps of the overall range)
	for _, timestamp := range elasticTimestamps[0:30] {
		assert.Contains(t, rows, timestamp, "should contain first 30 timestamps")
	}

	rows = a.sendSearchQuery(ctx, t, fmt.Sprintf(`{
		"size": 100,
		"query": {
			"range": {
				"@timestamp": {
					"gt": "2024-01-01T00:00:00Z",
					"lte": "2024-01-01T00:30:00Z"
				}
			}
		},
		%s
	}`, sortByTimestamp), 30, 30)
	// "Przemyslaw" and "Piotr" share the same 30 timestamps (30 timestamps starting from the second one of the overall range)
	for _, timestamp := range elasticTimestamps[1:31] {
		assert.Contains(t, rows, timestamp, "should contain first 30 timestamps")
	}

	// 30 minute span (inclusive of end), LIMIT 100, should return exactly 62 documents (Przemyslaw every minute + Piotr every minute: (30 - 0 + 1) * 2 = 62)
	rows = a.sendSearchQuery(ctx, t, fmt.Sprintf(`{
		"size": 100,
		"query": {
			"range": {
				"@timestamp": {
					"gte": "2024-01-01T00:00:00Z",
					"lte": "2024-01-01T00:30:00Z"
				}
			}
		},
		%s
	}`, sortByTimestamp), 31, 31)

	// 5 minute span (exclusive of end), LIMIT 100, should return exactly 10 documents (Przemyslaw every minute + Piotr every minute)
	rows = a.sendSearchQuery(ctx, t, fmt.Sprintf(`{
		"size": 100,
		"query": {
			"range": {
				"@timestamp": {
					"gte": "2024-01-01T00:00:00Z",
					"lt": "2024-01-01T00:05:00Z"
				}
			}
		},
		%s
	}`, sortByTimestamp), 5, 5)

	// 5 minute span (exclusive of end), LIMIT 6, should return exactly 6 documents (trimmed by LIMIT)
	rows = a.sendSearchQuery(ctx, t, fmt.Sprintf(`{
		"size": 6,
		"query": {
			"range": {
				"@timestamp": {
					"gte": "2024-01-01T00:00:00Z",
					"lt": "2024-01-01T00:05:00Z"
				}
			}
		},
		%s
	}`, sortByTimestamp), 3, 3)
	// LIMIT trimmed the results, so it should return 3 timestamps back from 5 minute mark
	for _, timestamp := range elasticTimestamps[5-3 : 5] {
		assert.Contains(t, rows, timestamp, "should contain 3 timestamps")
	}

	// 50 minute span (exclusive of end), LIMIT 40, should return exactly 40 documents (trimmed by LIMIT)
	rows = a.sendSearchQuery(ctx, t, fmt.Sprintf(`{
		"size": 40,
		"query": {
			"range": {
				"@timestamp": {
					"gte": "2024-01-01T00:00:00Z",
					"lt": "2024-01-01T00:50:00Z"
				}
			}
		},
		%s
	}`, sortByTimestamp), 20, 20)
	// LIMIT trimmed the results, so it should return 20 timestamps back from 50 minute mark
	for _, timestamp := range elasticTimestamps[50-20 : 50] {
		assert.Contains(t, rows, timestamp, "should contain 20 timestamps")
	}

	// 50 minute span (exclusive of end) with filter, LIMIT 40, should return exactly 40 documents (trimmed by LIMIT)
	rows = a.sendSearchQuery(ctx, t, fmt.Sprintf(`{
		"size": 40,
		"query": {
			"bool": {
				"must": [
					{
						"range": {
							"@timestamp": {
								"gte": "2024-01-01T00:00:00Z",
								"lt": "2024-01-01T00:50:00Z"
							}
						}
					},
					{
						"term": {
							"name": "Piotr"
						}
					}
				]
			}
		},
		%s
	}`, sortByTimestamp), 40, 0)
	// LIMIT trimmed the results, so it should return 40 timestamps back from 50 minute mark
	for _, timestamp := range elasticTimestamps[50-40 : 50] {
		assert.Contains(t, rows, timestamp, "should contain 40 timestamps")
	}
}

// Randomly generated query ranges
func (a *SplitTimeRangeTestcase) testTimerangesRandom(ctx context.Context, t *testing.T) {
	// Insert 2 documents for each minute between 2020-01-01 and 2020-01-02
	var bulkBody []byte
	var jsonTimestamps []string
	var elasticTimestamps []string
	for minute := 0; minute < 24*60; minute++ {
		jsonTimestamp := fmt.Sprintf("2020-01-01T%02d:%02d:00Z", minute/60, minute%60)
		jsonTimestamps = append(jsonTimestamps, jsonTimestamp)
		elasticTimestamps = append(elasticTimestamps, fmt.Sprintf("2020-01-01 %02d:%02d:00 +0000 UTC", minute/60, minute%60))

		bulkBody = append(bulkBody, []byte(fmt.Sprintf(`{ "index": { "_index": "testtable1" } }%s`, "\n"))...)
		bulkBody = append(bulkBody, []byte(fmt.Sprintf(`{"name": "Przemyslaw", "age": %d, "@timestamp": "%s"}%s`, 31337+minute, jsonTimestamp, "\n"))...)

		bulkBody = append(bulkBody, []byte(fmt.Sprintf(`{ "index": { "_index": "testtable1" } }%s`, "\n"))...)
		bulkBody = append(bulkBody, []byte(fmt.Sprintf(`{"name": "Piotr", "age": %d, "@timestamp": "%s"}%s`, 131337+minute, jsonTimestamp, "\n"))...)
	}
	resp, _ := a.RequestToQuesma(ctx, t, "POST", "/_bulk", bulkBody)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	testRangeNotFiltered := func(startTimestampIdx int, endTimestampIdx int, limit int) {
		assert.Equal(t, 0, limit%2, "Limit should be even, as we have 2 documents per minute")

		expectedCounts := min(limit/2, endTimestampIdx-startTimestampIdx)

		rows := a.sendSearchQuery(ctx, t, fmt.Sprintf(`{
			"size": %d,
			"query": {
				"range": {
					"@timestamp": {
						"gte": "%s",
						"lt": "%s"
					}
				}
			},
			%s
		}`, limit, jsonTimestamps[startTimestampIdx], jsonTimestamps[endTimestampIdx], sortByTimestamp), expectedCounts, expectedCounts)
		for i := 0; i < expectedCounts; i++ {
			assert.Contains(t, rows, elasticTimestamps[endTimestampIdx-1-i]) // - 1 because "lt" is exclusive
		}
	}
	testRangeFiltered := func(startTimestampIdx int, endTimestampIdx int, limit int) {
		expectedCounts := min(limit, endTimestampIdx-startTimestampIdx)

		rows := a.sendSearchQuery(ctx, t, fmt.Sprintf(`{
			"size": %d,
			"query": {
				"bool": {
					"must": [
						{
							"range": {
								"@timestamp": {
									"gte": "%s",
									"lt": "%s"
								}
							}
						},
						{
							"term": {
								"name": "Piotr"
							}
						}
					]
				}
			},
			%s
		}`, limit, jsonTimestamps[startTimestampIdx], jsonTimestamps[endTimestampIdx], sortByTimestamp), expectedCounts, 0)
		for i := 0; i < expectedCounts; i++ {
			assert.Contains(t, rows, elasticTimestamps[endTimestampIdx-1-i]) // - 1 because "lt" is exclusive
		}
	}

	r := rand.New(rand.NewSource(42))
	for testCaseNo := 0; testCaseNo < 200; testCaseNo++ {
		startIdx := r.Intn(len(jsonTimestamps) - 1)
		length := r.Intn(90) // maximum 90 minutes range
		endIdx := min(startIdx+length, len(jsonTimestamps)-1)

		testRangeNotFiltered(startIdx, endIdx, 6)
		testRangeNotFiltered(startIdx, endIdx, 44)
		testRangeNotFiltered(startIdx, endIdx, 10000)

		testRangeFiltered(startIdx, endIdx, 6)
		testRangeFiltered(startIdx, endIdx, 44)
		testRangeFiltered(startIdx, endIdx, 10000)
	}
}

// Helper for test functions - sends a _search query to Quesma and parses/checks the results.
func (a *SplitTimeRangeTestcase) sendSearchQuery(ctx context.Context, t *testing.T, query string, expectedPiotrCount, expectedPrzemyslawCount int) string {
	var respJson struct {
		Hits struct {
			Hits []struct {
				Source map[string]interface{} `json:"_source"`
			} `json:"hits"`
		} `json:"hits"`
	}

	resp, bodyBytes := a.RequestToQuesma(ctx, t, "GET", "/testtable1/_search", []byte(query))
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	assert.Contains(t, "Clickhouse", resp.Header.Get("X-Quesma-Source"))

	err := json.Unmarshal(bodyBytes, &respJson)
	assert.NoError(t, err)

	rows := ""
	for _, hit := range respJson.Hits.Hits {
		rows += fmt.Sprintf("%s\n", hit.Source)
	}

	assert.Equal(t, expectedPiotrCount, strings.Count(rows, "Piotr"))
	assert.Equal(t, expectedPrzemyslawCount, strings.Count(rows, "Przemyslaw"))

	return rows
}
