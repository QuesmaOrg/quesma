package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

const partialAsyncQueryURL = "http://localhost:8080/_async_search/"
const partialAsyncQueryURLParams = "?wait_for_completion_timeout=200ms&keep_alive=60000ms"
const url = "http://localhost:8080/github_events/_async_search?batched_reduce_size=64&ccs_minimize_roundtrips=true&wait_for_completion_timeout=200ms&keep_on_completion=true&keep_alive=60000ms&ignore_unavailable=true&preference=1710926987635"

func sendRequest(url string, jsonStr []byte) ([]byte, error) {
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonStr))
	if err != nil {
		fmt.Println("Error creating request:", err)
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json")

	// Send the request
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fmt.Println("Error sending request:", err)
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		fmt.Println("Error reading response body:", err)
		return nil, err
	}
	return body, nil
}

func getKey[T string | bool | int](body []byte, key string) (T, error) {
	var value T
	var data map[string]interface{}
	err := json.Unmarshal(body, &data)
	if err != nil {
		fmt.Println("Error:", err)
		return value, err
	}
	if val, ok := data[key]; ok {
		return val.(T), nil
	}
	return value, errors.New("key not found")
}

func getAsyncId(body []byte) (string, error) {
	return getKey[string](body, "id")
}

func getIsRunning(body []byte) (bool, error) {
	return getKey[bool](body, "is_running")
}

func getCompletionStatus(body []byte) (int, error) {
	return getKey[int](body, "completion_status")
}

func getFilterAsyncQuery(from time.Time, to time.Time) []byte {
	return []byte(`
{
    "_source": false,
    "fields": [
        {
            "field": "*",
            "include_unmapped": "true"
        },
        {
            "field": "closed_at",
            "format": "strict_date_optional_time"
        },
        {
            "field": "created_at",
            "format": "strict_date_optional_time"
        },
        {
            "field": "file_time",
            "format": "strict_date_optional_time"
        },
        {
            "field": "merged_at",
            "format": "strict_date_optional_time"
        },
        {
            "field": "updated_at",
            "format": "strict_date_optional_time"
        }
    ],
    "highlight": {
        "fields": {
            "*": {}
        },
        "fragment_size": 2147483647,
        "post_tags": [
            "@/kibana-highlighted-field@"
        ],
        "pre_tags": [
            "@kibana-highlighted-field@"
        ]
    },
    "query": {
        "bool": {
            "filter": [
                {
                    "range": {
                        "created_at": {
                            "format": "strict_date_optional_time",
                            "gte": ` + `"` + from.Format("2006-01-02T15:04:05.803Z") + `"` + `,
                            "lte": ` + `"` + to.Format("2006-01-02T15:04:05.803Z") + `"` + `
                        }
                    }
                }
            ],
            "must": [],
            "must_not": [],
            "should": []
        }
    },
    "runtime_mappings": {},
    "script_fields": {},
    "size": 500,
    "sort": [
        {
            "created_at": {
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
    "stored_fields": [
        "*"
    ],
    "track_total_hits": false,
    "version": true
}`)
}

func getHistogramAsyncQuery(from time.Time, to time.Time) []byte {
	return []byte(`
{
    "_source": {
        "excludes": []
    },
    "aggs": {
        "0": {
            "date_histogram": {
                "field": "created_at",
                "fixed_interval": "12h",
                "min_doc_count": 1,
                "time_zone": "Europe/Warsaw"
            }
        }
    },
    "fields": [
        {
            "field": "closed_at",
            "format": "date_time"
        },
        {
            "field": "created_at",
            "format": "date_time"
        },
        {
            "field": "file_time",
            "format": "date_time"
        },
        {
            "field": "merged_at",
            "format": "date_time"
        },
        {
            "field": "updated_at",
            "format": "date_time"
        }
    ],
    "query": {
        "bool": {
            "filter": [
                {
                    "range": {
                        "created_at": {
                            "format": "strict_date_optional_time",
                            "gte": ` + `"` + from.Format("2006-01-02T15:04:05.803Z") + `"` + `,
                            "lte": ` + `"` + to.Format("2006-01-02T15:04:05.803Z") + `"` + `
}
                    }
                }
            ],
            "must": [],
            "must_not": [],
            "should": []
        }
    },
    "runtime_mappings": {},
    "script_fields": {},
    "size": 0,
    "stored_fields": [
        "*"
    ],
    "track_total_hits": true
}`)
}

type queryStats struct {
	total      atomic.Int64
	successful atomic.Int64
	timeOuted  atomic.Int64
}

func makePartialAsyncQuery(id string) ([]byte, error) {
	url := partialAsyncQueryURL + id + partialAsyncQueryURLParams
	fmt.Println(url)
	body, err := sendRequest(url, []byte{})
	return body, err
}

func runPartialAsyncQuery(id string, stats *queryStats) {
	const timeout = 15 * time.Minute
	startTime := time.Now()
	stats.total.Add(1)
	for {
		body, err := makePartialAsyncQuery(id)
		if err != nil {
			fmt.Println("Error:", err)
		} else {
			fmt.Printf("Received: %d bytes for async query id : %s\n", len(body), id)
		}
		isRunning, _ := getIsRunning(body)
		if isRunning == false {
			completionStatus, _ := getCompletionStatus(body)
			if completionStatus == 200 {
				stats.successful.Add(1)
			}
			break
		}
		elapsed := time.Since(startTime)
		if elapsed > timeout {
			stats.timeOuted.Add(1)
			break
		}
		time.Sleep(1 * time.Second)
	}
}

func runAsyncSearchLoadTestInstance(from time.Time, to time.Time, stats *queryStats) {
	id1 := ""
	id2 := ""
	body, err := sendRequest(url, getFilterAsyncQuery(from, to))
	if err == nil {
		id1, _ = getAsyncId(body)
		fmt.Println("ID:", id1)
	}
	body, err = sendRequest(url, getHistogramAsyncQuery(from, to))
	if err == nil {
		id2, _ = getAsyncId(body)
		fmt.Println("ID:", id2)
	}
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		runPartialAsyncQuery(id1, stats)
		wg.Done()
	}()
	go func() {
		runPartialAsyncQuery(id2, stats)
		wg.Done()
	}()
	wg.Wait()
}

func runAsyncSearchLoadTests(numberOfIterations int, numberOfConcurrentRequests int, from time.Time, to time.Time) {
	stats := queryStats{}
	for i := 0; i < numberOfIterations; i++ {
		var concurrentRequests sync.WaitGroup
		concurrentRequests.Add(numberOfConcurrentRequests)
		for i := 0; i < numberOfConcurrentRequests; i++ {
			go func() {
				runAsyncSearchLoadTestInstance(from, to, &stats)
				concurrentRequests.Done()
			}()
		}
		concurrentRequests.Wait()
		fmt.Println("Stats:")
		fmt.Println("\tTotal:", stats.total.Load())
		fmt.Println("\tSuccessful:", stats.successful.Load())
		fmt.Println("\tTimeOuted:", stats.timeOuted.Load())
	}
}
