// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/qri-io/jsonpointer"
	"io"
	"net/http"
	"reflect"
	"strings"
	"time"
)

type testQuery struct {
	name     string
	category string
	body     string
	validate func(map[string]interface{}) bool
}

var sampleQueries = []testQuery{
	{
		name:     "Explore query",
		category: "simple",
		body: `
{
	"_source": false,
	"fields": [
		{
			"field": "*",
			"include_unmapped": "true"
		},
		{
			"field": "@timestamp",
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
					"multi_match": {
						"lenient": true,
						"query": "user",
						"type": "best_fields"
					}
				},
				{
					"range": {
						"@timestamp": {
							"format": "strict_date_optional_time",
							"gte": "now-1d",
							"lte": "now-1s"
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
	"stored_fields": [
		"*"
	],
	"track_total_hits": false,
	"version": true
}`,
		validate: func(response map[string]interface{}) bool {
			return ensureSomeHits(response)
		},
	},
	{
		name:     "Histogram in explore",
		category: "aggregate",
		body: `{
    "_source": {
        "excludes": []
    },
    "aggs": {
        "0": {
            "date_histogram": {
                "field": "@timestamp",
                "fixed_interval": "30s",
                "min_doc_count": 1,
                "time_zone": "Europe/Warsaw"
            }
        }
    },
    "fields": [
        {
            "field": "@timestamp",
            "format": "date_time"
        }
    ],
    "query": {
        "bool": {
            "filter": [
                {
                    "range": {
                        "@timestamp": {
                            "format": "strict_date_optional_time",
                            "gte": "now-1d",
                            "lte": "now-1s"
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
}`,
		validate: func(m map[string]interface{}) bool {
			return true
		},
	},
	{
		name:     "Facets aggregation, checking field types",
		category: "aggregate",
		body: `{
    "aggs": {
        "sample": {
            "aggs": {
                "sample_count": {
                    "value_count": {
                        "field": "service.name"
                    }
                },
                "top_values": {
                    "terms": {
                        "field": "service.name",
                        "shard_size": 25,
                        "size": 10
                    }
                }
            },
            "sampler": {
                "shard_size": 5000
            }
        }
    },
    "query": {
        "bool": {
            "filter": [
                {
                    "range": {
                        "@timestamp": {
                            "format": "strict_date_optional_time",
                            "gte": "now-1d",
                            "lte": "now-1s"
                        }
                    }
                },
                {
                    "bool": {
                        "filter": [],
                        "must": [],
                        "must_not": [],
                        "should": []
                    }
                }
            ]
        }
    },
    "runtime_mappings": {},
    "size": 0,
    "track_total_hits": true
}`,
		validate: func(response map[string]interface{}) bool {
			return checkTypeExpectation("float64", "/response/aggregations/sample/top_values/buckets/0/doc_count", response) &&
				checkTypeExpectation("string", "/response/aggregations/sample/top_values/buckets/0/key", response)
		},
	},
}

func checkTypeExpectation(expectedType string, path string, response map[string]interface{}) bool {
	ptr, err := jsonpointer.Parse(path)
	if err != nil {
		fmt.Println(err)
		return false
	}
	value, err := ptr.Eval(response)
	if err != nil {
		fmt.Println(err)
		return false
	}
	valueType := reflect.TypeOf(value)

	if valueType == nil {
		fmt.Println("Expected a value, got nil. Path", path)
		return false
	}

	// Check if the type is int
	if valueType.Kind().String() != expectedType {
		fmt.Printf("Expected %s, got %s. Path: %s\n", expectedType, valueType.Kind().String(), path)
		return false
	}
	return true

}

type asyncQueryType struct {
	Id string `json:"id"`
}

func waitForAsyncQuery(timeout time.Duration) {
	serviceName := "async query: "
	for _, query := range sampleQueries {
		var body []byte
		res := waitFor(serviceName+query.name, func() bool {
			resp, err := http.Post(asyncQueryUrl, "application/json", bytes.NewBuffer([]byte(query.body)))

			if err == nil {
				defer resp.Body.Close()
				if resp.StatusCode == 200 {
					body, err = io.ReadAll(resp.Body)
					if err != nil {
						fmt.Println("Failed to read the body", err)
						panic("can't read response body")
					}
					return validateResponse(query, resp, body)
				}
			}
			return false
		}, timeout)

		if !res {
			panic(serviceName + " is not alive or is not receiving logs")
		}

		var asyncQuery asyncQueryType
		err := json.Unmarshal(body, &asyncQuery)
		if err != nil {
			fmt.Println("Parsing JSON out of _async_search failed", err)
			panic("can't parse async query response")
		}

		resp, err := http.Get(asyncGetQueryUrlPrefix + asyncQuery.Id)
		if err != nil {
			fmt.Println("Getting _async_status failed", err)
			panic("can't get async query status")
		}
		defer resp.Body.Close()
		if resp.StatusCode != 200 {
			fmt.Printf("async query status is %d %s\n", resp.StatusCode, asyncGetQueryUrlPrefix+asyncQuery.Id)
			panic("async query status is not 200")
		}
	}
	checkLogs()
}

func parseHits(jsonBody map[string]interface{}) []interface{} {
	if response, ok := jsonBody["response"].(map[string]interface{}); ok {
		if hits, ok := response["hits"].(map[string]interface{}); ok {
			if hitList, ok := hits["hits"].([]interface{}); ok {
				return hitList
			}
		}
	}
	// If unable to parse hits, print the JSON body and panic
	body, _ := json.MarshalIndent(jsonBody, " ", " ")
	panic("Can't parse response: hits: hits")
}

type logJson struct {
	ID        string  `json:"_id"`
	Index     string  `json:"_index"`
	Score     float64 `json:"_score"`
	Version   int     `json:"_version"`
	Fields    fields  `json:"fields"`
	Highlight struct {
		HostName []interface{} `json:"host.name"`
	} `json:"highlight"`
	Sort []interface{} `json:"sort"`
}

type fields struct {
	Timestamp   []string `json:"@timestamp"`
	HostName    []string `json:"host.name"`
	Message     []string `json:"message"`
	ServiceName []string `json:"service.name"`
	Severity    []string `json:"severity"`
	Source      []string `json:"source"`
}

func isValidSeverity(severity string) bool {
	severityNames := []string{"info", "warning", "error", "critical", "debug"}
	for _, name := range severityNames {
		if severity == name {
			return true
		}
	}
	return false
}

func validateLog(log logJson) error {
	if len(log.Fields.Timestamp) != 1 {
		return errors.New("no one timestamp in log")
	}
	_, err := time.Parse("2006-01-02T15:04:05.999Z", log.Fields.Timestamp[0])
	if err != nil {
		return err
	}
	if len(log.Fields.HostName) != 1 {
		return errors.New("no one host.name in log")
	}
	if len(log.Fields.Severity) != 1 {
		return errors.New("no one severity in log")
	}
	if !isValidSeverity(log.Fields.Severity[0]) {
		return errors.New("invalid severity in log " + log.Fields.Severity[0])
	}
	if len(log.Fields.Message) != 1 {
		return errors.New("no one message in log")
	}
	if !strings.HasPrefix(log.Fields.Message[0], "User ") {
		return errors.New("all message should start with `User `")
	}
	if len(log.Fields.ServiceName) != 1 {
		return errors.New("no one service.name in log")
	}

	return nil
}

func ensureSomeHits(jsonBody map[string]interface{}) bool {
	fmt.Println("ensureSomeHits async response", jsonBody)
	hits := parseHits(jsonBody)

	if len(hits) == 0 {
		body, _ := json.Marshal(jsonBody)
		fmt.Println("async invalid hit format" + string(body))
		panic("no hits in response")
	}

	for _, hit := range hits {
		var log logJson
		jsonHit, _ := json.Marshal(hit)
		if err := json.Unmarshal(jsonHit, &log); err != nil {
			fmt.Println("async invalid hit format", string(jsonHit))
			panic(err)
		}
		if err := validateLog(log); err != nil {
			fmt.Println("async invalid hit format", string(jsonHit))
			panic(err)
		}
	}

	return true
}

func validateResponse(query testQuery, resp *http.Response, body []byte) bool {
	var response map[string]interface{}
	_ = json.Unmarshal(body, &response)

	if response["completion_time_in_millis"] != nil {
		if !sourceClickhouse(resp) {
			panic("invalid X-Quesma-Source header value")
		}
		return query.validate(response)
	}
	return true
}
