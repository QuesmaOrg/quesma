package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"
)

type testQuery struct {
	name     string
	category string
	body     string
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
	},
}

func waitForAsyncQuery(timeout time.Duration) {
	serviceName := "async query: "
	for _, query := range sampleQueries {
		res := waitFor(serviceName+query.name, func() bool {
			resp, err := http.Post(asyncQueryUrl, "application/json", bytes.NewBuffer([]byte(query.body)))

			if err == nil {
				defer resp.Body.Close()
				if resp.StatusCode == 200 {
					body, err := io.ReadAll(resp.Body)
					if err == nil {
						return validateResponse(query, resp, body)
					} else {
						log.Println(err)
					}
				}
			}
			return false
		}, timeout)

		if !res {
			panic(serviceName + " is not alive or is not receiving logs")
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
	fmt.Println("async body", string(body))
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
		if query.category == "simple" {
			return ensureSomeHits(response)
		} else {
			return true
		}
	}
	return true
}
