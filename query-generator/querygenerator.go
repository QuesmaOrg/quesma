package main

import (
	"bytes"
	"io"
	"log"
	"math/rand"
	"net/http"
	"time"
)

func main() {
	sync_url := "http://mitmproxy:8080/_search?pretty"
	async_url := "http://mitmproxy:8080/logs-*-*/_async_search?pretty"
	queries := []string{
		`{
			"query": {
			  "match_all": {}
			}
		 }`,
		`{
			"size": 0,
			"timeout": "1000ms",
			"terminate_after": 100000,
			"query": {
			  "bool": {
				"filter": [
				  {
					"bool": {
					  "must": [],
					  "filter": [
						{
						  "multi_match": {
							"type": "best_fields",
							"query": "user",
							"lenient": true
						  }
						},
						{
						  "range": {
							"@timestamp": {
							  "format": "strict_date_optional_time",
							  "gte": "2024-01-22T09:26:10.299Z",
							  "lte": "2024-12-22T09:41:10.299Z"
							}
						  }
						}
					  ],
					  "should": [],
					  "must_not": []
					}
				  }
				]
			  }
			},
			"aggs": {
			  "suggestions": {
				"terms": {
				  "size": 10,
				  "field": "data_stream.namespace",
				  "shard_size": 10,
				  "order": {
					"_count": "desc"
				  }
				}
			  },
			  "unique_terms": {
				"cardinality": {
				  "field": "data_stream.namespace"
				}
			  }
			},
			"runtime_mappings": {}
		}`,
		`{
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
									"gte": "2024-01-23T14:43:19.481Z",
									"lte": "2024-01-23T14:58:19.481Z"
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
	}
	queryIndex := 0

	for {
		time.Sleep(time.Duration(1000+rand.Intn(2000)) * time.Millisecond)

		var url string
		// async search every 3rd query
		if queryIndex == len(queries)-1 {
			url = async_url
		} else {
			url = sync_url
		}
		sendQuery(url, queries[queryIndex])

		queryIndex++
		if queryIndex > len(queries)-1 {
			queryIndex = 0
		}
	}
}

func sendQuery(url string, query string) {
	resp, err := http.Post(url, "application/json", bytes.NewBuffer([]byte(query)))

	if err != nil {
		log.Fatal(err)
	}
	_, err = io.ReadAll(resp.Body)
	if err != nil {
		log.Println(err)
	}

	resp.Body.Close()

}
