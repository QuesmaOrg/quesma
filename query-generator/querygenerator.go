package main

import (
	"bytes"
	"io"
	"log"
	"math/rand"
	"net/http"
	"time"
)

const url = "http://mitmproxy:8080/_search?pretty"

func main() {
	queryIndex := 0
	for {
		time.Sleep(time.Duration(1000+rand.Intn(2000)) * time.Millisecond)
		dummyQuery := `
		{
			"query": {
			  "match_all": {}
			}
		 }
		`
		userQuery := `
		{
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
		  }
		`
		var body []byte
		if queryIndex%2 == 0 {
			body = []byte(dummyQuery)
		} else {
			body = []byte(userQuery)
		}

		resp, err := http.Post(url, "application/json", bytes.NewBuffer(body))

		if err != nil {
			log.Fatal(err)
		}
		_, err = io.ReadAll(resp.Body)
		if err != nil {
			log.Println(err)
		}

		resp.Body.Close()
		queryIndex++
	}
}
