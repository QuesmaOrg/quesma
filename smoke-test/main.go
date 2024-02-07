package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	_ "github.com/mailru/go-clickhouse"
)

const (
	clickhouseUrl            = "http://localhost:8123"
	kibanaHealthCheckUrl     = "http://localhost:5601/api/status"
	elasticIndexCountUrl     = "http://localhost:9201/logs-generic-default/_count"
	quesmaIndexCountUrl      = "http://localhost:9200/logs-generic-default/_count"
	asyncQueryUrl            = "http://localhost:8080/logs-*-*/_async_search?pretty"
	kibanaLogExplorerMainUrl = "http://localhost:5601/app/observability-log-explorer/?controlPanels=(data_stream.namespace:(explicitInput:(fieldName:data_stream.namespace,id:data_stream.namespace,title:Namespace),grow:!f,order:0,type:optionsListControl,width:medium))&_a=(columns:!(service.name,host.name,message),filters:!(),grid:(columns:(host.name:(width:320),service.name:(width:240))),index:BQZwpgNmDGAuCWB7AdgFQJ4AcwC4CGEEAlEA,interval:auto,query:(language:kuery,query:%27%27),rowHeight:0,sort:!(!(%27@timestamp%27,desc)))&_g=(filters:!(),refreshInterval:(pause:!t,value:60000),time:(from:now-15m,to:now))"
	kibanaLogInternalUrl     = "http://localhost:5601/internal/controls/optionsList/logs-*-*"
)

const (
	waitInterval  = 100 * time.Millisecond
	printInterval = 5 * time.Second
)

const query = `
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
}
`

const kibanaInternalLog = `
{
   "size":10,
   "allowExpensiveQueries":true,
   "searchString":"",
   "filters":[
      {
         "bool":{
            "must":[

            ],
            "filter":[
               {
                  "range":{
                     "@timestamp":{
                        "format":"strict_date_optional_time",
                        "gte":"2024-02-07T13:31:07.243Z",
                        "lte":"2024-02-07T13:46:07.243Z"
                     }
                  }
               }
            ],
            "should":[

            ],
            "must_not":[

            ]
         }
      }
   ],
   "fieldName":"data_stream.namespace",
   "fieldSpec":{
      "count":0,
      "name":"data_stream.namespace",
      "type":"string",
      "esTypes":[
         "keyword"
      ],
      "scripted":false,
      "searchable":true,
      "aggregatable":true,
      "readFromDocValues":true,
      "shortDotsEnable":false,
      "isMapped":true
   },
   "runtimeFieldMap":{
      
   }
}
`

var timeoutAfter = time.Minute

func main() {
	waitForStart := flag.Bool("wait-for-start", false, "Wait for start of whole system")

	flag.Parse()

	// check if command line flag is just wait for count
	if *waitForStart {
		fmt.Println("Waiting for start of whole system... ")
		timeoutAfter = 5 * time.Minute
		waitForLogs()
		fmt.Println("Done")
	} else {
		waitForLogsInClickhouse("logs-generic-default")
		waitForLogsInClickhouse("device_logs")
		waitForLogsInElasticsearch()
		waitForKibana()
		waitForAsyncQuery()
		waitForKibanaLogExplorer("kibana")
	}
}

func waitFor(serviceName string, waitForFunc func() bool) bool {
	startTime := time.Now()
	lastPrintTime := startTime

	for time.Since(startTime) < timeoutAfter {
		if waitForFunc() {
			return true
		}

		if time.Since(lastPrintTime) > printInterval {
			elapsed := time.Since(startTime)
			elapsed = elapsed - (elapsed % time.Second) // round it to seconds
			fmt.Printf("smoke-test: elapsed %v, keep trying %s again...\n", elapsed, serviceName)
			lastPrintTime = time.Now()
		}
		time.Sleep(waitInterval)
	}

	return false
}

func waitForLogsInClickhouse(tableName string) {
	res := waitFor("clickhouse", func() bool {
		logCount := -1
		connection, err := sql.Open("clickhouse", clickhouseUrl)
		if err != nil {
			panic(err)
		}
		defer connection.Close()

		row := connection.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM `%s`", tableName))
		_ = row.Scan(&logCount)

		return logCount > 0
	})

	if !res {
		panic("no logs in clickhouse")
	}
}

func waitForKibana() {
	res := waitFor("kibana", func() bool {
		resp, err := http.Get(kibanaHealthCheckUrl)
		if err == nil {
			if resp.StatusCode == 200 {
				return true
			} else {
				fmt.Printf("response: %+v\n", resp)
			}
		}
		return false
	})

	if !res {
		panic("kibana is not alive")
	}
}

func waitForLogsInElasticsearch() {
	waitForLogsInElasticsearchRaw("elasticsearch", elasticIndexCountUrl)
}

func waitForLogs() {
	waitForLogsInElasticsearchRaw("quesma", quesmaIndexCountUrl)
}

func waitForAsyncQuery() {
	waitForAsyncQueryRaw("async query", asyncQueryUrl)
}

func waitForLogsInElasticsearchRaw(serviceName, url string) {
	res := waitFor(serviceName, func() bool {
		resp, err := http.Get(url)
		if err == nil {
			defer resp.Body.Close()
			if resp.StatusCode == 200 {
				body, err := io.ReadAll(resp.Body)
				if err == nil {
					var response map[string]int
					_ = json.Unmarshal(body, &response)
					var foo = response["count"]
					if foo > 0 {
						return true
					}
				}
			} else {
				fmt.Printf("response: %+v\n", resp)
			}
		}
		return false
	})

	if !res {
		panic(serviceName + " is not alive or is not receiving logs")
	}
}

func waitForAsyncQueryRaw(serviceName, url string) {
	res := waitFor(serviceName, func() bool {
		resp, err := http.Post(url, "application/json", bytes.NewBuffer([]byte(query)))

		if err == nil {
			defer resp.Body.Close()
			if resp.StatusCode == 200 {
				body, err := io.ReadAll(resp.Body)
				if err == nil {
					var response map[string]interface{}
					_ = json.Unmarshal(body, &response)
					if response["completion_time_in_millis"] != nil {
						return true
					}
				} else {
					log.Println(err)
				}
			}
		}
		return false
	})

	if !res {
		panic(serviceName + " is not alive or is not receiving logs")
	}
}

func waitForKibanaLogExplorer(serviceName string) {
	res := waitFor("kibana", func() bool {
		return sendKibanaRequest(kibanaLogInternalUrl, "POST", kibanaLogExplorerMainUrl, kibanaInternalLog)
	})
	if !res {
		panic(serviceName + " is not alive or is not receiving logs")
	}
}

func sendKibanaRequest(url string, method string, referrer, query string) bool {

	req, err := http.NewRequest(method, url, bytes.NewBuffer([]byte(query)))
	if err != nil {
		fmt.Println("Error creating request:", err)
		return false
	}

	if referrer != "" {
		// Set the Referer header
		req.Header.Set("Referer", referrer)
		req.Header.Set("kbn-xsrf", "reporting")
		req.Header.Set("Elastic-Api-Version", "1")
	}
	// Send the HTTP request
	client := http.Client{}
	response, err := client.Do(req)
	if err != nil {
		fmt.Println("Error sending request:", err)
		return false
	}
	defer response.Body.Close()

	// Read response body
	body, err := io.ReadAll(response.Body)
	if err != nil {
		fmt.Println("Error reading response:", err)
		return false
	}

	bodyStr := string(body)
	if strings.Contains(bodyStr, "\"statusCode\":500,\"error\":\"Internal Server Error\"") {
		return false
	}
	return true
}
