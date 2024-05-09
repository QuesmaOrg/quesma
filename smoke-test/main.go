package main

import (
	"bytes"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"slices"
	"strings"
	"time"

	_ "github.com/mailru/go-clickhouse"
)

const (
	clickhouseUrl            = "http://localhost:8123"
	kibanaUrl                = "http://localhost:5601"
	kibanaHealthCheckUrl     = "http://localhost:5601/api/status"
	kibanaDataViewsUrl       = "http://localhost:5601/api/data_views"
	kibanaCsvReportUrl       = "http://localhost:5601/api/reporting/generate/csv_searchsource"
	elasticsearchBaseUrl     = "http://localhost:9201"
	elasticIndexCountUrl     = "http://localhost:9201/logs-generic-default,logs-*/_count"
	quesmaIndexCountUrl      = "http://localhost:9200/logs-generic-default,logs-*/_count"
	asyncQueryUrl            = "http://localhost:8080/logs-*/_async_search?pretty"
	kibanaLogExplorerMainUrl = "http://localhost:5601/app/observability-log-explorer/?controlPanels=(data_stream.namespace:(explicitInput:(fieldName:data_stream.namespace,id:data_stream.namespace,title:Namespace),grow:!f,order:0,type:optionsListControl,width:medium))&_a=(columns:!(service.name,host.name,message),filters:!(),grid:(columns:(host.name:(width:320),service.name:(width:240))),index:BQZwpgNmDGAuCWB7AdgFQJ4AcwC4CGEEAlEA,interval:auto,query:(language:kuery,query:%27%27),rowHeight:0,sort:!(!(%27@timestamp%27,desc)))&_g=(filters:!(),refreshInterval:(pause:!t,value:60000),time:(from:now-15m,to:now))"
	kibanaLogInternalUrl     = "http://localhost:5601/internal/controls/optionsList/logs-*-*"
)

const (
	waitInterval  = 200 * time.Millisecond
	printInterval = 5 * time.Second
)

var queries = []string{`
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
}
`,
	`{
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
}`}

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

func main() {
	waitForStart := flag.Bool("wait-for-start", false, "Wait for start of whole system")

	flag.Parse()

	// check if command line flag is just wait for count
	if *waitForStart {
		fmt.Println("Waiting for start of whole system... ")
		waitForLogs(5 * time.Minute)
		fmt.Println("Done")
	} else {
		waitForKibana(5 * time.Minute)
		println("   Kibana: OK")
		waitForDataViews(5 * time.Minute)
		println("   Data Views: OK")
		reportUri := waitForScheduleReportGeneration()
		waitForLogsInClickhouse("logs-generic-default", time.Minute)
		println("   Logs in Clickhouse: OK")
		waitForAsyncQuery(time.Minute, queries)
		println("   AsyncQuery: OK")
		waitForKibanaLogExplorer("kibana LogExplorer", time.Minute)
		println("   Kibana LogExplorer: OK")
		waitForKibanaReportGeneration(reportUri, 5*time.Minute)
		println("   Kibana Report: OK")
	}
}

type dataView struct {
	Id    string `json:"id"`
	Name  string `json:"name"`
	Title string `json:"title"`
}

type dataViewsResponse struct {
	DataViews []dataView `json:"data_view"`
}

func waitForDataViews(timeout time.Duration) {
	var responseData dataViewsResponse
	res := waitFor("kibana data views", func() bool {
		if resp, err := http.Get(kibanaDataViewsUrl); err == nil {
			defer resp.Body.Close()
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				return false
			}
			if err := json.Unmarshal(body, &responseData); err != nil {
				return false
			}
			if len(responseData.DataViews) >= 5 {
				return true
			}
		}
		return false
	}, timeout)
	if !res {
		panic("kibana data views failed: " + fmt.Sprintf("%+v", responseData))
	}
}

func getClickHouseTableCount(tableName string) int {
	connection, err := sql.Open("clickhouse", clickhouseUrl)
	var rowsInClickHouse int
	if err != nil {
		panic(err)
	}
	defer connection.Close()
	row := connection.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM `%s`", tableName))
	_ = row.Scan(&rowsInClickHouse)
	return rowsInClickHouse
}

func getElasticsearchIndexCount(indexName string) int {
	if resp, err := http.Get(fmt.Sprintf("%s/%s/_count", elasticsearchBaseUrl, indexName)); err == nil {
		defer resp.Body.Close()
		if resp.StatusCode == 200 {
			if body, err := io.ReadAll(resp.Body); err == nil {
				var response map[string]int
				_ = json.Unmarshal(body, &response)
				return response["count"]
			}
		}
	}
	return -1
}

func compareClickHouseTableWithElasticsearchIndex(tableOrIndexName string, timeout time.Duration) {
	var clickHouseCount, elasticSearchCount int
	s := waitFor("Elastic/ClickHouse document count comparison", func() bool {
		clickHouseCount = getClickHouseTableCount(tableOrIndexName)
		elasticSearchCount = getElasticsearchIndexCount(tableOrIndexName)
		fmt.Printf("[%s] -> compating ClickHouse=(%d) with Elasticsearch=(%d) document count\n", tableOrIndexName, clickHouseCount, elasticSearchCount)
		return clickHouseCount == elasticSearchCount
	}, timeout)
	if !s {
		panic(fmt.Sprintf("Data set [%s] has %d elements in Clickhouse whereas in Elasticsearch it has %d", tableOrIndexName, clickHouseCount, elasticSearchCount))
	}

}

func compareKibanaSampleDataInClickHouseWithElasticsearch(timeout time.Duration) {
	// CI jobs uses LIMITED_DATASET and only this `flight` data index will get installed
	compareClickHouseTableWithElasticsearchIndex("kibana_sample_data_flights", timeout)
}

// just returns the path to the Kibana report for later download
func scheduleReportGeneration() (string, error) {
	body := `{"jobParams": "(objectType:search,searchSource:(index:'logs-generic',query:(language:kuery,query:'')))"}`
	req, _ := http.NewRequest("POST", kibanaCsvReportUrl, bytes.NewBuffer([]byte(body)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("kbn-xsrf", "custom-value")
	client := &http.Client{}
	if resp, err := client.Do(req); err == nil {
		defer resp.Body.Close()
		var responseData map[string]interface{}
		if err := json.NewDecoder(resp.Body).Decode(&responseData); err != nil {
			return "", fmt.Errorf("error decoding response")
		}
		return fmt.Sprintf("%s", responseData["path"]), nil
	}
	return "", fmt.Errorf("error scheduling report generation")
}

func waitForScheduleReportGeneration() string {
	reportUri, err := scheduleReportGeneration()
	if err != nil {
		panic("Error scheduling report generation")
	}
	return reportUri
}

func waitForKibanaReportGeneration(reportUri string, timeout time.Duration) {
	var csvReport [][]string
	res := waitFor("kibana report", func() bool {
		if resp, err := http.Get(fmt.Sprintf("%s%s", kibanaUrl, reportUri)); err != nil || resp.StatusCode != 200 {
			return false
		} else {
			defer resp.Body.Close()
			responseBody, _ := io.ReadAll(resp.Body)
			reader := csv.NewReader(strings.NewReader(string(responseBody)))
			csvReport, _ = reader.ReadAll()

			return true
		}
	}, timeout)
	if !res {
		panic("kibana report failed to generate")
	}
	csvHeader := csvReport[0]
	if slices.Contains(csvHeader, "@timestamp") {
		fmt.Printf("Report generation successful")
	} else {
		panic("Report doesn't have proper header")
	}
	if entriesCount := len(csvReport); entriesCount < 10 {
		panic(fmt.Sprintf("Report contains only %d lines", entriesCount))
	} else {
		fmt.Printf("Report generation successful, %d entries exported to CSV\n", entriesCount-1)
	}

}

func waitFor(serviceName string, waitForFunc func() bool, timeout time.Duration) bool {
	startTime := time.Now()
	lastPrintTime := startTime

	for time.Since(startTime) < timeout {
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

func waitForLogsInClickhouse(tableName string, timeout time.Duration) {
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
	}, timeout)

	if !res {
		panic("no logs in clickhouse")
	}
}

func waitForKibana(timeout time.Duration) {
	res := waitFor("kibana", func() bool {
		resp, err := http.Get(kibanaHealthCheckUrl)
		if err == nil {
			if resp.StatusCode == 200 {
				return true
			} else {
				fmt.Printf("kibana response: %+v\n", resp)
			}
		}
		return false
	}, timeout)

	if !res {
		panic("kibana is not alive")
	}
}

func waitForLogsInElasticsearch(timeout time.Duration) {
	waitForLogsInElasticsearchRaw("elasticsearch", elasticIndexCountUrl, false, timeout)
}

func waitForLogs(timeout time.Duration) {
	waitForLogsInElasticsearchRaw("quesma", quesmaIndexCountUrl, true, timeout)
}

func waitForLogsInElasticsearchRaw(serviceName, url string, quesmaSource bool, timeout time.Duration) {
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
						if quesmaSource {
							return sourceClickhouse(resp)
						} else {
							return !sourceClickhouse(resp)
						}
					}
				}
			} else {
				fmt.Printf("%s response: %+v\n", serviceName, resp)
			}
		}
		return false
	}, timeout)

	if !res {
		panic(serviceName + " is not alive or is not receiving logs")
	}
}

func checkLogs() {
	content, err := os.ReadFile("/var/quesma/logs/quesma.log")
	if err != nil {
		fmt.Println("Error reading file:", err)
		return
	}

	fileContent := string(content)
	searchString := "Panic recovered:"

	if bytes.Contains([]byte(fileContent), []byte(searchString)) {
		panic("Panic recovered in quesma.log")
	}
}

func waitForAsyncQuery(timeout time.Duration, queries []string) {
	serviceName := "async query"
	for _, query := range queries {
		res := waitFor(serviceName, func() bool {
			resp, err := http.Post(asyncQueryUrl, "application/json", bytes.NewBuffer([]byte(query)))

			if err == nil {
				defer resp.Body.Close()
				if resp.StatusCode == 200 {
					body, err := io.ReadAll(resp.Body)
					if err == nil {
						var response map[string]interface{}
						_ = json.Unmarshal(body, &response)

						if response["completion_time_in_millis"] != nil {
							if sourceClickhouse(resp) {
								return true
							} else {
								panic("invalid X-Quesma-Source header value")
							}
						}
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

func headerExists(headers http.Header, key string, value string) bool {
	for _, val := range headers[key] {
		if val == value {
			return true
		}
	}
	return false
}

func sourceClickhouse(resp *http.Response) bool {
	return headerExists(resp.Header, "X-Quesma-Source", "Clickhouse")
}

func waitForKibanaLogExplorer(serviceName string, timeout time.Duration) {
	res := waitFor(serviceName, func() bool {
		return sendKibanaRequest(kibanaLogInternalUrl, "POST", kibanaLogExplorerMainUrl, kibanaInternalLog)
	}, timeout)
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
		req.Header.Set("Referer", referrer)
		req.Header.Set("kbn-xsrf", "reporting")
		req.Header.Set("Elastic-Api-Version", "1")
	}
	client := http.Client{}
	response, err := client.Do(req)
	if err != nil {
		fmt.Println("Error sending request:", err)
		return false
	}
	defer response.Body.Close()

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
