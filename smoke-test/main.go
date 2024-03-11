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
	"slices"
	"strings"
	"time"

	_ "github.com/mailru/go-clickhouse"
)

const (
	clickhouseUrl              = "http://localhost:8123"
	kibanaUrl                  = "http://localhost:5601"
	kibanaHealthCheckUrl       = "http://localhost:5601/api/status"
	kibanaDataViewsUrl         = "http://localhost:5601/api/data_views"
	kibanaCsvReportUrl         = "http://localhost:5601/internal/reporting/generate/csv_searchsource"
	elasticsearchSampleDataUrl = "http://localhost:9201/kibana_sample_data_flights/_count"
	quesmaSampleDataUrl        = "http://localhost:9200/kibana_sample_data_flights/_count"
	elasticsearchBaseUrl       = "http://localhost:9201"
	elasticIndexCountUrl       = "http://localhost:9201/logs-generic-default/_count"
	quesmaIndexCountUrl        = "http://localhost:9200/logs-generic-default/_count"
	asyncQueryUrl              = "http://localhost:8080/logs-*/_async_search?pretty"
	kibanaLogExplorerMainUrl   = "http://localhost:5601/app/observability-log-explorer/?controlPanels=(data_stream.namespace:(explicitInput:(fieldName:data_stream.namespace,id:data_stream.namespace,title:Namespace),grow:!f,order:0,type:optionsListControl,width:medium))&_a=(columns:!(service.name,host.name,message),filters:!(),grid:(columns:(host.name:(width:320),service.name:(width:240))),index:BQZwpgNmDGAuCWB7AdgFQJ4AcwC4CGEEAlEA,interval:auto,query:(language:kuery,query:%27%27),rowHeight:0,sort:!(!(%27@timestamp%27,desc)))&_g=(filters:!(),refreshInterval:(pause:!t,value:60000),time:(from:now-15m,to:now))"
	kibanaLogInternalUrl       = "http://localhost:5601/internal/controls/optionsList/logs-*-*"
)

const (
	waitInterval  = 200 * time.Millisecond
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
		waitForKibana()
		timeoutAfter = 4 * time.Minute
		waitForSampleData()
		timeoutAfter = time.Minute
		reportUri := waitForScheduleReportGeneration()
		waitForLogsInClickhouse("logs-generic-default")
		waitForLogsInElasticsearch()
		waitForAsyncQuery()
		waitForKibanaLogExplorer("kibana LogExplorer")
		waitForKibanaReportGeneration(reportUri)
		compareKibanaSampleDataInClickHouseWithElasticsearch()
	}
}

func waitForSampleData() {
	waitForLogsInElasticsearchRaw("elasticsearch sample data", quesmaSampleDataUrl, true)
	waitForDataViews()
}

type dataView struct {
	Id    string `json:"id"`
	Name  string `json:"name"`
	Title string `json:"title"`
}

type dataViewsResponse struct {
	DataViews []dataView `json:"data_view"`
}

func waitForDataViews() {
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
	})
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

func compareClickHouseTableWithElasticsearchIndex(tableOrIndexName string) {
	var clickHouseCount, elasticSearchCount int
	s := waitFor("Elastic/ClickHouse document count comparison", func() bool {
		clickHouseCount = getClickHouseTableCount(tableOrIndexName)
		elasticSearchCount = getElasticsearchIndexCount(tableOrIndexName)
		fmt.Printf("[%s] -> compating ClickHouse=(%d) with Elasticsearch=(%d) document count\n", tableOrIndexName, clickHouseCount, elasticSearchCount)
		return clickHouseCount == elasticSearchCount
	})
	if !s {
		panic(fmt.Sprintf("Data set [%s] has %d elements in Clickhouse whereas in Elasticsearch it has %d", tableOrIndexName, clickHouseCount, elasticSearchCount))
	}

}

func compareKibanaSampleDataInClickHouseWithElasticsearch() {
	// CI jobs uses LIMITED_DATASET and only this `flight` data index will get installed
	compareClickHouseTableWithElasticsearchIndex("kibana_sample_data_flights")
}

// just returns the path to the Kibana report for later download
func scheduleReportGeneration() (string, error) {
	body := `{"jobParams":"(browserTimezone:Europe/Warsaw,columns:!(),objectType:search,searchSource:(fields:!((field:'*',include_unmapped:true)),filter:!((meta:(field:'@timestamp',index:logs-generic,params:()),query:(range:('@timestamp':(format:strict_date_optional_time,gte:now-1d,lte:now)))),(meta:(field:'@timestamp',index:logs-generic,params:()),query:(range:('@timestamp':(format:strict_date_optional_time,gte:now-1d,lte:now)))),(meta:(field:'@timestamp',index:logs-generic,params:()),query:(range:('@timestamp':(format:strict_date_optional_time,gte:now-1d,lte:now))))),index:logs-generic,query:(language:kuery,query:''),sort:!(('@timestamp':(format:strict_date_optional_time,order:desc)))),title:'Untitled discover search')"}`
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

func waitForKibanaReportGeneration(reportUri string) {
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
	})
	if !res {
		panic("kibana report failed to generate")
	}
	csvHeader := csvReport[0]
	if slices.Contains(csvHeader, "@timestamp") && slices.Contains(csvHeader, "message") && slices.Contains(csvHeader, "severity") {
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
				fmt.Printf("kibana response: %+v\n", resp)
			}
		}
		return false
	})

	if !res {
		panic("kibana is not alive")
	}
}

func waitForLogsInElasticsearch() {
	waitForLogsInElasticsearchRaw("elasticsearch", elasticIndexCountUrl, false)
}

func waitForLogs() {
	waitForLogsInElasticsearchRaw("quesma", quesmaIndexCountUrl, true)
}

func waitForLogsInElasticsearchRaw(serviceName, url string, quesmaSource bool) {
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
	})

	if !res {
		panic(serviceName + " is not alive or is not receiving logs")
	}
}

func waitForAsyncQuery() {
	serviceName := "async query"
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
	})

	if !res {
		panic(serviceName + " is not alive or is not receiving logs")
	}
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

func waitForKibanaLogExplorer(serviceName string) {
	res := waitFor(serviceName, func() bool {
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
