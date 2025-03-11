// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"slices"
	"sort"
	"strings"
	"time"

	_ "github.com/mailru/go-clickhouse/v2"
)

const (
	clickhouseUrl          = "http://localhost:8123"
	kibanaHealthCheckUrl   = "http://localhost:5601/api/status"
	kibanaDataViewsUrl     = "http://localhost:5601/api/data_views"
	elasticsearchBaseUrl   = "http://localhost:9201"
	elasticIndexCountUrl   = "http://localhost:9201/logs-generic-default,logs-*/_count"
	quesmaIndexCountUrl    = "http://localhost:9200/logs-generic-default,logs-*/_count"
	asyncQueryUrl          = "http://localhost:8080/logs-*/_async_search?pretty&keep_on_completion=true"
	asyncGetQueryUrlPrefix = "http://localhost:8080/_async_search/"

	kibanaLogExplorerMainUrl = "http://localhost:5601/app/observability-log-explorer/?controlPanels=(data_stream.namespace:(explicitInput:(fieldName:data_stream.namespace,id:data_stream.namespace,title:Namespace),grow:!f,order:0,type:optionsListControl,width:medium))&_a=(columns:!(service.name,host.name,message),filters:!(),grid:(columns:(host.name:(width:320),service.name:(width:240))),index:BQZwpgNmDGAuCWB7AdgFQJ4AcwC4CGEEAlEA,interval:auto,query:(language:kuery,query:%27%27),rowHeight:0,sort:!(!(%27@timestamp%27,desc)))&_g=(filters:!(),refreshInterval:(pause:!t,value:60000),time:(from:now-15m,to:now))"
	kibanaLogInternalUrl     = "http://localhost:5601/internal/controls/optionsList/logs-*-*"

	kibanaDiscoverMainUrl     = "http://localhost:5601/app/discover"
	kibanaDiscoverInternalUrl = "http://localhost:5601/internal/bsearch?compress=false"
)

const (
	waitInterval  = 200 * time.Millisecond
	printInterval = 5 * time.Second
)

const (
	localLogPath = "../docker/quesma/logs/quesma.log"
	ciLogPath    = "/home/runner/work/quesma/quesma/ci/quesma/logs/quesma.log"
	ciEnvVar     = "GITHUB_ACTIONS"
)

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

const kibanaDiscoverInternalQuery = `
{
	"batch": [{
		"request": {
			"params": {
				"index": "logs-generic-*",
				"body": {"size": 500}
			}
		}
	}]
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
		waitForLogsInClickhouse("logs-generic-default", time.Minute, []string{"@timestamp", "attributes_values", "attributes_metadata", "host_name", "message", "service_name", "severity", "source"})
		println("   Logs in Clickhouse: OK")
		waitForAsyncQuery(time.Minute)
		println("   AsyncQuery: OK")
		waitForKibanaLogExplorer("kibana LogExplorer", time.Minute)
		println("   Kibana LogExplorer: OK")
		waitForKibanaDiscover("kibana Search", time.Minute, []string{"severity", "service.name", "host.name", "message"})
		println("   Kibana Discover: OK")
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

func waitForLogsInClickhouse(tableName string, timeout time.Duration, expectColumns []string) {

	sort.Strings(expectColumns)

	var actualColumns []string

	res := waitFor("clickhouse", func() bool {

		// driver name has changed in https://github.com/mailru/go-clickhouse/pull/154/
		connection, err := sql.Open("chhttp", clickhouseUrl)
		if err != nil {
			panic(err)
		}
		defer connection.Close()

		rows, err := connection.Query(fmt.Sprintf("SELECT * FROM `%s` LIMIT 10", tableName))
		if err != nil {
			// wait for a table to be created
			if !strings.Contains(err.Error(), "Code: 60") {
				fmt.Println("Error querying clickhouse:", err)
			}
			return false
		}
		defer rows.Close()

		actualColumns, err = rows.Columns()

		if err != nil {
			panic(err)
		}

		return rows.Next()
	}, timeout)

	if !res {
		panic("no logs in clickhouse")
	}

	sort.Strings(expectColumns)
	sort.Strings(actualColumns)

	if !slices.Equal(expectColumns, actualColumns) {
		panic(fmt.Sprintf("expected columns %v, got %v", expectColumns, actualColumns))
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
			body, readErr := io.ReadAll(resp.Body)
			if resp.StatusCode == 200 {
				if readErr == nil {
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
				fmt.Printf("%s response: %+v body: %s readError: %+v\n", serviceName, resp, body, readErr)
			}
		}
		return false
	}, timeout)

	if !res {
		panic(serviceName + " is not alive or is not receiving logs")
	}
}

func checkLogs() {
	value := os.Getenv(ciEnvVar)
	logPath := localLogPath
	if value != "" {
		logPath = ciLogPath
	}
	content, err := os.ReadFile(logPath)
	if err != nil {
		panic("Error reading file:" + err.Error())
		return
	}

	fileContent := string(content)
	searchString := "Panic recovered:"

	if bytes.Contains([]byte(fileContent), []byte(searchString)) {
		panic("Panic recovered in quesma.log")
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

func waitForKibanaLogExplorer(serviceName string, timeout time.Duration) {
	res := waitFor(serviceName, func() bool {
		_, ok := sendKibanaRequest(kibanaLogInternalUrl, "POST", kibanaLogExplorerMainUrl, kibanaInternalLog)
		return ok
	}, timeout)
	if !res {
		panic(serviceName + " is not alive or is not receiving logs")
	}
}

func waitForKibanaDiscover(serviceName string, timeout time.Duration, expectColumns []string) {
	var response map[string]interface{}

	success := waitFor(serviceName, func() bool {
		body, ok := sendKibanaRequest(kibanaDiscoverInternalUrl, "POST", kibanaDiscoverMainUrl, kibanaDiscoverInternalQuery)
		if !ok {
			return false
		}

		err := json.Unmarshal([]byte(body), &response)
		if err != nil {
			panic(serviceName + " received invalid response from Kibana: " + body)
		}

		return true
	}, timeout)

	if !success {
		panic(serviceName + " is not alive or is not receiving logs")
	}

	result := response["result"]
	if result == nil {
		panic(fmt.Sprintf("Received invalid search results from Kibana (wrong 'result' field): %v", response))
	}
	rawResponse := result.(map[string]interface{})["rawResponse"]
	if rawResponse == nil {
		panic(fmt.Sprintf("Received invalid search results from Kibana (wrong 'rawResponse' field): %v", response))
	}
	hits := rawResponse.(map[string]interface{})["hits"]
	if hits == nil {
		panic(fmt.Sprintf("Received invalid search results from Kibana (wrong 'hits' field): %v", response))
	}
	innerHits := hits.(map[string]interface{})["hits"]
	if innerHits == nil {
		panic(fmt.Sprintf("Received invalid search results from Kibana (wrong inner 'hits' field): %v", response))
	}

	innerHitsCount := len(innerHits.([]interface{}))
	if innerHitsCount < 10 {
		panic(fmt.Sprintf("Search result contains only %d records", innerHitsCount))
	}

	innerHitsStr := fmt.Sprintf("%v", innerHits)
	for _, expectedColumn := range expectColumns {
		if !strings.Contains(innerHitsStr, expectedColumn) {
			panic(fmt.Sprintf("missing column %v from hits", expectedColumn))
		}
	}
}

func sendKibanaRequest(url string, method string, referrer, query string) (string, bool) {
	req, err := http.NewRequest(method, url, bytes.NewBuffer([]byte(query)))
	if err != nil {
		fmt.Println("Error creating request:", err)
		return "", false
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
		return "", false
	}
	defer response.Body.Close()

	body, err := io.ReadAll(response.Body)
	if err != nil {
		fmt.Println("Error reading response:", err)
		return "", false
	}

	bodyStr := string(body)
	if strings.Contains(bodyStr, "\"statusCode\":500,\"error\":\"Internal Server Error\"") {
		return "", false
	}
	return bodyStr, true
}
