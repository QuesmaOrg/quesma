package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"time"

	_ "github.com/mailru/go-clickhouse"
)

const (
	clickhouseUrl        = "http://localhost:8123"
	kibanaHealthCheckUrl = "http://localhost:5601/api/status"
	elasticIndexCountUrl = "http://localhost:9201/logs-generic-default/_count"
	quesmaIndexCountUrl  = "http://localhost:9200/logs-generic-default/_count"
)

const (
	waitInterval  = 100 * time.Millisecond
	printInterval = 5 * time.Second
)

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
