package main

import (
	"database/sql"
	"encoding/json"
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
)

func main() {
	waitForLogsInClickhouse("/logs-generic-default/_doc")
	waitForLogsInClickhouse("/device_logs/_doc")
	waitForLogsInElasticsearch()
	waitForKibana()
}

const waitInterval = 5 * time.Second
const timeoutAfter = time.Minute

func waitFor(serviceName string, waitForFunc func() bool) bool {
	startTime := time.Now()

	for time.Since(startTime) < timeoutAfter {
		if waitForFunc() {
			return true
		}

		fmt.Printf("elapsed %v, trying %s again in 5s...\n", time.Since(startTime), serviceName)
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
	res := waitFor("elasticsearch", func() bool {
		resp, err := http.Get(elasticIndexCountUrl)
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
		panic("elasticsearch is not alive or is not receiving logs")
	}
}
