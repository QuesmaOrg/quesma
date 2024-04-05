package main

import (
	"encoding/json"
	"log"
	"time"

	"bytes"
	vegeta "github.com/tsenart/vegeta/v12/lib"
	"net/http"
)

const URL_QUESMA = "http://localhost:8080/logs-generic*/_search"
const URL_ELASTIC = "http://localhost:9200/logs-generic*/_search"
const METHOD = "POST"
const DURATION = 60 * time.Second
const FREQUENCY = 1

func reportHDRHistogram(metrics vegeta.Metrics) {
	var resultBuffer bytes.Buffer
	reporter := vegeta.NewHDRHistogramPlotReporter(&metrics)
	reporter.Report(&resultBuffer)
	log.Println(resultBuffer.String())
}

func reportJSON(metrics vegeta.Metrics) {
	var jsonBuffer bytes.Buffer
	reporter := vegeta.NewJSONReporter(&metrics)
	reporter.Report(&jsonBuffer)
	err := json.Unmarshal([]byte(jsonBuffer.String()), &metrics)
	if err != nil {
		log.Println("Error:", err)
		return
	}

	prettyJSON, err := json.MarshalIndent(metrics, "", "  ")
	if err != nil {
		log.Println("Error:", err)
		return
	}
	log.Println(string(prettyJSON))
}

func runSearchLoadTests(url string, body []byte, rate vegeta.Rate, duration time.Duration) {
	log.Println(url)
	targeter := vegeta.NewStaticTargeter(vegeta.Target{
		Method: METHOD,
		URL:    url,
		Body:   body,
		Header: http.Header{
			"Content-Type": []string{"application/json"},
		},
	})
	attacker := vegeta.NewAttacker()

	var metrics vegeta.Metrics
	for res := range attacker.Attack(targeter, rate, duration, "Quesma perf tests") {
		metrics.Add(res)
	}
	metrics.Close()

	reportHDRHistogram(metrics)
	reportJSON(metrics)
}

func main() {
	body := getSearchAggregateQuery(-15 * time.Minute)
	rate := vegeta.Rate{Freq: FREQUENCY, Per: time.Second}
	runSearchLoadTests(URL_ELASTIC, body, rate, DURATION)
	runSearchLoadTests(URL_QUESMA, body, rate, DURATION)
	from := time.Date(2011, 2, 12, 15, 3, 12, 963000000, time.UTC)
	to := time.Date(2011, 3, 13, 15, 17, 59, 803000000, time.UTC)
	const numberOfConcurrentRequests = 1
	const numberOfIterations = 1
	runAsyncSearchLoadTests(numberOfIterations, numberOfConcurrentRequests, from, to)
}
