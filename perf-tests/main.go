package main

import (
	"encoding/json"
	"log"
	"time"

	"bytes"
	vegeta "github.com/tsenart/vegeta/v12/lib"
	"net/http"
)

const URL_QUESMA = "http://localhost:8080/logs-generic*/_async_search"
const URL_ELASTIC = "http://localhost:9200/logs-generic*/_async_search"
const METHOD = "POST"
const DURATION = 60 * time.Second
const FREQUENCY = 100

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

func runLoadTests(url string, body []byte, rate vegeta.Rate, duration time.Duration) {
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

func getAggregateQuery() []byte {
	now := time.Now()

	body := []byte(`{
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
              "gte": ` + `"` + now.Add(-15*time.Minute).Format("2006-01-02T15:04:05.726Z") + `"` + `,
              "lte": ` + `"` + now.Format("2006-01-02T15:04:05.726Z") + `"` + `
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
}`)
	return body
}

func main() {
	body := getAggregateQuery()
	rate := vegeta.Rate{Freq: FREQUENCY, Per: time.Second}
	runLoadTests(URL_ELASTIC, body, rate, DURATION)
	runLoadTests(URL_QUESMA, body, rate, DURATION)
}
