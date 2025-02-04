// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/brianvoe/gofakeit/v7"
	"io"
	"log"
	"math/rand"
	"net/http"
	"strings"
	"sync/atomic"
	"time"
)

var randomStringPool = []string{"foo", "bar"}

func getRandomValue(fieldType string, sampleValues []string) any {

	switch fieldType {
	case "boolean":
		return rand.Intn(2) == 1 // true or false
	case "constant_keyword", "keyword", "wildcard", "match_only_text":
		var val string

		// totaly random words
		if rand.Float64() < 0.3 {
			return gofakeit.Word()
		}

		// some domain specific values

		if len(sampleValues) > 0 {
			val = sampleValues[rand.Intn(len(sampleValues))]
			// Random sample value
		} else {
			val = randomStringPool[rand.Intn(len(randomStringPool))] // Random string
		}

		if strings.Contains(val, "*") {
			val = strings.ReplaceAll(val, "*", gofakeit.AppName()) // Random value with wildcard
		}

		return val
	case "date":
		return gofakeit.Date().Format("2006-01-02") // Random date
	case "flattened":
		return map[string]string{
			"key": fmt.Sprintf("value_%d", rand.Intn(100)),
		} // Random flattened object
	case "float", "scaled_float":
		return rand.Float64() * 90 // Random float value
	case "geo_point":
		return map[string]float64{
			"lat": rand.Float64()*180 - 90,  // Latitude: -90 to 90
			"lon": rand.Float64()*360 - 180, // Longitude: -180 to 180
		}
	case "ip":
		return fmt.Sprintf("%d.%d.%d.%d",
			rand.Intn(256), rand.Intn(256), rand.Intn(256), rand.Intn(256)) // Random IPv4
	case "long":
		return rand.Int63n(1000000) // Random long integer
	case "nested", "object":
		return map[string]any{
			"nested_field": rand.Intn(100), // Nested object with random int
		}
	default:
		return nil // Unknown type
	}
}

var docCounter atomic.Int64

func generateDocumentInDay(t time.Time, idx IndexMappings) ([]byte, error) {
	doc := make(map[string]interface{})
	for f, t := range idx.Properties {
		doc[f] = getRandomValue(t.Type, t.SampleValues)
	}

	t = t.Add(time.Duration(rand.Intn(24)) * time.Hour)
	t = t.Add(time.Duration(rand.Intn(60)) * time.Minute)

	doc["@timestamp"] = t.Format(time.RFC3339)
	doc["__number_of_fields"] = len(idx.Properties)
	doc["__id"] = docCounter.Add(1)

	data, err := json.Marshal(doc)
	if err != nil {
		return nil, err
	}

	return data, nil
}

func callHTTP(method string, url string, payload []byte) ([]byte, error) {
	client := &http.Client{}
	req, err := http.NewRequest(method, url, bytes.NewBuffer(payload))

	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return nil, err // Failed to send request
	}

	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Println("Failed to read response body:", err)
	}

	if resp.StatusCode != http.StatusOK {

		fmt.Println("Request URL:", method, url)
		fmt.Println("Response status:", resp.Status)
		fmt.Println("Response body:", string(body))

		return body, fmt.Errorf("unexpected status code: %d", resp.StatusCode)

	}
	return body, nil
}

func ingestData(idx IndexMappings) {
	ts := time.Now()

	days := 5
	perDay := 10

	endpoint := "http://localhost:8080"

	for d := range days {
		startAt := ts.Add(-time.Duration(d) * 24 * time.Hour)
		startAt = startAt.Truncate(24 * time.Hour)

		indexName := idx.Name
		if strings.HasSuffix(idx.Pattern, "*") {
			dayPattern := startAt.Format("20060102")
			indexName = strings.TrimSuffix(idx.Pattern, "*")
			if !strings.HasSuffix(indexName, "-") {
				indexName = fmt.Sprintf("%s_%s", indexName, dayPattern)
			} else {
				indexName = fmt.Sprintf("%s%s", indexName, dayPattern)
			}
		}

		bulkJson := fmt.Sprintf(`{"create":{"_index":"%s"}}`, indexName)

		var bulkPayload []byte
		log.Println("Generating bulk... Index:", indexName)
		for range perDay {

			doc, err := generateDocumentInDay(startAt, idx)
			if err != nil {
				fmt.Println(err)
				continue
			}

			bulkPayload = append(bulkPayload, []byte(bulkJson)...)
			bulkPayload = append(bulkPayload, []byte("\n")...)
			bulkPayload = append(bulkPayload, doc...)
			bulkPayload = append(bulkPayload, []byte("\n")...)
		}

		log.Println("Sending bulk... Index:", indexName, len(bulkPayload))
		_, err := callHTTP("POST", fmt.Sprintf("%s/_bulk", endpoint), bulkPayload)
		if err != nil {
			log.Println("Failed to send document:", err)
			continue
		}
	}
}

func ingestAll() {

	log.Println("Starting...")

	indexes, err := readIndexMappings()
	if err != nil {
		log.Fatal(err)
	}

	// populate string pool
	stringValues := make(map[string]bool)

	for _, idx := range indexes {
		for _, field := range idx.Properties {
			if field.Type == "keyword" {
				for _, val := range field.SampleValues {
					stringValues[val] = true
				}
			}
		}
	}

	for str := range stringValues {
		randomStringPool = append(randomStringPool, str)
	}

	log.Println("String pool size:", len(randomStringPool))

	for _, idx := range indexes {
		ingestData(idx)
	}

}
