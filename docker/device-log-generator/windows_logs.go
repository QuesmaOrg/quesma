package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"time"
)

const windowsJsonFile = "assets/windows_logs.json"

const windowsBulkJson = `{"create":{"_index":"windows_logs"}}`

func makeEntry(entry map[string]interface{}, when time.Time) (logBytes []byte) {

	entry["@timestamp"] = when.Format(time.RFC3339)

	serialized, err := json.Marshal(entry)
	if err != nil {
		log.Println(err)
	}

	logBytes = append(logBytes, []byte(windowsBulkJson)...)
	logBytes = append(logBytes, []byte("\n")...)
	logBytes = append(logBytes, serialized...)
	logBytes = append(logBytes, []byte("\n")...)
	return logBytes
}

func sendBulk(logBytes []byte) {
	targetUrl := configureTargetUrl()

	if resp, err := http.Post(targetUrl, "application/json", bytes.NewBuffer(logBytes)); err != nil {
		log.Printf("Failed to send windows logs: %v", err)
	} else {
		fmt.Printf("Sent windows_logs response=%s\n", resp.Status)
		if err := resp.Body.Close(); err != nil {
			log.Fatal(err)
		}
	}
}

func windowsLogGenerator() {

	targetUrl := configureTargetUrl()

	file, err := os.Open(windowsJsonFile)
	if err != nil {

		fmt.Println("Error opening file ", windowsJsonFile, err)
		fmt.Println(`
Warning: 

We can't commit the file to the repository because of licensing issues.


Run the following command to download the file:
curl https://raw.githubusercontent.com/elastic/elasticsearch/8.13/docs/src/yamlRestTest/resources/normalized-T1117-AtomicRed-regsvr32.json -o docker/device-log-generator/assets/windows_logs.json

This is temporary and will be removed in the future.
`)
		return
	}
	defer file.Close()

	entries := make([]map[string]interface{}, 0)

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()

		if strings.HasPrefix(line, "{\"create\":{}") {
			continue
		}

		entry := make(map[string]interface{})
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			log.Fatal(err)
		}
		entries = append(entries, entry)
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	start := time.Now()

	fmt.Println("entries:", len(entries))

	logBytes := []byte{}

	shift := -len(entries)
	for _, entry := range entries {
		shift++
		logBytes = append(logBytes, makeEntry(entry, start.Add(time.Duration(shift)*time.Minute))...)
	}
	fmt.Println("Sending logs to :", targetUrl, "\n", string(logBytes))
	sendBulk(logBytes)

	sleepDuration := time.Duration(5) * time.Second

	r := rand.NewSource(time.Now().UnixNano())
	for {
		time.Sleep(sleepDuration)

		p := r.Int63() % int64(len(entries))
		entry := entries[p]
		logBytes := []byte{}
		logBytes = makeEntry(entry, time.Now())
		sendBulk(logBytes)

	}
}
