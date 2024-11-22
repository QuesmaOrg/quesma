// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
//go:build integration

package e2e

import (
	"bytes"
	"fmt"
	"github.com/goccy/go-json"
	"log"
	"net/http"
	"time"
)

const quesmaUrl = "http://localhost:8080"
const elasticUrl = "http://localhost:9201"

type logEntry struct {
	Process struct {
		Name       string `json:"name"`
		Pid        int    `json:"pid"`
		EntityID   string `json:"entity_id"`
		Executable string `json:"executable"`
	} `json:"process"`
	Timestamp string `json:"@timestamp"`
	Event     struct {
		Category string `json:"category"`
		Type     string `json:"type"`
	} `json:"event"`
}

func someLogEntry(ts time.Time) logEntry {

	var entry logEntry

	entry.Event.Category = "process"
	entry.Event.Type = "start"

	entry.Timestamp = ts.Format(time.RFC3339)

	entry.Process.Name = "Test"
	entry.Process.Executable = "Test"
	entry.Process.Pid = 1
	entry.Process.EntityID = "1"

	return entry
}

func toBulk(entry logEntry) (logBytes []byte) {

	const windowsBulkJson = `{"create":{"_index":"windows_logs"}}`

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

func sendLogEntryTo(targetUrl string, logBytes []byte) {

	if resp, err := http.Post(targetUrl+"/_bulk", "application/json", bytes.NewBuffer(logBytes)); err != nil {
		log.Printf("Failed to send windows logs: %v", err)
	} else {
		fmt.Printf("Sent windows_logs to %s response=%s\n", targetUrl, resp.Status)
		if err := resp.Body.Close(); err != nil {
			log.Fatal(err)
		}
	}
}

func sendLogEntry(logBytes []byte) {
	sendLogEntryTo(quesmaUrl, logBytes)
	sendLogEntryTo(elasticUrl, logBytes)
}

func setup(categoryName string) {
	// setup

	// these events are used to test the queries
	{
		entry := someLogEntry(time.Unix(0, 0))
		entry.Event.Category = categoryName
		entry.Event.Type = "start"
		entry.Process.Pid = 1
		entry.Process.EntityID = "1"
		logBytes := toBulk(entry)
		sendLogEntry(logBytes)
	}

	{
		entry := someLogEntry(time.Unix(1, 0))
		entry.Event.Category = categoryName
		entry.Process.Pid = 1
		entry.Event.Type = "stop"
		entry.Process.EntityID = "1"
		logBytes2 := toBulk(entry)
		sendLogEntry(logBytes2)
	}

	{
		entry := someLogEntry(time.Unix(2, 0))
		entry.Event.Category = categoryName
		entry.Process.Pid = 1
		entry.Event.Type = "crash"
		entry.Process.EntityID = "1"
		logBytes2 := toBulk(entry)
		sendLogEntry(logBytes2)
	}
}
