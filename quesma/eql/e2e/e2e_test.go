package e2e

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"mitmproxy/quesma/jsonprocessor"
	"net/http"
	"strconv"
	"testing"
	"time"
)

const quesma = "http://localhost:8080"
const elastic = "http://localhost:9201"

type processLogEntry struct {
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

func someProcessEntry(ts time.Time) processLogEntry {

	var entry processLogEntry

	entry.Event.Category = "process"
	entry.Event.Type = "start"

	entry.Timestamp = ts.Format(time.RFC3339)

	entry.Process.Name = "Test"
	entry.Process.Executable = "Test"
	entry.Process.Pid = 1
	entry.Process.EntityID = "1"

	return entry
}

func toBulk(entry processLogEntry) (logBytes []byte) {

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

func sendToWindowsLogTo(targetUrl string, logBytes []byte) {

	if resp, err := http.Post(targetUrl+"/_bulk", "application/json", bytes.NewBuffer(logBytes)); err != nil {
		log.Printf("Failed to send windows logs: %v", err)
	} else {
		fmt.Printf("Sent windows_logs response=%s\n", resp.Status)
		if err := resp.Body.Close(); err != nil {
			log.Fatal(err)
		}
	}
}

func sendToWindowsLog(logBytes []byte) {
	sendToWindowsLogTo(quesma, logBytes)
	sendToWindowsLogTo(elastic, logBytes)
}

func parseResponse(response string) (map[string]interface{}, error) {

	var result map[string]interface{}
	err := json.Unmarshal([]byte(response), &result)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func queryEql(target string, eqlQuery string) (string, error) {

	type elasticQuery struct {
		Query string `json:"query"`
	}

	query := elasticQuery{Query: eqlQuery}
	data, err := json.Marshal(query)
	if err != nil {
		return "", err
	}
	reader := bytes.NewReader(data)

	url := target + "/windows_logs/_eql/search"

	req, err := http.NewRequest(http.MethodGet, url, reader)
	if err != nil {
		panic(err)
	}
	req.Header.Set("Content-Type", "application/json")
	res, err := http.DefaultClient.Do(req)

	if err != nil {
		return "", err
	}

	defer res.Body.Close()

	response, err := io.ReadAll(res.Body)
	if err != nil {
		return "", err
	}

	if res.StatusCode != http.StatusOK {
		return string(response), fmt.Errorf("Unexpected status code: %v, %v", res.StatusCode, res.Status)
	}

	return string(response), nil
}

var random = rand.New(rand.NewSource(time.Now().UnixNano()))

func toListOfEvents(response string) ([]map[string]interface{}, error) {

	var res []map[string]interface{}

	parsed, err := parseResponse(response)
	if err != nil {
		return nil, err
	}
	hits, ok := parsed["hits"]
	if !ok {
		return nil, fmt.Errorf("missing hits in response")
	}

	events, ok := hits.(map[string]interface{})["events"]
	if !ok {
		return nil, fmt.Errorf("missing events in hits")
	}

	for i, event := range events.([]interface{}) {

		m := event.(map[string]interface{})

		source, ok := m["_source"]
		if !ok {
			return nil, fmt.Errorf("missing source in event")
		}

		sourceAsMap := source.(map[string]interface{})

		sourceAsMap = jsonprocessor.FlattenMap(sourceAsMap, "::")

		fmt.Println("event", i, sourceAsMap)
		res = append(res, sourceAsMap)
	}

	return res, nil
}

func TestE2E(t *testing.T) {

	if true {
		t.Skip("Tests are disabled. To enable them, set the condition to false.")
		return
	}

	// setup
	pid := random.Intn(1000000)
	ts := time.Unix(0, 0)
	entry := someProcessEntry(ts)
	entry.Event.Type = "start"
	entry.Process.Pid = pid
	entry.Process.EntityID = "1"
	logBytes := toBulk(entry)
	sendToWindowsLog(logBytes)

	entry2 := someProcessEntry(time.Unix(1, 0))
	entry2.Process.Pid = pid
	entry2.Event.Type = "stop"
	entry2.Process.EntityID = "1"
	logBytes2 := toBulk(entry2)
	sendToWindowsLog(logBytes2)

	time.Sleep(5 * time.Second)

	// query
	eqlQuery := "process where process.pid == " + strconv.Itoa(pid)

	quesmaResponse, err := queryEql(quesma, eqlQuery)
	if err != nil {
		t.Fatal(fmt.Sprintf("error calling quesma: %v", err))
	}

	elasticResponse, err := queryEql(elastic, eqlQuery)
	if err != nil {
		t.Fatal(fmt.Sprintf("error calling elastic: %v", err))
	}

	qeusmaEvents, err := toListOfEvents(quesmaResponse)
	if err != nil {
		t.Fatal(fmt.Sprintf("error parsing quesma response: %v", err))
	}

	elasticEvents, err := toListOfEvents(elasticResponse)
	if err != nil {
		t.Fatal(fmt.Sprintf("error parsing elastic response: %v", err))
	}

	if len(qeusmaEvents) != len(elasticEvents) {
		t.Fatal(fmt.Sprintf("different number of events: %v != %v", len(qeusmaEvents), len(elasticEvents)))
	}

	fmt.Println("Quesma events:", qeusmaEvents)
	fmt.Println("Elastic events:", elasticEvents)

	for i := range len(qeusmaEvents) {

		qesmaEvent := qeusmaEvents[i]
		elasticEvent := elasticEvents[i]

		compareMap(t, i, qesmaEvent, elasticEvent)

	}

}

func compareMap(t *testing.T, evenNo int, quesma map[string]interface{}, elastic map[string]interface{}) {

	for k, v := range elastic {
		if quesma[k] != v {
			t.Errorf("eventNo: %d - different values for key %v: quesma: '%v' != elastic: '%v'", evenNo, k, quesma[k], v)
		}
	}

}
