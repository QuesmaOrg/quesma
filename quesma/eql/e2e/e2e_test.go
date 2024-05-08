package e2e

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"mitmproxy/quesma/jsonprocessor"
	"net/http"
	"sort"
	"strings"
	"testing"
	"time"
)

// Tests are disabled by default. To enable them, set the condition to false.
// Tests requires Quesma and Elastic to be running.

var runTests = true

var eqlQueries = []string{
	`any where false`,
	`not_existing where true`,
	"process where true",
	"process where process.pid == 1",
	"process where process.pid > 0",
	"process where process.pid >= 0",
	"process where process.pid < 2",
	"process where process.pid <= 2",
	`process where process.pid == 1 + 1 - 1 `,
	`process where process.pid == 2 / 2`,
	`process where process.pid == 3 % 2`,
	`process where process.pid == 2 * 3 / 6`,
	`-- process where process.pid < 4.0 / 2`, // TODO add floats
	`process where not false`,
	`process where not (event.type == "start")`,
	`process where process.pid == 1 and event.type == "start"`,
	`process where event.type : "start"`,
	`process where event.type : "st*"`,
	`process where event.type :  ("start", "stop")`,
	`process where process.pid == 1 and event.type like "st*"`,
	`-- process where process.pid == 1 and event.type like "st%"`, // FIXME this is a bug, we should escape % in like
	`process where process.name like~ "test"`,
	`process where process.name like ("test", "test2")`,
	`process where event.type in ("start", "stop")`,
	`process where event.type in~ ("STaRT", "StOP")`,
	`process where event.type not in ("start", "stop")`,
	`-- process where event.type not in~ ("STaRT", "StOP")`, // FIXME THIS IS A BUG,  quesma retured: 3 but elastic returned: 1

	`process where process.name != string(1)`,
	`process where process.name == null`,

	// FIXME elastic returns:  error calling elastic: Unexpected status code: 400, 400 Bad Request
	// {"error":{"root_cause":[{"type":"verification_exception","reason":"Found 1 problem\nline 1:25: Unknown column [ddl.name]"}],"type":"verification_exception","reason":"Found 1 problem\nline 1:25: Unknown column [ddl.name]"},"status":400}
	`-- process where ddl.name != null`,

	`process where process.name regex "T.*"`,
	`process where process.name regex~ "t.*"`,

	`process where process.name : "*est"`,
	`process where process.name : "T*t"`,
	`process where process.name : "Te*"`,

	`process where process.name like "Te"`,
	`process where process.name like "T*t"`,

	`-- process where process.name : "_est"`, //FIXME we shoule escace _ in like,  quesma retured: 3 but elastic returned: 0
	`-- process where process.name : "Te_t"`, // FIXME quesma retured: 3 but elastic returned: 0
	`process where process.name : "Te_"`,

	`-- process where process.name : "?est"`, // FIXME support ? wildcard , quesma retured: 0 but elastic returned: 3
	`-- process where process.name : "Te?t"`,
	`process where process.name : "Te?"`,

	`process where process.pid == add(0,1)`,
	`-- process where process.pid == add(-2,3)`, // FIXME this is a bug, we should support negative numbers
	`-- process where process.pid == add(-2,3)`,

	// FIXME this is an  elastic limitation
	// elastic fail response: {"error":{"root_cause":[{"type":"ql_illegal_argument_exception","reason":"Line 1:40: Comparisons against fields are not (currently) supported; offender [add(process.pid,0)] in [==]"}],"type":"ql_illegal_argument_exception","reason":"Line 1:40: Comparisons against fields are not (currently) supported; offender [add(process.pid,0)] in [==]"},"status":500}
	`-- process where process.pid == add(process.pid,0)`,

	`-- process where add(null, 1) == null`, // FIXME elastic supports it but quesma does not

	`process where process.pid == add(process.pid, null)`, // Comparisons against fields are not (currently) supported; offender

	`process where between(process.name, "T", "t") == "es"`,
}

func TestE2E(t *testing.T) {

	if !runTests {
		t.Skip("Tests are disabled. To enable them, set the condition to false.")
		return
	}

	categoryName := fmt.Sprintf("test%d", time.Now().UnixMilli())

	setup(categoryName)
	fmt.Println("Waiting for data to be indexed...")
	time.Sleep(5 * time.Second)

	for _, eqlQuery := range eqlQueries {
		t.Run(eqlQuery, func(tt *testing.T) {

			if strings.HasPrefix(eqlQuery, "--") {
				return
			}
			fmt.Println("Running test for query:", eqlQuery)

			if strings.HasPrefix(eqlQuery, "process") {
				eqlQuery = categoryName + eqlQuery[len("process"):]
			}

			testQuery(tt, eqlQuery)
		})
	}
}

const quesmaUrl = "http://localhost:8080"
const elasticUrl = "http://localhost:9201"

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
		fmt.Printf("Sent windows_logs to %s response=%s\n", targetUrl, resp.Status)
		if err := resp.Body.Close(); err != nil {
			log.Fatal(err)
		}
	}
}

func sendToWindowsLog(logBytes []byte) {
	sendToWindowsLogTo(quesmaUrl, logBytes)
	sendToWindowsLogTo(elasticUrl, logBytes)
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
		fmt.Println("missing events in hits")
		// FIXME this is a bug
		// quesma omits empty events array
		//return nil, fmt.Errorf("missing events in hits")
		events = []interface{}{}
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
	sort.Slice(res, func(i, j int) bool {
		return strings.Compare(res[i]["@timestamp"].(string), res[j]["@timestamp"].(string)) < 0
	})
	return res, nil
}

func setup(categoryName string) {
	// setup

	{
		entry := someProcessEntry(time.Unix(0, 0))
		entry.Event.Category = categoryName
		entry.Event.Type = "start"
		entry.Process.Pid = 1
		entry.Process.EntityID = "1"
		logBytes := toBulk(entry)
		sendToWindowsLog(logBytes)
	}

	{
		entry := someProcessEntry(time.Unix(1, 0))
		entry.Event.Category = categoryName
		entry.Process.Pid = 1
		entry.Event.Type = "stop"
		entry.Process.EntityID = "1"
		logBytes2 := toBulk(entry)
		sendToWindowsLog(logBytes2)
	}

	{
		entry := someProcessEntry(time.Unix(2, 0))
		entry.Event.Category = categoryName
		entry.Process.Pid = 1
		entry.Event.Type = "crash"
		entry.Process.EntityID = "1"
		logBytes2 := toBulk(entry)
		sendToWindowsLog(logBytes2)
	}
}

func testQuery(t *testing.T, eqlQuery string) {

	fmt.Println("Rewritten  query:", eqlQuery)

	fmt.Println("Calling Elastic...")
	elasticResponse, err := queryEql(elasticUrl, eqlQuery)
	if err != nil {
		fmt.Println("elastic fail response:", elasticResponse)
		t.Fatalf("error calling elastic: %v", err)
		return
	}

	elasticEvents, err := toListOfEvents(elasticResponse)
	if err != nil {
		t.Fatalf("error parsing elastic response: %v", err)
	}

	fmt.Println("Calling Quesma...")
	quesmaResponse, err := queryEql(quesmaUrl, eqlQuery)
	if err != nil {
		t.Fatalf("error calling quesma: %v", err)
		return
	}

	qeusmaEvents, err := toListOfEvents(quesmaResponse)
	if err != nil {
		t.Fatalf("error parsing quesma response: %v", err)
	}

	if len(qeusmaEvents) != len(elasticEvents) {
		t.Fatalf("different number of events: quesma retured: %v but elastic returned: %v", len(qeusmaEvents), len(elasticEvents))
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

	// TODO compare number of keys

	for k, v := range elastic {

		if k == "@timestamp" {
			continue //FIXME compare timestamps
		}

		if quesma[k] != v {
			t.Errorf("eventNo: %d - different values for key %v: quesma: '%v' != elastic: '%v'", evenNo, k, quesma[k], v)
		} else {
			fmt.Printf("eventNo: %d - same values for key %v: quesma: '%v' == elastic: '%v'\n", evenNo, k, quesma[k], v)
		}
	}
}
