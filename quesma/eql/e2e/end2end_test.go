package e2e

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// Tests are disabled by default. To enable them, set the condition to false.
// Tests requires Quesma and Elastic to be running.
// Queries are run against both Quesma and Elastic and results are compared.

var runTests = true

func TestE2E(t *testing.T) {

	if !runTests {
		t.Skip("Tests are disabled. To enable them, set the condition to false.")
		return
	}

	// These are the queries that are run against both Quesma and Elastic.
	// Queries start with a "--" are skipped.
	var eqlQueries = []string{
		`any where false`,
		`any where false and true`,
		`not_existing where true`,
		"process where true",
		"process where false and true",
		"process where not false and true",

		"process where process.pid == 1",
		"process where process.pid > 0",
		"process where process.pid >= 0",
		"process where process.pid < 2",
		"process where process.pid <= 2",
		`process where process.pid == 1 + 1 - 1 `,
		`process where process.pid == 2 / 2`,
		`process where process.pid == 3 % 2`,
		`process where process.pid == 2 * 3 / 6`,
		`process where process.pid < 4.0 / 2`,

		`process where not false`,
		`process where not (event.type == "start")`,
		`process where process.pid == 1 and event.type == "start"`,
		`process where event.type : "start"`,
		`process where event.type : "st*"`,
		`process where event.type :  ("start", "stop")`,
		`process where process.pid == 1 and event.type like "st*"`,
		`process where process.pid == 1 and event.type like "st%"`,
		`process where process.name like~ "test"`,
		`process where process.name like ("test", "test2")`,
		`process where event.type in ("start", "stop")`,
		`process where event.type in~ ("STaRT", "StOP")`,
		`process where event.type not in ("start", "stop")`,
		`process where event.type not in~ ("STaRT", "StOP")`,

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

		`process where process.name : "_est"`,
		`process where process.name : "Te_t"`,
		`process where process.name : "Te_"`,

		`process where process.name : "?est"`,
		`process where process.name : "Te?t"`,
		`process where process.name : "Te?"`,

		`process where process.pid == add(0,1)`,
		`process where process.pid == add(-2,3)`,
		`process where process.pid == add(-2,3)`,

		// FIXME this is an  elastic limitation
		// elastic fail response: {"error":{"root_cause":[{"type":"ql_illegal_argument_exception","reason":"Line 1:40: Comparisons against fields are not (currently) supported; offender [add(process.pid,0)] in [==]"}],"type":"ql_illegal_argument_exception","reason":"Line 1:40: Comparisons against fields are not (currently) supported; offender [add(process.pid,0)] in [==]"},"status":500}
		`-- process where process.pid == add(process.pid,0)`,

		`process where add(null, 1) == null`,

		// FIXME Comparisons against fields are not (currently) supported; offender
		`-- process where process.pid == add(process.pid, null)`,

		`-- process where between(process.name, "T", "t") == "es"`,

		// FIXME add IP fields to the test data, first argument of [cidrMatch(\"127.0.0.1\", \"127.0.0.0/24\")] must be [ip], found value [\"127.0.0.1\"] type [keyword]"}]
		`-- process where cidrMatch("127.0.0.1", "127.0.0.0/24")`,

		`-- process where cidrMatch(null, "127.0.0.1/24") == null`, // FIXME this is a bug, quesma returned 0 here

		`process where concat ("a", "b") == "ab"`,
		`process where concat ("a", "b", "c") == "abc"`,
		`process where concat (process.name, "1234") == "Test1234"`,
		`process where concat (process.name, 1234) == "Test1234"`,
		`process where concat ("a") == "a"`,

		`process where concat (null, "a") == null`,
		`process where concat ("a", null) == null`,
		`process where concat (null) == null`,

		`process where divide(4, 2) == 2`,
		`-- process where divide(4, 3) == 1`,       // FIXME this is a bug, Quesma returned 0 here
		`-- process where divide(1.0, 2.0) == 0.5`, // FIXME this is a bug, float are not supported

		`process where divide(null,2) == null`,
		`process where divide(2,null) == null`,

		`process where endsWith("quesma.exe", ".exe")`,
		`process where endsWith("quesma.exe", ".EXE")`,
		`process where endsWith("quesma.exe", "EXE")`,

		`process where endsWith~("quesma.exe", "EXE")`,

		`process where endsWith(null, ".exe") == null`,
		`process where endsWith("quesma.exe", null) == null`,

		`-- process where indexOf("quesma.exe", "ue") == 1`,    // FIXME this is bug in quesma
		`-- process where indexOf("quesma.exe", "UE") == null`, // FIXME this is bug in quesma
		`-- process where indexOf~("quesma.exe", "UE") == 1`,   // FIXME this is bug in quesma
		`-- process where indexOf("", "") == 0`,                // FIXME this is bug in quesma
		`-- process where indexOf("quesma.exe", "") == 0`,      // FIXME this is bug in quesma
		`-- process where indexOf("a.b.c", ".") == 1`,          // FIXME this is a bug in quesma

		`process where indexOf(null, "UE") == null`,
		`process where indexOf("Q", null) == null`,

		`process where length("quesma.exe") == 10`,
		`process where length("") == 0`,
		`process where length(null) == null`,

		`process where modulo(10, 3) == 1`,

		`process where multiply(2, 2) == 4`,
		`process where multiply(null, 2) == null`,
		`process where multiply(2, null) == null`,

		`-- process where number("1234") == 1234`,       // FIXME this is a bug in quesma it's false
		`-- process where number("1234.5") == 1234.5`,   // FIXME float
		`-- process where number("-1234.5") == -1234.5`, // FIXME
		`-- process where number("f", 16) == 15`,        // FIXME 2nd argument is base
		`-- process where number("0x1", null) == 1`,     // FIXME 2nd argument is base
		`-- process where number(null) == null`,         // FIXME it's false in quesma
		`-- process where number(null, 16) == null`,     // FIXME 2nd argument is base

		`process where startsWith("quesma.exe", "quesma")`,
		`process where startsWith("quesma.exe", "QUESMA")`,
		`process where startsWith~("quesma.exe", "QUESMA")`,
		`process where startsWith("", "")`,
		`process where startsWith(null, "quesma") == null`,
		`process where startsWith("quesma.exe", null) == null`,
		`process where startsWith("null", "null") == null`,

		`process where string(1) == "1"`,
		`process where string(null) == null`,
		`process where string(true) == "true"`,
		`process where string("foo") == "foo"`,

		`process where stringContains("quesma.exe", "quesma")`,
		`process where stringContains("quesma.exe", "QUESMA")`,
		`process where stringContains~("quesma.exe", "QUESMA")`,
		`-- process where stringContains("", "")`, // FIXME this is a bug, quesma returned true here
		`process where stringContains(null, "quesma") == null`,

		`-- process where substring("quesma.exe", 1) == "uesma.exe"`, // FIXME this is a bug, quesma returned false here
		`process where substring("quesma.exe", 1, 2) == "ue"`,
		`-- process where substring("quesma.exe", 1, 100) == "uesma.exe"`, // FIXME this is a bug, quesma returned false here
		`process where substring("quesma.exe", 1, 0) == ""`,
		`-- process where substring("quesma.exe", -4) == ".exe"`,    // FIXME this is a bug, quesma returned error here
		`-- process where substring("quesma.exe", -4, -1) == ".ex"`, // FIXME this is a bug, quesma returned error here

		`process where subtract(10, 2) == 8`,
		`process where subtract(null, 2) == null`,
		`process where subtract(2, null) == null`,

		`-- process where ?not_existing == null`, // FIXME this is a bug, optional fields are not supported yet
	}

	// This our category name. Each test runs in a separate category.
	// So we can run multiple tests without need to clean up the data.
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

			// here we replace given category name with the actual category name
			if strings.HasPrefix(eqlQuery, "process") {
				eqlQuery = categoryName + eqlQuery[len("process"):]
			}

			testQuery(tt, eqlQuery)
		})
	}
}

func testQuery(t *testing.T, eqlQuery string) {

	fmt.Println("Rewritten  query:", eqlQuery)

	fmt.Println("Calling Elastic...")
	elasticEvents, err := eqlClient(elasticUrl, eqlQuery)
	if err != nil {
		t.Fatalf("error calling elastic: %v", err)
		return
	}

	fmt.Println("Calling Quesma...")
	quesmaEvents, err := eqlClient(quesmaUrl, eqlQuery)
	if err != nil {
		t.Fatalf("error calling quesma: %v", err)
		return
	}

	if len(quesmaEvents) != len(elasticEvents) {
		t.Fatalf("different number of events: quesma retured: %v but elastic returned: %v", len(quesmaEvents), len(elasticEvents))
	}

	fmt.Println("Quesma events:", quesmaEvents)
	fmt.Println("Elastic events:", elasticEvents)

	for i := range len(quesmaEvents) {
		quesmaEvent := quesmaEvents[i]
		elasticEvent := elasticEvents[i]

		compareMap(t, i, quesmaEvent, elasticEvent)
	}
}

func compareMap(t *testing.T, evenNo int, quesma eqlEvent, elastic eqlEvent) {

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
