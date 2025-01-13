// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package proxy

import (
	"bytes"
	"fmt"
	"github.com/QuesmaOrg/quesma/quesma/stats"
	"github.com/QuesmaOrg/quesma/quesma/util"
	"github.com/goccy/go-json"
	"github.com/stretchr/testify/assert"
	"log"
	"net"
	"net/http"
	"slices"
	"strconv"
	"testing"
	"time"
)

const reason = `This test turned out to cause problems on GitHub Actions CI 
		(goroutines got stuck on IO Wait for 10 minutes and whole job timed out). 
		This is probably due to some resource problem with GitHub workers.
		We've discussed this in https://quesma.slack.com/archives/C06CNHT9944/p1709136102128349
		It's also not a critical test, so it's skipped for now.`

func TestTcpProxy_Ingest(t *testing.T) {
	t.Skip(reason)
	fromPort := findFreePort()
	toPort := findFreePort()

	proxy := NewTcpProxy(fromPort, "localhost:"+strconv.Itoa(int(toPort)), false)

	go proxy.Ingest()
	err := proxy.WaitUntilReady(time.Minute)
	assert.NoError(t, err, "Error waiting for proxy to be ready")

	verifyTCPProxy(t, "hello", fromPort, toPort)
}

func TestTcpProxy_IngestAndProcess(t *testing.T) {
	t.Skip(reason)
	fromPort := findFreePort()
	toPort := findFreePort()

	proxy := NewTcpProxy(fromPort, "localhost:"+strconv.Itoa(int(toPort)), true)

	go proxy.Ingest()
	err := proxy.WaitUntilReady(time.Minute)
	assert.NoError(t, err, "Error waiting for proxy to be ready")

	verifyTCPProxy(t, exampleLog(), fromPort, toPort)
	verifyStatistics(t, fromPort)
}

func verifyStatistics(t *testing.T, port util.Port) {
	go func() {
		_, err := http.Post(fmt.Sprintf("http://localhost:%d/logs/_doc", int(port)), "application/json", bytes.NewBuffer([]byte(exampleLog())))
		if err != nil {
			log.Fatal("Error posting log:", err)
			return
		}
	}()

	for i := 0; i < 50; i++ {
		ingestStats, err := stats.GlobalStatistics.GetIngestStatistics("logs")
		if err == nil && ingestStats != nil && ingestStats.Requests > 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Fatal("Statistics not updated")
}

func verifyTCPProxy(t1 *testing.T, data string, fromPort util.Port, toPort util.Port) {
	conn, err := net.Dial("tcp", ":"+strconv.Itoa(int(fromPort)))
	if err != nil {
		t1.Fatal("Error dialing to port:", err)
	}
	defer conn.Close()

	destListener, err := net.Listen("tcp", ":"+strconv.Itoa(int(toPort)))
	if err != nil {
		t1.Fatal("Error listening to port:", err)
	}

	destConn, err := destListener.Accept()
	if err != nil {
		t1.Fatal("Error accepting connection:", err)
	}

	_, err = conn.Write([]byte(data))

	if err != nil {
		t1.Fatal("Error writing to port:", err)
	}

	buf := make([]byte, 1024)
	n, err := destConn.Read(buf)
	if err != nil {
		t1.Fatal("Error reading from port:", err)
	}
	if string(buf[:n]) != data {
		t1.Fatalf("Expected '%s', got:", string(buf[:n]))
	}
}

var allocatedPorts = make([]util.Port, 0)

func findFreePort() util.Port {
	port := 11000

	for {
		if slices.Contains(allocatedPorts, util.Port(port)) {
			port++
			continue
		}
		listener, err := net.Listen("tcp", ":"+strconv.Itoa(port))
		if err == nil {
			_ = listener.Close()
			allocatedPorts = append(allocatedPorts, util.Port(port))
			log.Println("Allocated port:", port)
			return util.Port(port)
		}
		port++
	}
}

func exampleLog() string {
	body, _ := json.Marshal(map[string]string{
		"timestamp": time.Now().Format("2006-01-02T15:04:05.999Z"),
		"message":   "Something happened!",
		"severity":  "info",
		"source":    "oracle",
	})
	return string(body)
}
