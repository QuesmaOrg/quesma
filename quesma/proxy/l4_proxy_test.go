package proxy

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"mitmproxy/quesma/network"
	"mitmproxy/quesma/stats"
	"net"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"testing"
	"time"
)

func TestTcpProxy_Ingest(t *testing.T) {
	fromPort := findFreePort()
	toPort := findFreePort()
	toUrl, _ := url.Parse("localhost:" + strconv.Itoa(int(toPort)))

	proxy := NewTcpProxy(fromPort, toUrl, false)

	go proxy.Ingest()
	proxy.WaitUntilReady()

	verifyTCPProxy(t, "hello", fromPort, toPort)
}

func TestTcpProxy_IngestAndProcess(t *testing.T) {
	fromPort := findFreePort()
	toPort := findFreePort()
	toUrl, _ := url.Parse("localhost:" + strconv.Itoa(int(toPort)))

	proxy := NewTcpProxy(fromPort, toUrl, true)

	go proxy.Ingest()
	proxy.WaitUntilReady()

	verifyTCPProxy(t, exampleLog(), fromPort, toPort)
	verifyStatistics(t, fromPort)
}

func verifyStatistics(t *testing.T, port network.Port) {
	go func() {
		_, err := http.Post(fmt.Sprintf("http://localhost:%d/logs/_doc", int(port)), "application/json", bytes.NewBuffer([]byte(exampleLog())))
		if err != nil {
			log.Fatal("Error posting log:", err)
			return
		}
	}()

	for i := 0; i < 50; i++ {
		stats, ok := (*stats.GlobalStatistics)["logs"]
		if ok && stats.Requests > 0 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}

	t.Fatal("Statistics not updated")
}

func verifyTCPProxy(t1 *testing.T, data string, fromPort network.Port, toPort network.Port) {
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

var allocatedPorts = make([]network.Port, 0)

func findFreePort() network.Port {
	port := 11000

	for {
		if slices.Contains(allocatedPorts, network.Port(port)) {
			port++
			continue
		}
		listener, err := net.Listen("tcp", ":"+strconv.Itoa(port))
		if err == nil {
			_ = listener.Close()
			allocatedPorts = append(allocatedPorts, network.Port(port))
			log.Println("Allocated port:", port)
			return network.Port(port)
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
