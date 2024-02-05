package proxy

import (
	"log"
	"mitmproxy/quesma/network"
	"net"
	"net/url"
	"slices"
	"strconv"
	"testing"
)

func TestTcpProxy_Ingest(t1 *testing.T) {
	fromPort := findFreePort()
	toPort := findFreePort()
	toUrl, _ := url.Parse("localhost:" + strconv.Itoa(int(toPort)))

	proxy := NewTcpProxy(fromPort, toUrl)
	go proxy.Ingest()
	proxy.WaitUntilReady()

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

	_, err = conn.Write([]byte("Hello"))

	if err != nil {
		t1.Fatal("Error writing to port:", err)
	}

	buf := make([]byte, 1024)
	n, err := destConn.Read(buf)
	if err != nil {
		t1.Fatal("Error reading from port:", err)
	}
	if string(buf[:n]) != "Hello" {
		t1.Fatal("Expected 'Hello', got:", string(buf[:n]))
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
