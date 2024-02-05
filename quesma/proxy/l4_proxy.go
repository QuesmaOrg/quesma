package proxy

import (
	"context"
	"fmt"
	"io"
	"log"
	"mitmproxy/quesma/network"
	"net"
	"net/url"
	"sync/atomic"
)

type TcpProxy struct {
	From                 network.Port
	To                   *url.URL
	ready                chan struct{}
	acceptingConnections atomic.Bool
}

func NewTcpProxy(From network.Port, To *url.URL) *TcpProxy {
	return &TcpProxy{
		From:  From,
		To:    To,
		ready: make(chan struct{}),
	}
}

func (t *TcpProxy) Ingest() {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", t.From))
	if err != nil {
		log.Fatal("Error listening to port:", err)
	}
	defer func(l net.Listener) {
		if err := l.Close(); err != nil {
			log.Printf("Error closing the listener: %s\n", err)
		}
	}(listener)

	close(t.ready)
	t.acceptingConnections.Store(true)

	log.Printf("Listening on port %d and forwarding to %s\n", t.From, t.To.String())

	for t.acceptingConnections.Load() {
		clientConn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err)
			continue
		}

		serverConn, err := net.Dial("tcp", t.To.String())
		if err != nil {
			fmt.Println("Error connecting to remote server:", err)
			closeConnection(clientConn)
			continue
		}

		go t.handleConnection(clientConn, serverConn)
	}
}

func (t *TcpProxy) WaitUntilReady() {
	<-t.ready
}

func (t *TcpProxy) Stop(context.Context) {
	// TODO: handle the case where the proxy blocks on listener.Accept()
	t.acceptingConnections.Store(false)
}

func (t *TcpProxy) handleConnection(clientConn net.Conn, serverConn net.Conn) {
	log.Printf("Handling incoming connection from [%s] to [%s]\n", clientConn.RemoteAddr(), serverConn.RemoteAddr())
	defer closeConnection(clientConn)
	defer closeConnection(serverConn)

	go t.copyData(clientConn, serverConn)
	t.copyData(serverConn, clientConn)
}

func (t *TcpProxy) copyData(src net.Conn, dest net.Conn) {
	if _, err := io.Copy(dest, src); err != nil {
		fmt.Println("Error copying data:", err)
	}
}

func closeConnection(connection net.Conn) {
	if err := connection.Close(); err != nil {
		log.Printf("Error closing connection: %s\n", err)
	}
}
