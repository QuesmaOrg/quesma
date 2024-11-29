// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package frontend_connectors

import (
	"context"
	"fmt"
	"net"
	quesma_api "quesma_v2/core"
	"sync/atomic"
)

type TCPListener struct {
	listener   net.Listener
	Endpoint   string
	handler    quesma_api.TCPConnectionHandler
	isShutdown atomic.Bool
}

func NewTCPConnector(endpoint string) *TCPListener {
	return &TCPListener{
		Endpoint: endpoint,
	}
}

func (t *TCPListener) Listen() error {
	ln, err := net.Listen("tcp", t.Endpoint)
	if err != nil {
		return fmt.Errorf("failed to start TCP listener: %v", err)
	}
	t.listener = ln

	// Start listening for incoming connections in a goroutine
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				if t.isShutdown.Load() {
					return
				}
				fmt.Println("Failed to accept connection:", err)
				continue
			}
			// Handle each connection in a separate goroutine to allow concurrent handling
			go func() {
				err := t.GetConnectionHandler().HandleConnection(conn)
				if err != nil {
					fmt.Println("Error handling connection:", err)
				}
			}()
		}
	}()
	return nil
}

func (t *TCPListener) GetEndpoint() string {
	return t.Endpoint
}

func (t *TCPListener) AddConnectionHandler(handler quesma_api.TCPConnectionHandler) {
	t.handler = handler
}

func (t *TCPListener) GetConnectionHandler() quesma_api.TCPConnectionHandler {
	return t.handler
}

func (t *TCPListener) Stop(ctx context.Context) error {
	t.isShutdown.Store(true)
	if t.listener != nil {
		return t.listener.Close()
	}
	return nil
}
