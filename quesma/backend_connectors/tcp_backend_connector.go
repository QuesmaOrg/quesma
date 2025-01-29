// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package backend_connectors

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"time"

	quesma_api "github.com/QuesmaOrg/quesma/quesma/v2/core"
)

type TcpBackendConnector struct {
	addr string
}

func NewTcpBackendConnector(addr string) (*TcpBackendConnector, error) {
	return &TcpBackendConnector{
		addr: addr,
	}, nil
}

func (t TcpBackendConnector) InstanceName() string {
	return "TcpBackendConnector"
}

func (t TcpBackendConnector) GetId() quesma_api.BackendConnectorType {
	return quesma_api.TcpBackend
}

func (t TcpBackendConnector) Open() error {
	return nil
}

// FIXME: those functions below are only relevant for SQL connectors, we could potentially remove them from the BackendConnector interface

func (t TcpBackendConnector) Query(ctx context.Context, query string, args ...interface{}) (quesma_api.Rows, error) {
	return nil, fmt.Errorf("query is not available in TcpBackendConnector")
}

func (t TcpBackendConnector) QueryRow(ctx context.Context, query string, args ...interface{}) quesma_api.Row {
	log.Fatal("QueryRow is not available in TcpBackendConnector")
	return nil
}

func (t TcpBackendConnector) Exec(ctx context.Context, query string, args ...interface{}) error {
	return fmt.Errorf("exec is not available in TcpBackendConnector")
}

func (t TcpBackendConnector) Stats() quesma_api.DBStats {
	log.Fatal("QueryRow is not available in TcpBackendConnector")
	return quesma_api.DBStats{}
}

func (t TcpBackendConnector) Close() error {
	return nil
}

func (t TcpBackendConnector) Ping() error {
	return nil
}

func (t TcpBackendConnector) NewConnection() (net.Conn, error) {
	return net.Dial("tcp", t.addr)
}

// FIXME: those functions below are just TCP net.Conn helpers, not actually a part of the backend connector logic

func ConnWrite(conn net.Conn, data []byte) error {
	if conn == nil {
		return fmt.Errorf("connection is nil")
	}

	n, err := conn.Write(data)
	if err != nil {
		return err
	}

	if n != len(data) {
		return fmt.Errorf("short write: wrote %d bytes but expected to write %d bytes", n, len(data))
	}
	return nil
}

func ConnRead(conn net.Conn, n int) ([]byte, error) {
	if conn == nil {
		return nil, fmt.Errorf("connection is nil")
	}

	var result bytes.Buffer

	err := conn.SetReadDeadline(time.Now().Add(1000 * time.Millisecond))
	if err != nil {
		return result.Bytes(), err
	}

	tmp := make([]byte, n)
	n, err = io.ReadAtLeast(conn, tmp, n)
	if err != nil {
		return result.Bytes(), err
	}

	result.Write(tmp[:n])
	return result.Bytes(), nil
}
