// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package backend_connectors

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	quesma_api "github.com/QuesmaOrg/quesma/quesma/v2/core"
	"io"
	"net"
	"os"
	"time"
)

type TcpBackendConnector struct {
	conn net.Conn
	addr string
}

func NewTcpBackendConnector(addr string) (*TcpBackendConnector, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to %s: %w", addr, err)
	}

	return &TcpBackendConnector{
		conn: conn,
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

func (t TcpBackendConnector) Query(ctx context.Context, query string, args ...interface{}) (quesma_api.Rows, error) {
	//TODO implement me
	panic("implement me")
}

func (t TcpBackendConnector) QueryRow(ctx context.Context, query string, args ...interface{}) quesma_api.Row {
	//TODO implement me
	panic("implement me")
}

func (t TcpBackendConnector) Exec(ctx context.Context, query string, args ...interface{}) error {
	//TODO implement me
	panic("implement me")
}

func (t TcpBackendConnector) Stats() quesma_api.DBStats {
	//TODO implement me
	panic("implement me")
}

func (t TcpBackendConnector) Close() error {
	return nil
}

func (t TcpBackendConnector) Ping() error {
	//TODO implement me
	panic("implement me")
}

func (t TcpBackendConnector) Write(data []byte) error {
	if t.conn == nil {
		return fmt.Errorf("connection is nil")
	}
	n, err := t.conn.Write(data)
	if err != nil {
		return err
	}
	if n != len(data) {
		return fmt.Errorf("short write: wrote %d bytes but expected to write %d bytes", n, len(data))
	}
	return nil
}

func (t TcpBackendConnector) Read(n int) ([]byte, error) {
	if t.conn == nil {
		return nil, fmt.Errorf("connection is nil")
	}

	// Create buffer to store all read data
	var buffer bytes.Buffer

	// Create temporary buffer for reading chunks
	tmp := make([]byte, n)

	// Set read deadline to avoid blocking forever
	err := t.conn.SetReadDeadline(time.Now().Add(1000 * time.Millisecond))
	if err != nil {
		return buffer.Bytes(), err
	}

	n, err = t.conn.Read(tmp)
	if err != nil {
		if err == io.EOF || errors.Is(err, os.ErrDeadlineExceeded) {
			return buffer.Bytes(), nil
		}
		return buffer.Bytes(), err
	}

	buffer.Write(tmp[:n])

	return buffer.Bytes(), nil
}
