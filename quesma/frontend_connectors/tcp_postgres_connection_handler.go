// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package frontend_connectors

import (
	"fmt"
	quesma_api "github.com/QuesmaOrg/quesma/quesma/v2/core"
	"github.com/jackc/pgx/v5/pgproto3"
	"net"
)

type TcpPostgresConnectionHandler struct {
	processors []quesma_api.Processor
}

func (p *TcpPostgresConnectionHandler) HandleConnection(conn net.Conn) error {
	backend := pgproto3.NewBackend(conn, conn)
	defer p.close(conn)

	//err := p.handleStartup(conn, backend)
	//if err != nil {
	//	return err
	//}

	receivedStartupMessage := false

	dispatcher := quesma_api.Dispatcher{}

	for {
		var resp any
		var err error

		if receivedStartupMessage {
			resp, err = backend.Receive()
		} else {
			resp, err = backend.ReceiveStartupMessage()
			if _, isStartup := resp.(*pgproto3.StartupMessage); isStartup {
				receivedStartupMessage = true
			}
		}
		if err != nil {
			return fmt.Errorf("error receiving message: %w", err)
		}

		metadata := make(map[string]interface{})
		_, resp = dispatcher.Dispatch(p.processors, metadata, resp)
		if resp != nil {
			_, err = conn.Write(resp.([]byte))
			if err != nil {
				return fmt.Errorf("error sending response: %w", err)
			}
		}
	}
}

func (p *TcpPostgresConnectionHandler) handleStartup(conn net.Conn, backend *pgproto3.Backend) error {
	startupMessage, err := backend.ReceiveStartupMessage()
	if err != nil {
		return fmt.Errorf("error receiving startup message: %w", err)
	}

	switch startupMessage.(type) {
	case *pgproto3.StartupMessage:
		buf := mustEncode((&pgproto3.AuthenticationOk{}).Encode(nil))
		buf = mustEncode((&pgproto3.ReadyForQuery{TxStatus: 'I'}).Encode(buf))
		_, err = conn.Write(buf)
		if err != nil {
			return fmt.Errorf("error sending ready for query: %w", err)
		}
	case *pgproto3.SSLRequest:
		_, err = conn.Write([]byte("N"))
		if err != nil {
			return fmt.Errorf("error sending deny SSL request: %w", err)
		}
		return p.handleStartup(conn, backend)
	default:
		return fmt.Errorf("unknown startup message: %#v", startupMessage)
	}

	return nil
}

func (p *TcpPostgresConnectionHandler) close(conn net.Conn) error {
	return conn.Close()
}

func mustEncode(buf []byte, err error) []byte {
	if err != nil {
		panic(err)
	}
	return buf
}

func (h *TcpPostgresConnectionHandler) SetHandlers(processors []quesma_api.Processor) {
	h.processors = processors
}
