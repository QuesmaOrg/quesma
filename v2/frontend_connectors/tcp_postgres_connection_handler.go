package frontend_connectors

import (
	"fmt"
	"github.com/jackc/pgx/v5/pgproto3"
	"net"
	quesma_api "quesma_v2/core"
)

type TcpPostgresConnectionHandler struct {
	processors []quesma_api.Processor
}

func (p *TcpPostgresConnectionHandler) HandleConnection(conn net.Conn) error {
	backend := pgproto3.NewBackend(conn, conn)
	defer p.close(conn)

	err := p.handleStartup(conn, backend)
	if err != nil {
		return err
	}

	dispatcher := quesma_api.Dispatcher{}

	for {
		msg, err := backend.Receive()
		if err != nil {
			return fmt.Errorf("error receiving message: %w", err)
		}
		var resp any = msg
		metadata := make(map[string]interface{})
		metadata, resp = dispatcher.Dispatch(p.processors, metadata, resp)
		if resp != nil {
			_, err = conn.Write(resp.([]byte))
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
