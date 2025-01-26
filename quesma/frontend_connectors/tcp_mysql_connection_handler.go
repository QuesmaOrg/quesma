// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
// Experimental alpha frontend for MySQL protocol

package frontend_connectors

import (
	"encoding/binary"
	"fmt"
	"github.com/QuesmaOrg/quesma/quesma/backend_connectors"
	quesma_api "github.com/QuesmaOrg/quesma/quesma/v2/core"
	"io"
	"net"
)

type TcpMysqlConnectionHandler struct {
	processors []quesma_api.Processor
}

var ErrInvalidPacket = fmt.Errorf("invalid packet")

func ReadMysqlPacket(conn net.Conn) ([]byte, error) {
	// MySQL wire protocol packet format (see https://dev.mysql.com/doc/dev/mysql-server/8.4.3/PAGE_PROTOCOL.html):
	// - 3 bytes: length of the packet (= LEN)
	// - 1 byte: sequence ID
	// - LEN bytes: packet body
	//
	// TODO: when packet is larger than 16MB, it's split into multiple packets. This code does NOT support this case yet.

	packetLengthBytes, err := backend_connectors.ConnRead(conn, 3)
	if err == io.EOF {
		return nil, err
	}
	if err != nil || len(packetLengthBytes) != 3 {
		return nil, ErrInvalidPacket
	}
	packetLength := int(binary.LittleEndian.Uint32(append(packetLengthBytes, 0)))

	sequenceId, err := backend_connectors.ConnRead(conn, 1)
	if err == io.EOF {
		return nil, err
	}
	if err != nil || len(sequenceId) != 1 {
		return nil, ErrInvalidPacket
	}

	body, err := backend_connectors.ConnRead(conn, packetLength)
	if err == io.EOF {
		return nil, err
	}
	if err != nil || len(body) != packetLength {
		return nil, ErrInvalidPacket
	}

	fullPacketBytes := packetLengthBytes
	fullPacketBytes = append(fullPacketBytes, sequenceId...)
	fullPacketBytes = append(fullPacketBytes, body...)

	return fullPacketBytes, nil
}

func (p *TcpMysqlConnectionHandler) HandleConnection(conn net.Conn) error {
	dispatcher := quesma_api.Dispatcher{}
	metadata := make(map[string]interface{})

	// When you connect to MySQL, the server sends a greeting packet.
	// Therefore, we dispatch a dummy nil message to the processor for it to be able to try to receive that initial packet
	// (from its TCP backend connector).
	{
		var message any

		metadata, message = dispatcher.Dispatch(p.processors, metadata, nil)
		if message != nil {
			_, err := conn.Write(message.([]byte))
			if err != nil {
				return fmt.Errorf("error sending response: %w", err)
			}
		}
	}

	for {
		var message any

		fullPacketBytes, err := ReadMysqlPacket(conn)
		if err == io.EOF {
			break
		}
		if err != nil {
			continue
		}

		message = fullPacketBytes

		metadata, message = dispatcher.Dispatch(p.processors, metadata, message)
		if message != nil {
			_, err = conn.Write(message.([]byte))
			if err != nil {
				return fmt.Errorf("error sending response: %w", err)
			}
		}
	}

	return nil
}

func (h *TcpMysqlConnectionHandler) SetHandlers(processors []quesma_api.Processor) {
	h.processors = processors
}
