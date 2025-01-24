// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
// Experimental alpha processor for MySQL protocol

package processors

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/QuesmaOrg/quesma/quesma/backend_connectors"
	"github.com/QuesmaOrg/quesma/quesma/frontend_connectors"
	"github.com/QuesmaOrg/quesma/quesma/logger"
	quesma_api "github.com/QuesmaOrg/quesma/quesma/v2/core"
	"net"
)

type TcpPassthroughMysqlProcessor struct {
	BaseProcessor
}

func (t TcpPassthroughMysqlProcessor) InstanceName() string {
	return "TcpPassthroughMysqlProcessor"
}

func (t TcpPassthroughMysqlProcessor) GetId() string {
	return "TcpPassthroughMysqlProcessor"
}

func (t *TcpPassthroughMysqlProcessor) Handle(metadata map[string]interface{}, messages ...any) (map[string]interface{}, any, error) {
	//
	// MySQL client -> tcp_mysql_connection_handler -> tcp_passthrough_mysql_processor -> tcp_backend_connector -> real MySQL server
	//
	backendConnector := t.GetBackendConnector(quesma_api.TcpBackend).(*backend_connectors.TcpBackendConnector)

	var conn net.Conn
	if metadata["conn"] != nil {
		conn = metadata["conn"].(net.Conn)
	} else {
		var err error

		conn, err = backendConnector.NewConnection()
		if err != nil {
			return metadata, nil, err
		}

		metadata["conn"] = conn
	}

	var response []byte

	// This loop reads MySQL packets from this part of the pipeline:
	// MySQL client -> tcp_mysql_connection_handler -> tcp_passthrough_mysql_processor
	//
	// and forwards them to the real MySQL server (via tcp_backend_connector).
	for _, m := range messages {
		if m == nil {
			continue
		}

		msg := m.([]byte)
		msg = maybeProcessComQuery(msg)

		err := backend_connectors.ConnWrite(conn, msg)
		if err != nil {
			return nil, nil, err
		}
	}

	// This loop reads MySQL packets from this part of the pipeline:
	// tcp_passthrough_mysql_processor <- tcp_backend_connector <- real MySQL server
	//
	// and forwards them back to the MySQL client.
	for {
		fullPacketBytes, err := frontend_connectors.ReadMysqlPacket(conn)
		if err != nil {
			break
		}

		if metadata["handshake_processed"] == nil {
			metadata["handshake_processed"] = true
			fullPacketBytes = maybeProcessHandshake(fullPacketBytes)
		}

		response = append(response, fullPacketBytes...)
	}

	return metadata, response, nil
}

func maybeProcessHandshake(msg []byte) []byte {
	// This function processes the initial handshake packet sent by the MySQL server.
	// https://dev.mysql.com/doc/dev/mysql-server/8.4.3/page_protocol_connection_phase_packets_protocol_handshake_v10.html

	// It rewrites capability_flags field, disabling SSL flag (CLIENT_SSL).
	reader := bytes.NewReader(msg)

	// Skip packet length and packet number
	packetLenNoData := make([]byte, 3+1)
	if n, err := reader.Read(packetLenNoData); err != nil || n != 3+1 {
		logger.Warn().Msg("While parsing handshake packet, error reading packet length and packet number")
		return msg
	}

	// Protocol version (1 byte)
	const PROTOCOL_VERSION = 10
	if b, err := reader.ReadByte(); err != nil || b != PROTOCOL_VERSION {
		logger.Warn().Msgf("While parsing handshake packet, unexpected protocol version: %d", b)
		return msg
	}

	// Server version (null-terminated string)
	for {
		b, err := reader.ReadByte()
		if err != nil {
			logger.Warn().Msg("While parsing handshake packet, error reading server version")
			return msg
		}
		if b == 0 {
			break
		}
	}

	// Thread id (4 bytes)
	threadIdBytes := make([]byte, 4)
	if n, err := reader.Read(threadIdBytes); err != nil || n != 4 {
		logger.Warn().Msg("While parsing handshake packet, error reading thread id")
		return msg
	}

	// auth-plugin-data-part-1 (8 bytes)
	authData := make([]byte, 8)
	if n, err := reader.Read(authData); err != nil || n != 8 {
		logger.Warn().Msg("While parsing handshake packet, error reading auth-plugin-data-part-1")
		return msg
	}

	// filler (1 byte)
	if _, err := reader.ReadByte(); err != nil {
		logger.Warn().Msg("While parsing handshake packet, error reading filler")
		return msg
	}

	// capability_flags_1 (2 bytes)
	capabilityFlagsBytes := make([]byte, 2)
	if n, err := reader.Read(capabilityFlagsBytes); err != nil || n != 2 {
		logger.Warn().Msg("While parsing handshake packet, error reading capability_flags_1")
		return msg
	}

	// Clear CLIENT_SSL
	const CLIENT_SSL = 2048
	capabilityFlags := binary.LittleEndian.Uint16(capabilityFlagsBytes)
	capabilityFlags &^= CLIENT_SSL
	pos := len(msg) - reader.Len() - 2
	binary.LittleEndian.PutUint16(msg[pos:pos+2], capabilityFlags)

	// Don't bother parsing the rest of the packet

	return msg
}

func maybeProcessComQuery(msg []byte) []byte {
	// COM_QUERY packet
	// https://dev.mysql.com/doc/dev/mysql-server/8.4.3/page_protocol_com_query.html

	const COM_QUERY = 0x03

	if len(msg) > 5 && msg[4] == COM_QUERY {
		sequenceId := msg[3]

		query := string(msg[5:])
		fmt.Println("Got query: ", query)

		// Potentially rewrite the query here:
		// query = strings.Replace(query, "foo", "bar", -1)

		// Serialize back:
		msg = make([]byte, 4)

		binary.LittleEndian.PutUint32(msg, uint32(len(query)+1))
		msg = msg[0:3]

		msg = append(msg, sequenceId)
		msg = append(msg, COM_QUERY)
		msg = append(msg, []byte(query)...)
	}
	return msg
}

func NewTcpMysqlPassthroughProcessor() *TcpPassthroughMysqlProcessor {
	return &TcpPassthroughMysqlProcessor{
		BaseProcessor: NewBaseProcessor(),
	}
}
