// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

// Experimental alpha processor for MySQL protocol

package processors

import (
	"bytes"
	"encoding/binary"
	"io"
	"net"

	"github.com/QuesmaOrg/quesma/quesma/backend_connectors"
	"github.com/QuesmaOrg/quesma/quesma/frontend_connectors"
	"github.com/QuesmaOrg/quesma/quesma/logger"
	quesma_api "github.com/QuesmaOrg/quesma/quesma/v2/core"
)

type TcpPassthroughMySqlProcessor struct {
	BaseProcessor
}

func (t TcpPassthroughMySqlProcessor) InstanceName() string {
	return "TcpPassthroughMySqlProcessor"
}

func (t TcpPassthroughMySqlProcessor) GetId() string {
	return "TcpPassthroughMySqlProcessor"
}

func (t *TcpPassthroughMySqlProcessor) Handle(metadata map[string]interface{}, messages ...any) (map[string]interface{}, any, error) {
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

		if _, isEOF := m.(*frontend_connectors.TcpEOF); isEOF {
			conn.Close()
			return metadata, nil, nil
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

	eofCount := 0

	for {
		fullPacketBytes, err := frontend_connectors.ReadMySqlPacket(conn)
		if err != nil {
			if err != io.EOF {
				logger.Warn().Err(err).Msgf("Error reading MySQL packet from the MySQL TCP backend")
			} else {
				logger.Debug().Msg("Finished reading MySQL packets from the MySQL TCP backend (EOF), returning control to the frontend")
			}
			break
		}

		if metadata["handshake_processed"] == nil {
			metadata["handshake_processed"] = true
			fullPacketBytes = maybeProcessHandshake(fullPacketBytes)

			response = append(response, fullPacketBytes...)
			logger.Debug().Msg("Finished reading MySQL packets from the MySQL TCP backend (handshake), returning control to the frontend")
			break
		}

		logger.Debug().Msgf("Received packet from MySQL backend of length %d", len(fullPacketBytes))

		response = append(response, fullPacketBytes...)

		// Early exit - stop reading packets if we figure out that the server has sent the entire response:
		// an OK or ERR response or a result set (with two EOF packets).

		// https://dev.mysql.com/doc/dev/mysql-server/8.4.3/page_protocol_basic_ok_packet.html
		// Quote:
		// > These rules distinguish whether the packet represents OK or EOF:
		// > OK: header = 0 and length of packet > 7
		// > EOF: header = 0xfe and length of packet < 9
		//
		// Note: length of packet > 7 seems like a mistake in the documentation, it should be length of packet >= 7

		// OK packet
		const OK_PACKET = 0x00
		if len(fullPacketBytes) >= 7+4 && fullPacketBytes[4] == OK_PACKET {
			logger.Debug().Msg("Finished reading MySQL packets from the MySQL TCP backend (OK packet), returning control to the frontend")
			break
		}

		// ERROR packet
		const ERROR_PACKET = 0xFF
		if len(fullPacketBytes) > 4 && fullPacketBytes[4] == ERROR_PACKET {
			logger.Debug().Msg("Finished reading MySQL packets from the MySQL TCP backend (ERROR packet), returning control to the frontend")
			break
		}

		// EOF packet
		// there are two EOF packets in a result set: one after field metadata and one after rows
		const EOF_PACKET = 0xFE
		if len(fullPacketBytes) > 4 && len(fullPacketBytes) < 9+4 && fullPacketBytes[4] == EOF_PACKET {
			logger.Debug().Msg("Read EOF packet from the MySQL TCP backend")
			eofCount++
			if eofCount == 2 {
				logger.Debug().Msg("Finished reading MySQL packets from the MySQL TCP backend (two EOF packets), returning control to the frontend")
				break
			}
		}
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
		logger.Info().Msgf("Received query: %s", query)

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

func NewTcpMySqlPassthroughProcessor() *TcpPassthroughMySqlProcessor {
	return &TcpPassthroughMySqlProcessor{
		BaseProcessor: NewBaseProcessor(),
	}
}
