// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
// Experimental alpha processor for MySQL protocol

package processors

import (
	"encoding/binary"
	"fmt"
	"github.com/QuesmaOrg/quesma/quesma/backend_connectors"
	"github.com/QuesmaOrg/quesma/quesma/frontend_connectors"
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

		response = append(response, fullPacketBytes...)
	}

	return metadata, response, nil
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
