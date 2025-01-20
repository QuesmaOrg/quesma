// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package processors

import (
	"fmt"
	"github.com/QuesmaOrg/quesma/quesma/backend_connectors"
	quesma_api "github.com/QuesmaOrg/quesma/quesma/v2/core"
	"github.com/jackc/pgx/v5/pgproto3"
)

type TcpPassthroughProcessor struct {
	BaseProcessor
	alreadySentHandshake bool
	alreadyReceivedReady bool
}

func (t TcpPassthroughProcessor) InstanceName() string {
	return "TcpPassthroughProcessor"
}

func (t TcpPassthroughProcessor) GetId() string {
	return "TcpPassthroughProcessor"
}

func (t *TcpPassthroughProcessor) Handle(metadata map[string]interface{}, messages ...any) (map[string]interface{}, any, error) {
	backendConnector := t.GetBackendConnector(quesma_api.TcpBackend).(*backend_connectors.TcpBackendConnector)
	//
	//if !t.alreadySentHandshake {
	//	t.alreadySentHandshake = true
	//	err := backendConnector.Write([]byte{0x0, 0x0, 0x0, 0x5a, 0x0, 0x3, 0x0, 0x0, 0x75, 0x73, 0x65, 0x72, 0x0, 0x70, 0x69, 0x6f, 0x74, 0x72, 0x67, 0x72, 0x61, 0x62, 0x6f, 0x77, 0x73, 0x6b, 0x69, 0x0, 0x64, 0x61, 0x74, 0x61, 0x62, 0x61, 0x73, 0x65, 0x0, 0x70, 0x6f, 0x73, 0x74, 0x67, 0x72, 0x65, 0x73, 0x0, 0x61, 0x70, 0x70, 0x6c, 0x69, 0x63, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x0, 0x70, 0x73, 0x71, 0x6c, 0x0, 0x63, 0x6c, 0x69, 0x65, 0x6e, 0x74, 0x5f, 0x65, 0x6e, 0x63, 0x6f, 0x64, 0x69, 0x6e, 0x67, 0x0, 0x55, 0x54, 0x46, 0x38, 0x0, 0x0})
	//	if err != nil {
	//		panic(err)
	//	}
	//}

	var fullMsg []byte

	for _, m := range messages {
		msg := m.(pgproto3.FrontendMessage)
		encoded := make([]byte, 0)
		encoded, err := msg.Encode(encoded)
		if err != nil {
			panic(err)
		}

		err = backendConnector.Write(encoded)
		if err != nil {
			panic(err)
		}

		if _, isSslRequest := msg.(*pgproto3.SSLRequest); isSslRequest {
			sslResponse, err := backendConnector.Read(1)
			if err != nil {
				panic(err)
			}
			fmt.Printf("Got SSL response: %v\n", sslResponse)
			fullMsg = append(fullMsg, sslResponse...)
		}

		fmt.Printf("%v %T %v\n", msg, msg, encoded)
	}

	//if !t.alreadyReceivedReady {
	//	t.alreadyReceivedReady = true
	//	for {
	//		// Read 4-byte length prefix
	//		lengthBytes, err := backendConnector.Read(5)
	//		if err != nil {
	//			panic(err)
	//		}
	//		//fmt.Printf("Got length (startup): %v\n", lengthBytes)
	//
	//		// Convert 4 bytes to big endian integer
	//		msgLength := int(lengthBytes[1])<<24 | int(lengthBytes[2])<<16 | int(lengthBytes[3])<<8 | int(lengthBytes[4]) - 4
	//
	//		// Read the rest of the message based on length
	//		_, err = backendConnector.Read(msgLength)
	//		if err != nil {
	//			panic(err)
	//		}
	//
	//		// Combine length prefix and message body
	//		//fullMsg = append(fullMsg, lengthBytes...)
	//		//fullMsg = append(fullMsg, msgBody...)
	//		//fmt.Printf("Got response (startup): %v\n", fullMsg)
	//
	//		if lengthBytes[0] == 90 /* Z - ready */ {
	//			break
	//		}
	//	}
	//}

	// Read 4-byte length prefix
	for {
		lengthBytes, err := backendConnector.Read(5)
		if err != nil {
			panic(err)
		}
		//fmt.Printf("Got length: %v\n", lengthBytes)
		if len(lengthBytes) == 0 {
			break
		}

		// Convert 4 bytes to big endian integer
		msgLength := int(lengthBytes[1])<<24 | int(lengthBytes[2])<<16 | int(lengthBytes[3])<<8 | int(lengthBytes[4]) - 4

		// Read the rest of the message based on length
		msgBody, err := backendConnector.Read(msgLength)
		if err != nil {
			panic(err)
		}

		// Combine length prefix and message body
		fullMsg = append(fullMsg, lengthBytes...)
		fullMsg = append(fullMsg, msgBody...)

		if lengthBytes[0] == 90 /* Z - ready */ {
			break
		}
	}

	//fmt.Printf("Got response: %v\n", fullMsg)
	//fmt.Printf("Will write: %v\n", fullMsg)
	return metadata, fullMsg, nil
}

func NewTcpPassthroughProcessor() *TcpPassthroughProcessor {
	return &TcpPassthroughProcessor{
		BaseProcessor: NewBaseProcessor(),
	}
}
