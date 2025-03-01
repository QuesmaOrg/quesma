// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package frontend_connectors

import (
	"fmt"
	quesma_api "github.com/QuesmaOrg/quesma/platform/v2/core"
	"io"
	"net"
)

type BasicTcpConnectionHandler struct {
	processors []quesma_api.Processor
}

func (h *BasicTcpConnectionHandler) SetHandlers(processors []quesma_api.Processor) {
	h.processors = processors
}

func (h *BasicTcpConnectionHandler) HandleConnection(conn net.Conn) error {
	fmt.Println("Handling connection")
	defer conn.Close()

	// Example: Read data from the connection
	buffer := make([]byte, 1024)
	for {
		n, err := conn.Read(buffer)
		if err != nil {
			if err == io.EOF {
				fmt.Println("Connection closed")
			} else {
				fmt.Println("Error reading from connection:", err)
			}
			return err
		}
		for _, processor := range h.processors {
			if processor != nil {
				processor.Handle(nil, buffer[:n])
			}
		}
		fmt.Printf("Received data: %s\n", string(buffer[:n]))

		// Echo the data back (for demonstration purposes)
		conn.Write(buffer[:n])
	}
}
