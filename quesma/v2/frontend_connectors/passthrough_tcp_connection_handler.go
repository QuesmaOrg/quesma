// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package frontend_connectors

import (
	"fmt"
	"io"
	"net"
	quesma_api "quesma_v2/core"
)

func (p *PassThroughConnectionHandler) copyData(src io.Reader, dest io.Writer) {
	if _, err := io.Copy(dest, src); err != nil {
		fmt.Printf("Error copying data: %v", err)
	}
}

type PassThroughConnectionHandler struct {
	endpoint string
}

func NewPassThroughConnectionHandler(endpoint string) *PassThroughConnectionHandler {
	return &PassThroughConnectionHandler{
		endpoint: endpoint,
	}
}

func (p *PassThroughConnectionHandler) SetHandlers(processors []quesma_api.Processor) {
}

func closeConnection(connection net.Conn) {
	if err := connection.Close(); err != nil {
		fmt.Printf("Error closing connection: %v", err)
	}
}
func (p *PassThroughConnectionHandler) handle(fromConn, destConn net.Conn) {
	fmt.Println("handle:", fromConn.RemoteAddr(), "->", destConn.RemoteAddr())
	defer closeConnection(fromConn)
	defer closeConnection(destConn)
	go p.copyData(fromConn, destConn)
	p.copyData(destConn, fromConn)
}

func (p *PassThroughConnectionHandler) HandleConnection(fromConn net.Conn) error {
	fmt.Println("Tcp connection handler")
	destConn, err := net.Dial("tcp", p.endpoint)
	if err != nil {
		closeConnection(fromConn)
		return err
	}
	p.handle(fromConn, destConn)
	return nil
}
