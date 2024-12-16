// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package quesma_api

import (
	"context"
	"net"
)

type Router interface {
	Cloner
	AddRoute(path string, handler HTTPFrontendHandler)
	AddFallbackHandler(handler HTTPFrontendHandler)
	GetFallbackHandler() HTTPFrontendHandler
	GetHandlers() map[string]HandlersPipe
	SetHandlers(handlers map[string]HandlersPipe)
	Register(pattern string, predicate RequestMatcher, handler HTTPFrontendHandler)
	Matches(req *Request) (*HandlersPipe, *Decision)
}

type FrontendConnector interface {
	Listen() error // Start listening on the endpoint
	GetEndpoint() string
	Stop(ctx context.Context) error // Stop listening
}

type HTTPFrontendConnector interface {
	FrontendConnector
	AddRouter(router Router)
	GetRouter() Router
}

type TCPFrontendConnector interface {
	FrontendConnector
	AddConnectionHandler(handler TCPConnectionHandler)
	GetConnectionHandler() TCPConnectionHandler
}

type TCPConnectionHandler interface {
	HandleConnection(conn net.Conn) error
	SetHandlers(processor []Processor)
}

type CompoundProcessor interface {
	AddProcessor(proc Processor)
	GetProcessors() []Processor
}

type PipelineBuilder interface {
	AddFrontendConnector(conn FrontendConnector)
	GetFrontendConnectors() []FrontendConnector
	AddBackendConnector(conn BackendConnector)
	GetBackendConnectors() map[BackendConnectorType]BackendConnector
	CompoundProcessor
	Build() PipelineBuilder
	Start()
}

type QuesmaBuilder interface {
	AddPipeline(pipeline PipelineBuilder)
	GetPipelines() []PipelineBuilder
	SetDependencies(dependencies *Dependencies)
	Build() (QuesmaBuilder, error)
	Start()
	Stop(ctx context.Context)
}

type Processor interface {
	CompoundProcessor
	GetId() string
	Handle(metadata map[string]interface{}, message ...any) (map[string]interface{}, any, error)
	SetBackendConnectors(conns map[BackendConnectorType]BackendConnector)
	GetBackendConnector(connectorType BackendConnectorType) BackendConnector
	GetSupportedBackendConnectors() []BackendConnectorType
	Init() error
}

type Rows interface {
	Next() bool
	Scan(dest ...interface{}) error
	Close()
	Err() error
}

type BackendConnector interface {
	GetId() BackendConnectorType
	Open() error
	// Query executes a query that returns rows, typically a SELECT.
	Query(ctx context.Context, query string, args ...interface{}) (Rows, error)

	// Exec executes a command that doesn't return rows, typically an INSERT, UPDATE, or DELETE.
	Exec(ctx context.Context, query string, args ...interface{}) error
	Close() error
}
