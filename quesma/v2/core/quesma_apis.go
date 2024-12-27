// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package quesma_api

import (
	"context"
	"net"
	"net/http"
)

type InstanceNamer interface {
	InstanceName() string
}

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

// FrontendConnector is the components that listen for incoming requests
// They can be shared between multiple pipelines
type FrontendConnector interface {
	InstanceNamer
	// Listen starts listening on the endpoint
	Listen() error // Start listening on the endpoint
	GetEndpoint() string
	Stop(ctx context.Context) error // Stop listening
}

type HTTPFrontendConnector interface {
	FrontendConnector
	// AddRouter adds a router to the HTTPFrontendConnector
	AddRouter(router Router)
	GetRouter() Router
	// AddMiddleware adds a middleware to the HTTPFrontendConnector.
	// The middleware chain is executed in the order it is added
	// and before the router is executed.
	AddMiddleware(middleware http.Handler)
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
	Start(ctx context.Context)
}

type QuesmaBuilder interface {
	AddPipeline(pipeline PipelineBuilder)
	GetPipelines() []PipelineBuilder
	Build() (QuesmaBuilder, error)
	Start(ctx context.Context)
	Stop(ctx context.Context)
}

type Processor interface {
	InstanceNamer
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
	InstanceNamer
	GetId() BackendConnectorType
	Open() error
	// Query executes a query that returns rows, typically a SELECT.
	Query(ctx context.Context, query string, args ...interface{}) (Rows, error)

	// Exec executes a command that doesn't return rows, typically an INSERT, UPDATE, or DELETE.
	Exec(ctx context.Context, query string, args ...interface{}) error
	Close() error
}
