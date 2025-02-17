// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package quesma_api

import (
	"fmt"
)

type Pipeline struct {
	Name               string // Name is the user-defined name of the pipeline
	FrontendConnectors []FrontendConnector
	Processors         []Processor
	BackendConnectors  map[BackendConnectorType]BackendConnector
	logger             QuesmaLogger
}

func NewPipeline() *Pipeline {
	backendConnectors := make(map[BackendConnectorType]BackendConnector)
	backendConnectors[NoopBackend] = &NoopBackendConnector{}
	return &Pipeline{
		FrontendConnectors: make([]FrontendConnector, 0),
		Processors:         make([]Processor, 0),
		BackendConnectors:  backendConnectors,
	}
}

func NewNamedPipeline(name string) *Pipeline {
	backendConnectors := make(map[BackendConnectorType]BackendConnector)
	backendConnectors[NoopBackend] = &NoopBackendConnector{}
	return &Pipeline{
		Name:               name,
		FrontendConnectors: make([]FrontendConnector, 0),
		Processors:         make([]Processor, 0),
		BackendConnectors:  backendConnectors,
	}
}

func (p *Pipeline) GetChildComponents() []any {
	var components []any

	for _, conn := range p.FrontendConnectors {
		components = append(components, conn)
	}
	for _, proc := range p.Processors {
		components = append(components, proc)
	}
	for _, conn := range p.BackendConnectors {
		components = append(components, conn)
	}

	return components
}

func (p *Pipeline) SetDependencies(deps Dependencies) {
	p.logger = deps.Logger()
}

func (p *Pipeline) InstanceName() string {
	return fmt.Sprintf("pipeline(%p)", p) // TODO return name from config
}

func (p *Pipeline) AddFrontendConnector(conn FrontendConnector) {
	p.FrontendConnectors = append(p.FrontendConnectors, conn)
}

func (p *Pipeline) AddProcessor(proc Processor) {
	p.Processors = append(p.Processors, proc)
}

func (p *Pipeline) AddBackendConnector(conn BackendConnector) {
	p.BackendConnectors[conn.GetId()] = conn
}

func (p *Pipeline) Build() PipelineBuilder {
	return p
}

func (p *Pipeline) Start() {
	// TODO connectors for the same endpoint should be sharing the same listener
	// This is a temporary solution to start all connectors
	// some of them will fail to start
	// because the port is already in use
	// This works well from application perspective
	// because we are copying routing table from all connectors
	// however, bind error remains
	for _, conn := range p.FrontendConnectors {
		p.logger.Info().Msgf("Starting frontend connector %s", conn.GetEndpoint())
		go conn.Listen()
	}
}

func (p *Pipeline) GetFrontendConnectors() []FrontendConnector {
	return p.FrontendConnectors
}

func (p *Pipeline) GetProcessors() []Processor {
	return p.Processors
}

func (p *Pipeline) GetBackendConnectors() map[BackendConnectorType]BackendConnector {
	return p.BackendConnectors
}
