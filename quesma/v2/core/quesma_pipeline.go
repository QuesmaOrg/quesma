// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package quesma_api

type Pipeline struct {
	FrontendConnectors []FrontendConnector
	Processors         []Processor
	BackendConnectors  map[BackendConnectorType]BackendConnector
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
	for _, conn := range p.FrontendConnectors {
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
