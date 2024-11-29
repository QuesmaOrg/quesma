package processors

import quesma_api "quesma_v2/core"

type BaseProcessor struct {
	InnerProcessors   []quesma_api.Processor
	BackendConnectors map[quesma_api.BackendConnectorType]quesma_api.BackendConnector
}

func NewBaseProcessor() BaseProcessor {
	return BaseProcessor{
		InnerProcessors:   make([]quesma_api.Processor, 0),
		BackendConnectors: make(map[quesma_api.BackendConnectorType]quesma_api.BackendConnector),
	}
}

func (p *BaseProcessor) AddProcessor(proc quesma_api.Processor) {
	p.InnerProcessors = append(p.InnerProcessors, proc)
}

func (p *BaseProcessor) GetProcessors() []quesma_api.Processor {
	return p.InnerProcessors
}

func (p *BaseProcessor) SetBackendConnectors(conns map[quesma_api.BackendConnectorType]quesma_api.BackendConnector) {
	p.BackendConnectors = conns
}

func (p *BaseProcessor) GetBackendConnector(connectorType quesma_api.BackendConnectorType) quesma_api.BackendConnector {
	if conn, ok := p.BackendConnectors[connectorType]; ok {
		return conn
	}
	return nil
}

func (p *BaseProcessor) GetSupportedBackendConnectors() []quesma_api.BackendConnectorType {
	return []quesma_api.BackendConnectorType{quesma_api.NoopBackend}
}
