// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package quesma_api

import (
	"bytes"
	"context"
	"fmt"
)

type Quesma struct {
	pipelines    []PipelineBuilder
	dependencies Dependencies
}

func NewQuesma(deps Dependencies) *Quesma {
	return &Quesma{
		pipelines:    make([]PipelineBuilder, 0),
		dependencies: deps,
	}
}

func (quesma *Quesma) GetChildComponents() []any {

	componentList := make([]any, 0)

	for _, pipeline := range quesma.pipelines {
		componentList = append(componentList, pipeline)
	}

	return componentList
}

func (quesma *Quesma) AddPipeline(pipeline PipelineBuilder) {
	quesma.pipelines = append(quesma.pipelines, pipeline)
}

func (quesma *Quesma) GetPipelines() []PipelineBuilder {
	return quesma.pipelines
}

func (quesma *Quesma) Start() {
	for _, pipeline := range quesma.pipelines {
		quesma.dependencies.Logger().Info().Msgf("Starting pipeline %v", pipeline)
		pipeline.Start()
	}
}

func (quesma *Quesma) Stop(ctx context.Context) {
	for _, pipeline := range quesma.pipelines {
		for _, conn := range pipeline.GetFrontendConnectors() {
			conn.Stop(ctx)
		}
	}
	for _, pipeline := range quesma.pipelines {
		for _, conn := range pipeline.GetBackendConnectors() {
			conn.Close()
		}
	}
}

func (quesma *Quesma) buildInternal() (QuesmaBuilder, error) {

	endpoints := make(map[string]struct{})
	handlers := make(map[string]HandlersPipe)
	for _, pipeline := range quesma.pipelines {
		for _, conn := range pipeline.GetFrontendConnectors() {
			if httpConn, ok := conn.(HTTPFrontendConnector); ok {
				endpoints[conn.GetEndpoint()] = struct{}{}
				router := httpConn.GetRouter()
				for path, handlerWrapper := range router.GetHandlers() {
					handlerWrapper.Processors = append(handlerWrapper.Processors, pipeline.GetProcessors()...)
					handlers[path] = handlerWrapper
				}
			}
		}
	}
	if len(endpoints) == 1 {
		for _, pipeline := range quesma.pipelines {
			for _, conn := range pipeline.GetFrontendConnectors() {
				if httpConn, ok := conn.(HTTPFrontendConnector); ok {
					router := httpConn.GetRouter().Clone().(Router)
					if len(endpoints) == 1 {
						router.SetHandlers(handlers)
					}
					httpConn.AddRouter(router)

				}
			}
		}
	}

	for _, pipeline := range quesma.pipelines {
		backendConnectorTypesPerPipeline := make(map[BackendConnectorType]struct{})
		for _, conn := range pipeline.GetFrontendConnectors() {
			if tcpConn, ok := conn.(TCPFrontendConnector); ok {
				if len(pipeline.GetProcessors()) > 0 {
					tcpConn.GetConnectionHandler().SetHandlers(pipeline.GetProcessors())
				}
			}
		}
		backendConnectors := pipeline.GetBackendConnectors()
		for _, backendConnector := range backendConnectors {
			backendConnectorTypesPerPipeline[backendConnector.GetId()] = struct{}{}
		}
		for _, proc := range pipeline.GetProcessors() {
			supportedBackendConnectorsByProc := proc.GetSupportedBackendConnectors()
			for _, backendConnectorType := range supportedBackendConnectorsByProc {
				if _, ok := backendConnectorTypesPerPipeline[backendConnectorType]; !ok {
					return nil, fmt.Errorf("processor %v requires backend connector %v which is not provided", proc.GetId(), GetBackendConnectorNameFromType(backendConnectorType))
				}
			}
			proc.SetBackendConnectors(backendConnectors)
			if err := proc.Init(); err != nil {
				return nil, fmt.Errorf("processor %v failed to initialize: %v", proc.GetId(), err)
			}
		}

	}

	return quesma, nil
}

func (quesma *Quesma) injectDependencies(tree *ComponentTreeNode) error {
	if quesma.dependencies == nil {
		return fmt.Errorf("dependencies not set")
	}

	tree.walk(func(n *ComponentTreeNode) {
		quesma.dependencies.InjectDependenciesInto(n.Component)
	})

	return nil
}

func (quesma *Quesma) printTree(tree *ComponentTreeNode) {

	var buff bytes.Buffer

	_, _ = fmt.Fprintln(&buff, "Component tree:")

	tree.walk(func(n *ComponentTreeNode) {

		for i := 0; i < n.Level; i++ {
			_, _ = fmt.Fprint(&buff, "  ")
		}

		_, _ = fmt.Fprintln(&buff, n.Name)
	})

	quesma.dependencies.Logger().Debug().Msg(buff.String())
}

func (quesma *Quesma) Build() (QuesmaBuilder, error) {

	_, err := quesma.buildInternal()
	if err != nil {
		return nil, fmt.Errorf("failed to build quesma instance: %v", err)
	}

	treeBuilder := NewComponentToInitializeProviderBuilder()
	tree := treeBuilder.BuildComponentTree(quesma)

	err = quesma.injectDependencies(tree)
	if err != nil {
		return nil, fmt.Errorf("failed to inject dependencies: %v", err)
	}

	if traceDependencyInjection {
		quesma.printTree(tree)
	}

	return quesma, nil
}
