// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package processors

import (
	"context"
	"github.com/QuesmaOrg/quesma/quesma/logger"
	"github.com/QuesmaOrg/quesma/quesma/model"
	quesma_api "github.com/QuesmaOrg/quesma/quesma/v2/core"
	"github.com/hashicorp/go-multierror"
)

type BaseProcessor struct {
	InnerProcessors             []quesma_api.Processor
	BackendConnectors           map[quesma_api.BackendConnectorType]quesma_api.BackendConnector
	QueryTransformationPipeline QueryTransformationPipeline
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

func (p *BaseProcessor) Init() error {
	return nil
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

func (p *BaseProcessor) executeQueries(queries []*model.Query) ([]model.QueryResultRow, error) {
	results := make([]model.QueryResultRow, 0)
	for _, query := range queries {
		logger.Debug().Msgf("BaseProcessor: executeQuery:%s", query.SelectCommand.String())
	}
	// This will be forwarded to the query execution engine
	return results, nil
}

func (p *BaseProcessor) Handle(metadata map[string]interface{}, messages ...any) (map[string]interface{}, any, error) {
	logger.Debug().Msg("BaseProcessor: Handle")
	var resp any
	var mError error
	for _, msg := range messages {
		executionPlan, err := p.QueryTransformationPipeline.ParseQuery(msg)
		if err != nil {
			mError = multierror.Append(mError, err)
		}
		queries, err := p.QueryTransformationPipeline.Transform(context.Background(), executionPlan.Queries)
		if err != nil {
			mError = multierror.Append(mError, err)
		}
		// Execute the queries
		var results [][]model.QueryResultRow
		result, _ := p.executeQueries(queries)
		results = append(results, result)
		// Transform the results
		transformedResults, err := p.QueryTransformationPipeline.TransformResults(results)
		if err != nil {
			mError = multierror.Append(mError, err)
		}
		resp = p.QueryTransformationPipeline.ComposeResult(transformedResults)
	}

	return metadata, resp, mError
}

func (p *BaseProcessor) RegisterTransformationPipeline(pipeline QueryTransformationPipeline) {
	p.QueryTransformationPipeline = pipeline
}
