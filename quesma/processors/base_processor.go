// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package processors

import (
	"github.com/QuesmaOrg/quesma/quesma/logger"
	quesma_api "github.com/QuesmaOrg/quesma/quesma/v2/core"
)

// Query This is placeholder
// Concrete definition will be taken
// from `quesma/model/query.go`
type Query struct {
	Query string
}

// ExecutionPlan This is placeholder
// Concrete definition will be taken
// from `quesma/model/query.go`
type ExecutionPlan struct {
	Queries []*Query
}

// QueryResultTransformer This is a copy of the
// interface `ResultTransformer` from `quesma/model/transformers.go`
// from `quesma/model/transformers.go`
type QueryResultTransformer interface {
	TransformResults(results [][]QueryResultRow) [][]QueryResultRow
}

// QueryTransformer This is a copy of the
// interface `QueryTransformer` from `quesma/model/transformers.go`
// from `quesma/model/transformers.go`
type QueryTransformer interface {
	Transform(query []*Query) ([]*Query, error)
}

// QueryTransformationPipeline is the interface that parsing and composing
// `QueryTransformer` and `QueryResultTransformer`
// and makes body of BaseProcessor::Handle() method
type QueryTransformationPipeline interface {
	QueryTransformer
	QueryResultTransformer
	ParseQuery(message any) (*ExecutionPlan, error)
	ComposeResult(results [][]QueryResultRow) any
	AddTransformer(transformer QueryTransformer)
	GetTransformers() []QueryTransformer
}

// QueryResultRow This is a copy of the
// struct `QueryResultRow` from `quesma/model/query.go`
// and something that we should unify
type QueryResultRow struct {
}

// QueryExecutor is the interface that wraps the ExecuteQuery method.
type QueryExecutor interface {
	ExecuteQuery(query string) ([]QueryResultRow, error)
}

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

func (p *BaseProcessor) executeQuery(query string) ([]QueryResultRow, error) {
	logger.Debug().Msgf("BaseProcessor: executeQuery:%s", query)
	// This will be forwarded to the query execution engine
	return nil, nil
}

func (p *BaseProcessor) Handle(metadata map[string]interface{}, messages ...any) (map[string]interface{}, any, error) {
	logger.Debug().Msg("BaseProcessor: Handle")
	var resp any
	for _, msg := range messages {
		executionPlan, err := p.QueryTransformationPipeline.ParseQuery(msg)
		if err != nil {
			logger.Error().Err(err).Msg("Error parsing query")
		}
		queries, err := p.QueryTransformationPipeline.Transform(executionPlan.Queries)
		if err != nil {
			logger.Error().Err(err).Msg("Error transforming queries")
		}
		// Execute the queries
		var results [][]QueryResultRow
		for _, query := range queries {
			result, _ := p.executeQuery(query.Query)
			results = append(results, result)
		}
		// Transform the results
		transformedResults := p.QueryTransformationPipeline.TransformResults(results)
		resp = p.QueryTransformationPipeline.ComposeResult(transformedResults)
	}

	return metadata, resp, nil
}

func (p *BaseProcessor) RegisterTransformationPipeline(pipeline QueryTransformationPipeline) {
	p.QueryTransformationPipeline = pipeline
}
