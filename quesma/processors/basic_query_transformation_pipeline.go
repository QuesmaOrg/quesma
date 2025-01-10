// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package processors

import (
	"github.com/QuesmaOrg/quesma/quesma/logger"
	quesma_api "github.com/QuesmaOrg/quesma/quesma/v2/core"
)

type BasicQueryTransformationPipeline struct {
	transformers []quesma_api.QueryTransformer
}

func NewBasicQueryTransformationPipeline() *BasicQueryTransformationPipeline {
	return &BasicQueryTransformationPipeline{}
}

func (p *BasicQueryTransformationPipeline) Transform(queries []*quesma_api.Query) ([]*quesma_api.Query, error) {
	logger.Debug().Msg("SimpleQueryTransformationPipeline: Transform")
	var err error
	for _, transformer := range p.transformers {
		queries, err = transformer.Transform(queries)
		if err != nil {
			return nil, err
		}
	}
	return queries, nil
}

func (p *BasicQueryTransformationPipeline) AddTransformer(transformer quesma_api.QueryTransformer) {
	logger.Debug().Msg("SimpleQueryTransformationPipeline: AddTransformer")
	p.transformers = append(p.transformers, transformer)
}

func (p *BasicQueryTransformationPipeline) GetTransformers() []quesma_api.QueryTransformer {
	logger.Debug().Msg("SimpleQueryTransformationPipeline: GetTransformers")
	return p.transformers
}
