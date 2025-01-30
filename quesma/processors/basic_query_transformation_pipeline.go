// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package processors

import (
	"github.com/QuesmaOrg/quesma/quesma/logger"
	"github.com/QuesmaOrg/quesma/quesma/model"
)

type BasicQueryTransformationPipeline struct {
	transformers []model.QueryTransformer
}

func NewBasicQueryTransformationPipeline() *BasicQueryTransformationPipeline {
	return &BasicQueryTransformationPipeline{}
}

func (p *BasicQueryTransformationPipeline) Transform(queries []*model.Query) ([]*model.Query, error) {
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

func (p *BasicQueryTransformationPipeline) AddTransformer(transformer model.QueryTransformer) {
	logger.Debug().Msg("SimpleQueryTransformationPipeline: AddTransformer")
	p.transformers = append(p.transformers, transformer)
}
