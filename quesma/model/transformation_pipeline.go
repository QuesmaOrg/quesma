// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package model

import "context"

type TransformationPipeline struct {
	transformers []QueryTransformer
}

func NewTransformationPipeline() *TransformationPipeline {
	return &TransformationPipeline{}
}

func (o *TransformationPipeline) Transform(ctx context.Context, queries []*Query) ([]*Query, error) {
	var err error
	for _, transformer := range o.transformers {
		queries, err = transformer.Transform(ctx, queries)
		if err != nil {
			return nil, err
		}
	}
	return queries, nil
}

func (o *TransformationPipeline) AddTransformer(transformer QueryTransformer) {
	o.transformers = append(o.transformers, transformer)
}
