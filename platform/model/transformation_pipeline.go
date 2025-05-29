// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package model

type TransformationPipeline struct {
	transformers []QueryTransformer
}

func NewTransformationPipeline() *TransformationPipeline {
	return &TransformationPipeline{}
}

func (o *TransformationPipeline) Transform(plan *ExecutionPlan) (*ExecutionPlan, error) {
	var err error
	for _, transformer := range o.transformers {
		plan, err = transformer.Transform(plan)
		if err != nil {
			return nil, err
		}
	}
	return plan, nil
}

func (o *TransformationPipeline) AddTransformer(transformer QueryTransformer) {
	o.transformers = append(o.transformers, transformer)
}
