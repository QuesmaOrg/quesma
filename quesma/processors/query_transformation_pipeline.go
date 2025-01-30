// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package processors

import (
	"github.com/QuesmaOrg/quesma/quesma/model"
)

// QueryResultTransformer This is a copy of the
// interface `ResultTransformer` from `quesma/model/transformers.go`
// from `quesma/model/transformers.go`
// It's a copy as we can't embed model.ResultTransformer into QueryTransformationPipeline
// due to the same name of the method Transform()
type QueryResultTransformer interface {
	TransformResults(results [][]model.QueryResultRow) ([][]model.QueryResultRow, error)
}

// QueryTransformationPipeline is the interface that parsing and composing
// `QueryTransformer` and `QueryResultTransformer`
// and makes body of BaseProcessor::Handle() method
type QueryTransformationPipeline interface {
	model.QueryTransformer
	QueryResultTransformer
	ParseQuery(message any) (*model.ExecutionPlan, error)
	ComposeResult(results [][]model.QueryResultRow) any
	AddTransformer(transformer model.QueryTransformer)
	GetTransformers() []model.QueryTransformer
}
