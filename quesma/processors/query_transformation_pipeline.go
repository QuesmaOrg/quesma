// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package processors

import (
	"github.com/QuesmaOrg/quesma/quesma/model"
)

// QueryResultTransformer This is a copy of the
// interface `ResultTransformer` from `quesma/model/transformers.go`
// from `quesma/model/transformers.go`
type QueryResultTransformer interface {
	TransformResults(results [][]model.QueryResultRow) [][]model.QueryResultRow
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

// QueryResultRow This is a copy of the
// struct `QueryResultRow` from `quesma/model/query.go`
// and something that we should unify
type QueryResultRow struct {
}

// QueryExecutor is the interface that wraps the ExecuteQuery method.
type QueryExecutor interface {
	ExecuteQuery(query string) ([]QueryResultRow, error)
}
