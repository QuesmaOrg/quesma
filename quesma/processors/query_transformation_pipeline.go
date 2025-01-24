// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package processors

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
