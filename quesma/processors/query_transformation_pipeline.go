// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package processors

import (
	"github.com/QuesmaOrg/quesma/quesma/model"
	"github.com/QuesmaOrg/quesma/quesma/schema"
	"time"
)

// Query This is placeholder
// Concrete definition will be taken
// from `quesma/model/query.go`
type Query struct {
	Query         string
	SelectCommand model.SelectCommand // The representation of SELECT query

	OptimizeHints         *model.QueryOptimizeHints   // it can be optional
	TransformationHistory model.TransformationHistory // it can be optional

	Type      model.QueryType
	TableName string // TODO delete this and use Indexes instead

	Indexes []string // list of indexes we're going to use for this query

	// this is schema for current query, this schema should be used in pipeline processing
	Schema schema.Schema

	Highlighter model.Highlighter
	SearchAfter any // Value of query's "search_after" param. Used for pagination of hits. SearchAfterEmpty means no pagination

	RuntimeMappings map[string]model.RuntimeMapping

	// dictionary to add as 'meta' field in the response.
	// WARNING: it's probably not passed everywhere where it's needed, just in one place.
	// But it works for the test + our dashboards, so let's fix it later if necessary.
	// NoMetadataField (nil) is a valid option and means no meta field in the response.
	Metadata model.JsonMap
}

// ExecutionPlan This is placeholder
// Concrete definition will be taken
// from `quesma/model/query.go`
type ExecutionPlan struct {
	Name string

	IndexPattern string

	Queries []*Query

	QueryRowsTransformers []model.QueryRowsTransformer

	// add more fields here
	// JSON renderers
	StartTime time.Time
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
