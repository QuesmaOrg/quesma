// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package model

import (
	"github.com/QuesmaOrg/quesma/platform/parsers/painful"
	"github.com/QuesmaOrg/quesma/platform/schema"
	quesma_api "github.com/QuesmaOrg/quesma/platform/v2/core"
	"time"
)

const (
	RowNumberColumnName = "row_number"
	noLimit             = 0
)

// QueryOptimizeHints contains hints for query execution, e.g., performance settings, temporary table usage
type QueryOptimizeHints struct {
	ClickhouseQuerySettings map[string]any // Clickhouse settings, e.g., use_query_cache, max_threads, etc. Added by the optimizers
	OptimizationsPerformed  []string       // List of optimizations performed by the optimizers
}

type TransformationHistory struct {
	SchemaTransformers []string
	// we may keep AST for each transformation here
	// or anything that will help to understand what was done
}

type AggregationType int

const (
	BucketAggregation AggregationType = iota
	MetricsAggregation
	PipelineMetricsAggregation // Pipeline aggregation that returns metrics
	PipelineBucketAggregation  // Pipeline aggregation that operate on buckets
	TypicalAggregation         // Not a real aggregation, but we reuse type
	UnknownAggregation
)

type PipelineAggregationType int

const (
	PipelineParentAggregation PipelineAggregationType = iota
	PipelineSiblingAggregation
)

func (s AggregationType) String() string {
	return [...]string{"BucketAggregation", "MetricsAggregation", "PipelineMetricsAggregation",
		"PipelineBucketAggregation", "TypicalAggregation", "UnknownAggregation"}[s]
}

type (
	Query struct {
		SelectCommand SelectCommand // The representation of SELECT query

		OptimizeHints         *QueryOptimizeHints   // it can be optional
		TransformationHistory TransformationHistory // it can be optional

		Type      QueryType
		TableName string // TODO delete this and use Indexes instead

		Indexes []string // list of indexes we're going to use for this query

		// this is schema for current query, this schema should be used in pipeline processing
		Schema schema.Schema

		Highlighter           Highlighter
		SearchAfter           any      // Value of query's "search_after" param. Used for pagination of hits. SearchAfterEmpty means no pagination
		SearchAfterFieldNames []string // Names of fields used in search_after. These can be different from the order by fields,

		RuntimeMappings map[string]RuntimeMapping

		// dictionary to add as 'meta' field in the response.
		// WARNING: it's probably not passed everywhere where it's needed, just in one place.
		// But it works for the test + our dashboards, so let's fix it later if necessary.
		// NoMetadataField (nil) is a valid option and means no meta field in the response.
		Metadata JsonMap
	}
	QueryType interface {
		// TranslateSqlResponseToJson
		// For 'bucket' aggregation result is a map wrapped in 'buckets' key.
		TranslateSqlResponseToJson(rows []QueryResultRow) JsonMap

		AggregationType() AggregationType

		String() string
	}
)

var SearchAfterEmpty any = nil

// RuntimeMapping is a mapping of a field to a runtime expression
type RuntimeMapping struct {
	Field                 string
	Type                  string
	DatabaseExpression    Expr
	PostProcessExpression painful.Expr
}

const MainExecutionPlan = "main"
const AlternativeExecutionPlan = "alternative"

type ExecutionPlan struct {
	Name string // Name of the execution plan, e.g., "main" or "alternative"

	IndexPattern string // Pattern for the index used in the execution plan

	Queries []*Query // List of queries included in the execution plan

	QueryRowsTransformers []QueryRowsTransformer // Transformers to process query result rows

	StartTime time.Time // Timestamp indicating when the execution plan started

	// Interrupt function to stop the execution of the plan
	// This function is invoked to determine if the execution should be stopped
	// based on certain conditions, e.g., when enough results are retrieved
	Interrupt func(queryId int, rows []QueryResultRow) bool

	// Function to merge results from sibling queries
	// This is used to combine results from related queries into a single result set
	MergeSiblingResults func(plan *ExecutionPlan, results [][]QueryResultRow) (*ExecutionPlan, [][]QueryResultRow)

	SiblingQueries map[int][]int // Map of query IDs to their sibling query IDs

	BackendConnector quesma_api.BackendConnector // Backend connector used for executing the queries
}

// NewExecutionPlan creates a new instance of model.ExecutionPlan
func NewExecutionPlan(queries []*Query, queryRowsTransformers []QueryRowsTransformer) *ExecutionPlan {
	return &ExecutionPlan{
		Queries:               queries,
		QueryRowsTransformers: queryRowsTransformers,
		SiblingQueries:        make(map[int][]int),
		Interrupt: func(queryId int, rows []QueryResultRow) bool {
			return false
		},
	}
}

func NewQueryExecutionHints() *QueryOptimizeHints {
	return &QueryOptimizeHints{ClickhouseQuerySettings: make(map[string]any)}
}

func NewSortColumn(field string, direction OrderByDirection) OrderByExpr {
	return NewOrderByExpr(NewColumnRef(field), direction)
}

var NoMetadataField JsonMap = nil

func (q *Query) NewSelectExprWithRowNumber(selectFields []Expr, groupByFields []Expr,
	whereClause Expr, orderByField string, orderByDesc bool) SelectCommand {
	orderBy := []OrderByExpr{}
	if orderByField != "" {
		if orderByDesc {
			orderBy = []OrderByExpr{NewOrderByExpr(NewColumnRef(orderByField), DescOrder)}
		} else {
			orderBy = []OrderByExpr{NewOrderByExpr(NewColumnRef(orderByField), AscOrder)}
		}
	}
	selectFields = append(selectFields, NewAliasedExpr(NewWindowFunction(
		"ROW_NUMBER", nil, groupByFields, orderBy,
	), RowNumberColumnName))

	return *NewSelectCommand(selectFields, nil, nil, q.SelectCommand.FromClause, whereClause, []Expr{}, 0, 0, false, []*CTE{})
}

type HitsInfo int // TODO/warning: right now difference between ListByField/ListAllFields/Normal is not very clear. It probably should be merged into 1 type.

const (
	ListByField HitsInfo = iota
	ListAllFields
	Normal
)

const (
	DefaultSizeListQuery = 10 // we use LIMIT 10 in some simple list queries (SELECT ...)
	TrackTotalHitsTrue   = -1
	TrackTotalHitsFalse  = -2
)

func (queryType HitsInfo) String() string {
	return []string{"ListByField", "ListAllFields", "Normal"}[queryType]
}

type HitsCountInfo struct {
	Type            HitsInfo
	RequestedFields []string
	Size            int // how many hits to return
	TrackTotalHits  int // >= 0: we want this nr of total hits, TrackTotalHitsTrue: it was "true", TrackTotalHitsFalse: it was "false", in the request
	SearchAfter     any // Value of query's "search_after" param. Used for pagination of hits. SearchAfterEmpty means no pagination
}

func NewEmptyHitsCountInfo() HitsCountInfo {
	return HitsCountInfo{Type: Normal}
}

func (q *Query) Clone() *Query {
	// Create a new Query object
	clone := &Query{
		SelectCommand:         q.SelectCommand, // Assuming SelectCommand has its own copy logic if needed
		OptimizeHints:         nil,
		TransformationHistory: q.TransformationHistory, // Assuming TransformationHistory is immutable or shallow copy is sufficient
		Type:                  q.Type,
		TableName:             q.TableName,
		Indexes:               append([]string{}, q.Indexes...), // Deep copy of slice
		Schema:                q.Schema,                         // Assuming schema.Schema is immutable or shallow copy is sufficient
		Highlighter:           q.Highlighter,                    // Assuming Highlighter is immutable or shallow copy is sufficient
		SearchAfter:           q.SearchAfter,                    // Assuming `any` is immutable or shallow copy is sufficient
		RuntimeMappings:       make(map[string]RuntimeMapping),
		Metadata:              nil,
	}

	// Deep copy OptimizeHints if it exists
	if q.OptimizeHints != nil {
		clone.OptimizeHints = &QueryOptimizeHints{
			ClickhouseQuerySettings: make(map[string]any),
			OptimizationsPerformed:  append([]string{}, q.OptimizeHints.OptimizationsPerformed...),
		}
		for k, v := range q.OptimizeHints.ClickhouseQuerySettings {
			clone.OptimizeHints.ClickhouseQuerySettings[k] = v
		}
	}

	// Deep copy RuntimeMappings
	for k, v := range q.RuntimeMappings {
		clone.RuntimeMappings[k] = v
	}

	// Deep copy Metadata if it exists
	if q.Metadata != nil {
		clone.Metadata = make(JsonMap)
		for k, v := range q.Metadata {
			clone.Metadata[k] = v
		}
	}

	return clone
}
