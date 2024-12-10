// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package model

import (
	"quesma/painful"
	"quesma/schema"
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
		SelectCommand            SelectCommand // The representation of SELECT query
		AlternativeSelectCommand *SelectCommand

		OptimizeHints         *QueryOptimizeHints   // it can be optional
		TransformationHistory TransformationHistory // it can be optional

		Type      QueryType
		TableName string // TODO delete this and use Indexes instead

		Indexes []string // list of indexes we're going to use for this query

		// this is schema for current query, this schema should be used in pipeline processing
		Schema schema.Schema

		Highlighter Highlighter

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
	Name string

	IndexPattern string

	Queries []*Query

	QueryRowsTransformers []QueryRowsTransformer

	// add more fields here
	// JSON renderers
	StartTime time.Time
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
	Typ             HitsInfo
	RequestedFields []string
	Size            int // how many hits to return
	TrackTotalHits  int // >= 0: we want this nr of total hits, TrackTotalHitsTrue: it was "true", TrackTotalHitsFalse: it was "false", in the request
}

func NewEmptyHitsCountInfo() HitsCountInfo {
	return HitsCountInfo{Typ: Normal}
}
