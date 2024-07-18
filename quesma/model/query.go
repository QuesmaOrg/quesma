// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package model

import (
	"context"
)

const (
	RowNumberColumnName = "row_number"
	noLimit             = 0
)

// QueryOptimizeHints contains hints for query execution, e.g., performance settings, temporary table usage
type QueryOptimizeHints struct {
	Settings               map[string]any
	OptimizationsPerformed []string
}

type (
	Query struct {
		SelectCommand SelectCommand // The representation of SELECT query

		OptimizeHints *QueryOptimizeHints // it can be optional

		Type      QueryType
		TableName string

		Highlighter Highlighter
		NoDBQuery   bool         // true <=> we don't need query to DB here, true in some pipeline aggregations
		Parent      string       // parent aggregation name, used in some pipeline aggregations
		Aggregators []Aggregator // keeps names of aggregators, e.g. "0", "1", "2", "suggestions". Needed for JSON response.

		// dictionary to add as 'meta' field in the response.
		// WARNING: it's probably not passed everywhere where it's needed, just in one place.
		// But it works for the test + our dashboards, so let's fix it later if necessary.
		// NoMetadataField (nil) is a valid option and means no meta field in the response.
		Metadata JsonMap
	}
	QueryType interface {
		// TranslateSqlResponseToJson 'level' - we want to translate [level:] (metrics aggr) or [level-1:] (bucket aggr) columns to JSON
		// Previous columns are used for bucketing.
		// For 'bucket' aggregation result is a map wrapped in 'buckets' key.
		TranslateSqlResponseToJson(rows []QueryResultRow, level int) JsonMap

		PostprocessResults(rowsFromDB []QueryResultRow) (ultimateRows []QueryResultRow)

		// IsBucketAggregation if true, result from 'MakeResponse' will be a slice of buckets
		// if false, it's a metrics aggregation and result from 'MakeResponse' will be a single bucket
		IsBucketAggregation() bool
		String() string
	}
)

func NewQueryExecutionHints() *QueryOptimizeHints {
	return &QueryOptimizeHints{Settings: make(map[string]any)}
}

func NewSortColumn(field string, direction OrderByDirection) OrderByExpr {
	return NewOrderByExpr([]Expr{NewColumnRef(field)}, direction)
}

func NewSortByCountColumn(direction OrderByDirection) OrderByExpr {
	return NewOrderByExpr([]Expr{NewCountFunc()}, direction)
}

var NoMetadataField JsonMap = nil

// CopyAggregationFields copies all aggregation fields from qwa to q
func (q *Query) CopyAggregationFields(qwa Query) {
	q.SelectCommand.GroupBy = make([]Expr, len(qwa.SelectCommand.GroupBy))
	copy(q.SelectCommand.GroupBy, qwa.SelectCommand.GroupBy)

	q.SelectCommand.OrderBy = make([]OrderByExpr, len(qwa.SelectCommand.OrderBy))
	copy(q.SelectCommand.OrderBy, qwa.SelectCommand.OrderBy)

	q.SelectCommand.LimitBy = make([]Expr, len(qwa.SelectCommand.LimitBy))
	copy(q.SelectCommand.LimitBy, qwa.SelectCommand.LimitBy)

	q.SelectCommand.Columns = make([]Expr, len(qwa.SelectCommand.Columns))
	copy(q.SelectCommand.Columns, qwa.SelectCommand.Columns)

	q.SelectCommand.OrderBy = make([]OrderByExpr, len(qwa.SelectCommand.OrderBy))
	copy(q.SelectCommand.OrderBy, qwa.SelectCommand.OrderBy)

	q.SelectCommand.CTEs = make([]*SelectCommand, len(qwa.SelectCommand.CTEs))
	copy(q.SelectCommand.CTEs, qwa.SelectCommand.CTEs)

	q.Aggregators = make([]Aggregator, len(qwa.Aggregators))
	copy(q.Aggregators, qwa.Aggregators)

	// new
	q.SelectCommand.newColumns = make([]Expr, len(qwa.SelectCommand.newColumns))
	copy(q.SelectCommand.newColumns, qwa.SelectCommand.newColumns)

	q.SelectCommand.newGroupBy = make([]Expr, len(qwa.SelectCommand.newGroupBy))
	copy(q.SelectCommand.newGroupBy, qwa.SelectCommand.newGroupBy)
}

func (q *Query) AddColumn(column Expr) {
	q.SelectCommand.Columns = append(q.SelectCommand.Columns, column)
}

func (q *Query) AddColumnNew(column Expr) {
	q.SelectCommand.newColumns = append(q.SelectCommand.newColumns, column)
}

func (q *Query) AddGroupBy(column Expr) {
	q.SelectCommand.GroupBy = append(q.SelectCommand.GroupBy, column)
}

func (q *Query) AddGroupByNew(column Expr) {
	q.SelectCommand.newGroupBy = append(q.SelectCommand.newGroupBy, column)
}

func (q *Query) AddFullGroupByNew(column Expr) {
	q.SelectCommand.newFullGroupBy = append(q.SelectCommand.newFullGroupBy, column)
}

func (q *Query) AddSize(size int) {
	q.SelectCommand.newGroupBySize = append(q.SelectCommand.newGroupBySize, size)
}

func (q *Query) AddOrderBy(orderBy []OrderByExpr) {
	q.SelectCommand.newOrderBy = append(q.SelectCommand.newOrderBy, orderBy)
}

func (q *Query) PopNewGroupBy() {
	q.SelectCommand.newGroupBy = q.SelectCommand.newGroupBy[:len(q.SelectCommand.newGroupBy)-1]
}

// Name returns the name of this aggregation (specifically, the last aggregator)
// So for nested aggregation {"a": {"b": {"c": this aggregation}}}, it returns "c".
// In some queries aggregations are referenced by full name, so "a>b>c", but so far this implementation seems sufficient.
func (q *Query) Name() string {
	if len(q.Aggregators) == 0 {
		return ""
	}
	return q.Aggregators[len(q.Aggregators)-1].Name
}

// HasParentAggregation returns true <=> this aggregation has a parent aggregation, so there's no query to the DB,
// and results are calculated based on parent aggregation's results.
func (q *Query) HasParentAggregation() bool {
	return q.NoDBQuery && len(q.Parent) > 0 // first condition should be enough, second just in case
}

// IsChild returns true <=> this aggregation is a child of maybeParent (so maybeParent is its parent).
func (q *Query) IsChild(maybeParent *Query) bool {
	return q.HasParentAggregation() && q.Parent == maybeParent.Name()
}

func (q *Query) NewSelectExprWithRowNumber(selectFields []Expr, groupByFields []Expr,
	whereClause Expr, orderByField string, orderByDesc bool) SelectCommand {
	var orderByExpr OrderByExpr
	if orderByField != "" {
		if orderByDesc {
			orderByExpr = NewOrderByExpr([]Expr{NewColumnRef(orderByField)}, DescOrder)
		} else {
			orderByExpr = NewOrderByExpr([]Expr{NewColumnRef(orderByField)}, AscOrder)
		}
	}
	selectFields = append(selectFields, NewAliasedExpr(NewWindowFunction(
		"ROW_NUMBER", nil, groupByFields, orderByExpr,
	), RowNumberColumnName))

	return *NewSelectCommand(selectFields, nil, nil, q.SelectCommand.FromClause, whereClause, []Expr{}, 0, 0, false, []*SelectCommand{})
}

// Aggregator is always initialized as "empty", so with SplitOverHowManyFields == 0, Keyed == false, Filters == false.
// It's updated after construction, during further processing of aggregations.
type Aggregator struct {
	Name                   string
	SplitOverHowManyFields int  // normally 0 or 1, currently only multi_terms have > 1, as we split over multiple fields on one level.
	Keyed                  bool // determines how results are returned in response's JSON
	Filters                bool // if true, this aggregator is a filters aggregator
}

// NewAggregator (the only constructor) initializes Aggregator as "empty", so with SplitOverHowManyFields == 0, Keyed == false, Filters == false.
// It's updated after construction, during further processing of aggregations.
func NewAggregator(name string) Aggregator {
	return Aggregator{Name: name}
}

type SearchQueryType int // TODO/warning: right now difference between ListByField/ListAllFields/Normal is not very clear. It probably should be merged into 1 type.

const (
	Facets SearchQueryType = iota
	FacetsNumeric
	ListByField
	ListAllFields
	Normal
)

const (
	DefaultSizeListQuery = 10 // we use LIMIT 10 in some simple list queries (SELECT ...)
	TrackTotalHitsTrue   = -1
	TrackTotalHitsFalse  = -2
)

func (queryType SearchQueryType) String() string {
	return []string{"Facets", "FacetsNumeric", "ListByField", "ListAllFields", "Normal"}[queryType]
}

type SearchQueryInfo struct {
	Typ SearchQueryType
	// to be used as replacement for FieldName
	RequestedFields []string
	// deprecated
	FieldName      string
	I1             int
	I2             int
	Size           int // how many hits to return
	TrackTotalHits int // >= 0: we want this nr of total hits, TrackTotalHitsTrue: it was "true", TrackTotalHitsFalse: it was "false", in the request
}

func NewSearchQueryInfoNormal() SearchQueryInfo {
	return SearchQueryInfo{Typ: Normal}
}

// UnknownAggregationType is a placeholder for an aggregation type that'll be determined in the future,
// after descending further into the aggregation tree
type UnknownAggregationType struct {
	ctx context.Context
}

func NewUnknownAggregationType(ctx context.Context) UnknownAggregationType {
	return UnknownAggregationType{ctx: ctx}
}

func (query UnknownAggregationType) IsBucketAggregation() bool {
	return false
}

func (query UnknownAggregationType) TranslateSqlResponseToJson(rows []QueryResultRow, level int) JsonMap {
	return make(JsonMap, 0)
}

func (query UnknownAggregationType) String() string {
	return "unknown aggregation type"
}

func (query UnknownAggregationType) PostprocessResults(rowsFromDB []QueryResultRow) []QueryResultRow {
	return rowsFromDB
}
