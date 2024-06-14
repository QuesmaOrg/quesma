package model

import (
	"context"
	"sort"
	"strings"
)

const (
	RowNumberColumnName = "row_number"
	noLimit             = 0
)

type (
	Query struct {
		SelectCommand SelectCommand // The representation of SELECT query
		CanParse      bool          // true <=> query is valid

		// Eventually we should merge this two
		QueryInfoType SearchQueryType
		Type          QueryType
		TableName     string

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
		// For 'bucket' aggregation result is a slice of buckets, for 'metrics' aggregation it's a single bucket (only look at [0])
		TranslateSqlResponseToJson(rows []QueryResultRow, level int) []JsonMap

		PostprocessResults(rowsFromDB []QueryResultRow) (ultimateRows []QueryResultRow)

		// IsBucketAggregation if true, result from 'MakeResponse' will be a slice of buckets
		// if false, it's a metrics aggregation and result from 'MakeResponse' will be a single bucket
		IsBucketAggregation() bool
		String() string
	}
	Highlighter struct {
		Tokens []string
		Fields map[string]bool

		PreTags  []string
		PostTags []string
	}
)

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

	q.SelectCommand.Columns = make([]Expr, len(qwa.SelectCommand.Columns))
	copy(q.SelectCommand.Columns, qwa.SelectCommand.Columns)

	q.Aggregators = make([]Aggregator, len(qwa.Aggregators))
	copy(q.Aggregators, qwa.Aggregators)
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

	return *NewSelectCommand(selectFields, nil, nil, q.SelectCommand.FromClause, whereClause, 0, 0, false)
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

func (h *Highlighter) ShouldHighlight(columnName string) bool {
	_, ok := h.Fields[columnName]
	return ok
}

func (h *Highlighter) HighlightValue(value string) []string {

	//https://www.elastic.co/guide/en/elasticsearch/reference/current/highlighting.html
	// https://medium.com/@andre.luiz1987/using-highlighting-elasticsearch-9ccd698f08

	// paranoia check for empty tags
	if len(h.PreTags) < 1 && len(h.PostTags) < 1 {
		return []string{}
	}

	type match struct {
		start int
		end   int
	}

	var matches []match

	lowerValue := strings.ToLower(value)
	length := len(lowerValue)

	// find all matches
	for _, token := range h.Tokens {

		if token == "" {
			continue
		}

		pos := 0
		for pos < length {
			// token are lower cased already
			idx := strings.Index(lowerValue[pos:], token)
			if idx == -1 {
				break
			}

			start := pos + idx
			end := start + len(token)

			matches = append(matches, match{start, end})
			pos = end
		}
	}

	if len(matches) == 0 {
		return []string{}
	}

	// sort matches by start position
	sort.Slice(matches, func(i, j int) bool {
		return matches[i].start < matches[j].start
	})

	var mergedMatches []match

	// merge overlapping matches
	for i := 0; i < len(matches); i++ {
		lastMerged := len(mergedMatches) - 1

		if len(mergedMatches) > 0 && matches[i].start <= mergedMatches[len(mergedMatches)-1].end {
			mergedMatches[lastMerged].end = max(matches[i].end, mergedMatches[lastMerged].end)
		} else {
			mergedMatches = append(mergedMatches, matches[i])
		}
	}

	// populate highlights
	var highlights []string
	for _, m := range mergedMatches {
		highlights = append(highlights, h.PreTags[0]+value[m.start:m.end]+h.PostTags[0])
	}

	return highlights
}

func (h *Highlighter) SetTokens(tokens []string) {

	uniqueTokens := make(map[string]bool)
	for _, token := range tokens {
		uniqueTokens[strings.ToLower(token)] = true
	}

	h.Tokens = make([]string, 0, len(uniqueTokens))
	for token := range uniqueTokens {
		h.Tokens = append(h.Tokens, token)
	}

	// longer tokens firsts
	sort.Slice(h.Tokens, func(i, j int) bool {
		return len(h.Tokens[i]) > len(h.Tokens[j])
	})
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

func (query UnknownAggregationType) TranslateSqlResponseToJson(rows []QueryResultRow, level int) []JsonMap {
	return make([]JsonMap, 0)
}

func (query UnknownAggregationType) String() string {
	return "unknown aggregation type"
}

func (query UnknownAggregationType) PostprocessResults(rowsFromDB []QueryResultRow) []QueryResultRow {
	return rowsFromDB
}
