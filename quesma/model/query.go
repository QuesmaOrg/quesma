package model

import (
	"context"
	"fmt"
	"mitmproxy/quesma/queryparser/aexp"
	"sort"
	"strings"
)

const (
	RowNumberColumnName = "row_number"
	EmptyFieldSelection = "''" // we can query SELECT '', that's why such quotes
)

type (
	SelectColumn struct {
		Alias      string
		Expression aexp.AExp
	}

	Query struct {
		IsDistinct bool // true <=> query is SELECT DISTINCT

		// This is the future.
		Columns []SelectColumn // Columns to select, including aliases

		// TO BE REMOVED
		xFields          []string // Fields in 'SELECT Fields FROM ...'
		xNonSchemaFields []string // Fields that are not in schema, but are in 'SELECT ...', e.g. count()

		WhereClause   string   // "WHERE ..." until next clause like GROUP BY/ORDER BY, etc.
		GroupByFields []string // if not empty, we do GROUP BY GroupByFields... They are quoted if they are column names, unquoted if non-schema. So no quotes need to be added.
		SuffixClauses []string // ORDER BY, etc.
		FromClause    string   // usually just "tableName", or databaseName."tableName". Sometimes a subquery e.g. (SELECT ...)
		CanParse      bool     // true <=> query is valid
		QueryInfo     SearchQueryInfo
		Highlighter   Highlighter
		NoDBQuery     bool         // true <=> we don't need query to DB here, true in some pipeline aggregations
		Parent        string       // parent aggregation name, used in some pipeline aggregations
		Aggregators   []Aggregator // keeps names of aggregators, e.g. "0", "1", "2", "suggestions". Needed for JSON response.
		Type          QueryType
		SortFields    SortFields // fields to sort by
		SubSelect     string
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
	SortFields []SortField
	SortField  struct {
		Field string
		Desc  bool
	}
)

func (c SelectColumn) SQL() string {

	if c.Expression == nil {
		panic("SelectColumn expression is nil")
	}

	exprAsString := aexp.RenderSQL(c.Expression)

	if c.Alias == "" {
		return exprAsString
	}

	return fmt.Sprintf("%s AS \"%s\"", exprAsString, c.Alias)
}

func (c SelectColumn) String() string {
	return fmt.Sprintf("SelectColumn(Alias: '%s', expression: '%v')", c.Alias, c.Expression)
}

func (sf SortFields) Properties() []string {
	properties := make([]string, 0)
	for _, sortField := range sf {
		properties = append(properties, sortField.Field)
	}
	return properties
}

var NoMetadataField JsonMap = nil

// returns string with SQL query
func (q *Query) String() string {
	return q.StringFromColumns([]string{})
}

func (q *Query) StringFromColumns(colNames []string) string {

	// render based on Columns
	newSQL := q.StringFromColumnsNew(colNames)

	// we return old SQL for now
	return newSQL
}

// returns string with SQL query
// colNames - list of columns (schema fields) for SELECT
func (q *Query) StringFromColumnsNew(colNames []string) string {
	var sb strings.Builder
	sb.WriteString("SELECT ")
	if q.IsDistinct {
		sb.WriteString("DISTINCT ")
	}

	columns := make([]string, 0)

	if len(q.Columns) == 1 && q.Columns[0].Expression == aexp.Wildcard && len(colNames) > 0 {

		for _, col := range colNames {

			if col == "*" || col == EmptyFieldSelection {
				columns = append(columns, SelectColumn{Expression: aexp.Wildcard}.SQL())
			} else {
				columns = append(columns, SelectColumn{Expression: aexp.TableColumn(col)}.SQL())
			}
		}

		//columns = append(columns, "*")
	} else {
		for _, col := range q.Columns {
			if col.Expression == nil {
				// this is paraonoid check, it should never happen
				panic("SelectColumn expression is nil")
			} else {
				columns = append(columns, col.SQL())
			}
		}
	}

	sb.WriteString(strings.Join(columns, ", "))

	sb.WriteString(" FROM ")
	sb.WriteString(q.FromClause)

	if len(q.WhereClause) > 0 {
		sb.WriteString(" WHERE ")
		sb.WriteString(q.WhereClause)
	}

	if len(q.GroupByFields) > 0 {
		sb.WriteString(" GROUP BY (")
		for i, field := range q.GroupByFields {
			sb.WriteString(field)
			if i < len(q.GroupByFields)-1 {
				sb.WriteString(", ")
			}
		}
		sb.WriteString(")")

		if len(q.SuffixClauses) == 0 {
			sb.WriteString(" ORDER BY (")
			for i, field := range q.GroupByFields {
				sb.WriteString(field)
				if i < len(q.GroupByFields)-1 {
					sb.WriteString(", ")
				}
			}
			sb.WriteString(")")
		}
	}
	if len(q.SuffixClauses) > 0 {
		sb.WriteString(" " + strings.Join(q.SuffixClauses, " "))
	}
	return sb.String()
}

func (q *Query) IsWildcard() bool {

	for _, col := range q.Columns {
		if col.Expression == aexp.Wildcard {
			return true
		}
	}

	return false
}

// CopyAggregationFields copies all aggregation fields from qwa to q
func (q *Query) CopyAggregationFields(qwa Query) {
	q.GroupByFields = make([]string, len(qwa.GroupByFields))
	copy(q.GroupByFields, qwa.GroupByFields)

	q.Columns = make([]SelectColumn, len(qwa.Columns))
	copy(q.Columns, qwa.Columns)

	q.Aggregators = make([]Aggregator, len(qwa.Aggregators))
	copy(q.Aggregators, qwa.Aggregators)
}

// RemoveEmptyGroupBy removes EmptyFieldSelection from GroupByFields
func (q *Query) RemoveEmptyGroupBy() {
	nonEmptyFields := make([]string, 0)
	for _, field := range q.GroupByFields {
		if field != EmptyFieldSelection {
			nonEmptyFields = append(nonEmptyFields, field)
		}
	}
	q.GroupByFields = nonEmptyFields
}

// TrimKeywordFromFields trims .keyword from fields and group by fields
// In future probably handle it in a better way
func (q *Query) TrimKeywordFromFields() {

	for i := range q.GroupByFields {
		if strings.HasSuffix(q.GroupByFields[i], `.keyword"`) {
			q.GroupByFields[i] = strings.TrimSuffix(q.GroupByFields[i], `.keyword"`)
			q.GroupByFields[i] += `"`
		}
	}

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
func (q *Query) IsChild(maybeParent Query) bool {
	return q.HasParentAggregation() && q.Parent == maybeParent.Name()
}

type Aggregator struct {
	Name    string
	Empty   bool // is this aggregator empty, so no buckets
	Keyed   bool // determines how results are returned in response's JSON
	Filters bool // if true, this aggregator is a filters aggregator
}

func NewAggregatorEmpty(name string) Aggregator {
	return Aggregator{Name: name, Empty: true}
}

type SearchQueryType int

const (
	Facets SearchQueryType = iota
	FacetsNumeric
	ListByField
	ListAllFields
	CountAsync
	Normal
	None
)

const DefaultSizeListQuery = 1000 // we use LIMIT 1000 in some simple list queries (SELECT ...)

func (queryType SearchQueryType) String() string {
	return []string{"Facets", "FacetsNumeric", "ListByField", "ListAllFields", "CountAsync", "Normal", "None"}[queryType]
}

type SearchQueryInfo struct {
	Typ SearchQueryType
	// to be used as replacement for FieldName
	RequestedFields []string
	// deprecated
	FieldName string
	Interval  string
	I1        int
	I2        int
	Size      int // how many hits to return
}

func NewSearchQueryInfoNone() SearchQueryInfo {
	return SearchQueryInfo{Typ: None}
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
