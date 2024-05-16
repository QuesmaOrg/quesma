package model

import (
	"context"
	"mitmproxy/quesma/logger"
	"sort"
	"strconv"
	"strings"
)

const RowNumberColumnName = "row_number"
const EmptyFieldSelection = "''" // we can query SELECT '', that's why such quotes

type Highlighter struct {
	Tokens []string
	Fields map[string]bool

	PreTags  []string
	PostTags []string
}

// implements String() (now) and MakeResponse() interface (in the future (?))
type Query struct {
	IsDistinct      bool     // true <=> query is SELECT DISTINCT
	Fields          []string // Fields in 'SELECT Fields FROM ...'
	NonSchemaFields []string // Fields that are not in schema, but are in 'SELECT ...', e.g. count()
	WhereClause     string   // "WHERE ..." until next clause like GROUP BY/ORDER BY, etc.
	GroupByFields   []string // if not empty, we do GROUP BY GroupByFields... They are quoted if they are column names, unquoted if non-schema. So no quotes need to be added.
	SuffixClauses   []string // ORDER BY, etc.
	FromClause      string   // usually just "tableName", or databaseName."tableName". Sometimes a subquery e.g. (SELECT ...)
	CanParse        bool     // true <=> query is valid
	QueryInfo       SearchQueryInfo
	Highlighter     Highlighter
	NoDBQuery       bool     // true <=> we don't need query to DB here, true in some pipeline aggregations
	Parent          string   // parent aggregation name, used in some pipeline aggregations
	SortFields      []string // fields to sort by
}

var NoMetadataField JsonMap = nil

// implements String() (now) and MakeResponse() interface (in the future (?))
type QueryWithAggregation struct {
	Query
	Aggregators []Aggregator // keeps names of aggregators, e.g. "0", "1", "2", "suggestions". Needed for JSON response.
	Type        QueryType
	// dictionary to add as 'meta' field in the response.
	// WARNING: it's probably not passed everywhere where it's needed, just in one place.
	// But it works for the test + our dashboards, so let's fix it later if necessary.
	// NoMetadataField (nil) is a valid option and means no meta field in the response.
	Metadata JsonMap
}

// returns string with * in SELECT
func (q *Query) String() string {
	var sb strings.Builder
	sb.WriteString("SELECT ")
	if q.IsDistinct {
		sb.WriteString("DISTINCT ")
	}
	for i, field := range q.Fields {
		if field == "*" || field == EmptyFieldSelection {
			sb.WriteString(field)
		} else {
			sb.WriteString(strconv.Quote(field))
		}
		if i < len(q.Fields)-1 || len(q.NonSchemaFields) > 0 {
			sb.WriteString(", ")
		}
	}
	for i, field := range q.NonSchemaFields {
		sb.WriteString(field)
		if i < len(q.NonSchemaFields)-1 {
			sb.WriteString(", ")
		}
	}
	where := " WHERE "
	if len(q.WhereClause) == 0 {
		where = ""
	}
	sb.WriteString(" FROM " + q.FromClause + where + q.WhereClause + " ")
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

// returns string without * in SELECT
// colNames - list of columns (schema fields) for SELECT
func (q *Query) StringFromColumns(colNames []string) string {
	var sb strings.Builder
	sb.WriteString("SELECT ")
	if q.IsDistinct {
		sb.WriteString("DISTINCT ")
	}
	for i, field := range colNames {
		if field == "*" || field == EmptyFieldSelection {
			sb.WriteString(field)
		} else {
			sb.WriteString(strconv.Quote(field))
		}
		if i < len(colNames)-1 || len(q.NonSchemaFields) > 0 {
			sb.WriteString(", ")
		}
	}
	for i, field := range q.NonSchemaFields {
		sb.WriteString(field)
		if i < len(q.NonSchemaFields)-1 {
			sb.WriteString(", ")
		}
	}
	where := " WHERE "
	if len(q.WhereClause) == 0 {
		where = ""
	}
	sb.WriteString(" FROM " + q.FromClause + where + q.WhereClause + " ")
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
	return len(q.Fields) == 1 && q.Fields[0] == "*"
}

// CopyAggregationFields copies all aggregation fields from qwa to q
func (q *QueryWithAggregation) CopyAggregationFields(qwa QueryWithAggregation) {
	q.GroupByFields = make([]string, len(qwa.GroupByFields))
	copy(q.GroupByFields, qwa.GroupByFields)

	q.Fields = make([]string, len(qwa.Fields))
	copy(q.Fields, qwa.Fields)

	q.NonSchemaFields = make([]string, len(qwa.NonSchemaFields))
	copy(q.NonSchemaFields, qwa.NonSchemaFields)

	q.Aggregators = make([]Aggregator, len(qwa.Aggregators))
	copy(q.Aggregators, qwa.Aggregators)
}

// RemoveEmptyGroupBy removes EmptyFieldSelection from GroupByFields
func (q *QueryWithAggregation) RemoveEmptyGroupBy() {
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
func (q *QueryWithAggregation) TrimKeywordFromFields(ctx context.Context) {
	for i := range q.Fields {
		if strings.HasSuffix(q.Fields[i], `.keyword"`) {
			logger.WarnWithCtx(ctx).Msgf("trimming .keyword from field %s", q.Fields[i])
			q.Fields[i] = strings.TrimSuffix(q.Fields[i], `.keyword"`)
			q.Fields[i] += `"`
		}
	}
	for i := range q.GroupByFields {
		if strings.HasSuffix(q.GroupByFields[i], `.keyword"`) {
			logger.WarnWithCtx(ctx).Msgf("trimming .keyword from group by field %s", q.GroupByFields[i])
			q.GroupByFields[i] = strings.TrimSuffix(q.GroupByFields[i], `.keyword"`)
			q.GroupByFields[i] += `"`
		}
	}
	for i := range q.NonSchemaFields {
		if strings.HasSuffix(q.NonSchemaFields[i], `.keyword"`) {
			logger.WarnWithCtx(ctx).Msgf("trimming .keyword from group by field %s", q.GroupByFields[i])
			q.NonSchemaFields[i] = strings.TrimSuffix(q.NonSchemaFields[i], `.keyword"`)
			q.NonSchemaFields[i] += `"`
		}
	}
}

// Name returns the name of this aggregation (specifically, the last aggregator)
// So for nested aggregation {"a": {"b": {"c": this aggregation}}}, it returns "c".
// In some queries aggregations are referenced by full name, so "a>b>c", but so far this implementation seems sufficient.
func (q *QueryWithAggregation) Name() string {
	if len(q.Aggregators) == 0 {
		return ""
	}
	return q.Aggregators[len(q.Aggregators)-1].Name
}

// HasParentAggregation returns true <=> this aggregation has a parent aggregation, so there's no query to the DB,
// and results are calculated based on parent aggregation's results.
func (q *QueryWithAggregation) HasParentAggregation() bool {
	return q.NoDBQuery && len(q.Parent) > 0 // first condition should be enough, second just in case
}

// IsChild returns true <=> this aggregation is a child of maybeParent (so maybeParent is its parent).
func (q *QueryWithAggregation) IsChild(maybeParent QueryWithAggregation) bool {
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
