package model

import (
	"context"
	"fmt"
	"mitmproxy/quesma/logger"
	"sort"
	"strconv"
	"strings"
)

const RowNumberColumnName = "row_number"
const EmptyFieldSelection = "''" // we can query SELECT '', that's why such quotes
const CountShortName = "cnt"

type SortField struct {
	Field string
	Desc  bool
}

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
	OrderBy         []string // ORDER BY fields
	SuffixClauses   []string // LIMIT, etc.
	FromClause      string   // usually just "tableName", or databaseName."tableName". Sometimes a subquery e.g. (SELECT ...)
	TableName       string
	SubQueries      []subQuery
	OrderByCount    bool
	CanParse        bool // true <=> query is valid
	QueryInfo       SearchQueryInfo
	Highlighter     Highlighter
	NoDBQuery       bool         // true <=> we don't need query to DB here, true in some pipeline aggregations
	Parent          string       // parent aggregation name, used in some pipeline aggregations
	Aggregators     []Aggregator // keeps names of aggregators, e.g. "0", "1", "2", "suggestions". Needed for JSON response.
	Type            QueryType
	SortFields      []SortField // fields to sort by
	SubSelect       string
}

type subQuery struct {
	sql       string
	innerJoin string
	name      string
}

func newSubQuery(sql, innerJoin, name string) subQuery {
	return subQuery{sql: sql, innerJoin: innerJoin, name: name}
}

var NoMetadataField JsonMap = nil

// returns string with * in SELECT
func (q *Query) String() string {
	return q.stringCommon(q.allFields())
}

// returns string with SQL query
// colNames - list of columns (schema fields) for SELECT
func (q *Query) StringFromColumns(colNames []string) string {
	return q.stringCommon(colNames)
}

func (q *Query) stringCommon(selectSchemaFields []string) string {
	var sb strings.Builder
	if len(q.SubQueries) > 0 {
		sb.WriteString("WITH ")
		for i, sq := range q.SubQueries {
			sb.WriteString(sq.name + " AS (" + sq.sql + ")")
			if i < len(q.SubQueries)-1 {
				sb.WriteString(", ")
			}
		}
		sb.WriteString(" ")
	}
	sb.WriteString("SELECT ")
	if q.IsDistinct {
		sb.WriteString("DISTINCT ")
	}
	sb.WriteString(strings.Join(selectSchemaFields, ", "))
	sb.WriteString(" FROM " + q.FromClause + " ") //where + q.WhereClause + " ")
	for i, sq := range q.SubQueries {
		sb.WriteString("INNER JOIN " + sq.name + " ON " + sq.innerJoin + " ")
		if i < len(q.SubQueries)-1 {
			sb.WriteString("AND ")
		}
	}
	if len(q.WhereClause) > 0 {
		sb.WriteString("WHERE " + q.WhereClause + " ")
	}
	where := " WHERE "
	if len(q.WhereClause) == 0 {
		where = ""
	}
	sb.WriteString(" FROM " + q.FromClause + where + q.WhereClause)
	lastLetterIsSpace := true
	if len(q.GroupByFields) > 0 {
		sb.WriteString("GROUP BY ")
		for i, field := range q.GroupByFields {
			sb.WriteString(field)
			if i < len(q.GroupByFields)-1 {
				sb.WriteString(", ")
			}
		}
		lastLetterIsSpace = false
	}
	if len(q.OrderBy) > 0 {
		if !lastLetterIsSpace {
			sb.WriteString(" ")
		}
		sb.WriteString("ORDER BY ")
		for i, field := range q.OrderBy {
			sb.WriteString(field)
			if i < len(q.OrderBy)-1 {
				sb.WriteString(", ")
			}
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

func (q *Query) allFields() []string {
	fields := make([]string, 0, len(q.Fields)+len(q.NonSchemaFields))
	for _, field := range q.Fields {
		if field == "*" {
			fields = append(fields, "*")
		} else {
			fields = append(fields, strconv.Quote(field))
		}
	}
	for _, field := range q.NonSchemaFields {
		fields = append(fields, field)
	}
	return fields
}

func (q *Query) AddSubQueryFromCurrentState(ctx context.Context, subqueryNr int) {
	queryName := q.subQueryName(subqueryNr)

	selectFields := make([]string, 0, len(q.Fields)+len(q.NonSchemaFields)+1)
	for _, schemaField := range q.Fields {
		if schemaField == "*" {
			logger.WarnWithCtx(ctx).Msgf("Query with * shouldn't happen here. Skipping (query: %+v)", q)
			continue
		}
		selectFields = append(selectFields, fmt.Sprintf(`"%s" AS "%s_%s"`, schemaField, queryName, schemaField))
	}
	for i, nonSchemaField := range q.NonSchemaFields {
		selectFields = append(selectFields, fmt.Sprintf(`%s AS "%s_ns_%d"`, nonSchemaField, queryName, i))
	}
	selectFields = append(selectFields, fmt.Sprintf("count() AS %s", strconv.Quote(q.subQueryCountFieldName(subqueryNr))))
	sql := q.StringFromColumns(selectFields)
	innerJoinParts := make([]string, 0, len(q.GroupByFields))
	for _, field := range q.Fields {
		innerJoinParts = append(innerJoinParts, fmt.Sprintf(`"%s" = "%s_%s"`, field, queryName, field))
		// FIXME add support for non-schema fields
	}
	innerJoin := strings.Join(innerJoinParts, " AND ")
	q.SubQueries = append(q.SubQueries, newSubQuery(sql, innerJoin, queryName))
}

func (q *Query) subQueryName(nr int) string {
	return "subQuery" + strconv.Itoa(nr)
}

func (q *Query) subQueryCountFieldName(nr int) string {
	return q.subQueryName(nr) + "_" + CountShortName
}

// CopyAggregationFields copies all aggregation fields from qwa to q
func (q *Query) CopyAggregationFields(qwa Query) {
	q.GroupByFields = make([]string, len(qwa.GroupByFields))
	copy(q.GroupByFields, qwa.GroupByFields)

	q.Fields = make([]string, len(qwa.Fields))
	copy(q.Fields, qwa.Fields)

	q.NonSchemaFields = make([]string, len(qwa.NonSchemaFields))
	copy(q.NonSchemaFields, qwa.NonSchemaFields)

	q.SuffixClauses = make([]string, len(qwa.SuffixClauses))
	copy(q.SuffixClauses, qwa.SuffixClauses)

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
func (q *Query) TrimKeywordFromFields(ctx context.Context) {
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
