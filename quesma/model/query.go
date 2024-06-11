package model

import (
	"context"
	"fmt"
	"mitmproxy/quesma/logger"
	"sort"
	"strings"
)

const (
	RowNumberColumnName = "row_number"
	noLimit             = 0
	Desc                = "DESC"
	Asc                 = "ASC"
)

type (
	SelectColumn struct {
		Alias      string
		Expression Expr
	}

	Query struct {
		IsDistinct bool // true <=> query is SELECT DISTINCT

		// This is SELECT query. These fields should be extracted to separate struct.
		Columns     []SelectColumn // Columns to select, including aliases
		GroupBy     []SelectColumn // if not empty, we do GROUP BY GroupBy...
		OrderBy     []SelectColumn // if not empty, we do ORDER BY OrderBy...
		FromClause  SelectColumn   // usually just "tableName", or databaseName."tableName". Sometimes a subquery e.g. (SELECT ...)
		WhereClause Expr           // "WHERE ..." until next clause like GROUP BY/ORDER BY, etc.
		Limit       int            // LIMIT clause, noLimit (0) means no limit
		SampleLimit int            // LIMIT, but before grouping, 0 means no limit

		CanParse bool // true <=> query is valid

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

func NewSortColumn(field string, desc bool) SelectColumn {
	var order string
	if desc {
		order = Desc
	} else {
		order = Asc
	}
	return SelectColumn{Expression: NewComposite(NewTableColumnExpr(field), NewStringExpr(order))}
}

func NewSortByCountColumn(desc bool) SelectColumn {
	var order string
	if desc {
		order = Desc
	} else {
		order = Asc
	}
	return SelectColumn{Expression: NewComposite(NewCountFunc(), NewStringExpr(order))}
}

func NewSelectColumnTableField(fieldName string) SelectColumn {
	return SelectColumn{Expression: NewTableColumnExpr(fieldName)}
}

func NewSelectColumnFromString(s string) SelectColumn {
	return SelectColumn{Expression: StringExpr{Value: s}}
}

func (c SelectColumn) SQL() string {

	if c.Expression == nil {
		panic("SelectColumn expression is nil")
	}

	exprAsString := AsString(c.Expression)

	if c.Alias == "" {
		return exprAsString
	}

	// if alias is the same as column name, we don't need to add it
	switch exp := c.Expression.(type) {
	case TableColumnExpr:
		if exp.ColumnRef.ColumnName == c.Alias {
			return exprAsString
		}
	}

	return fmt.Sprintf("%s AS \"%s\"", exprAsString, c.Alias)
}

func (c SelectColumn) String() string {
	return fmt.Sprintf("SelectColumn(Alias: '%s', expression: '%v')", c.Alias, c.Expression)
}

var NoMetadataField JsonMap = nil

// returns string with SQL query
func (q *Query) String(ctx context.Context) string {
	var sb strings.Builder
	sb.WriteString("SELECT ")
	if q.IsDistinct {
		sb.WriteString("DISTINCT ")
	}

	columns := make([]string, 0)

	for _, col := range q.Columns {
		if col.Expression == nil {
			// this is paraonoid check, it should never happen
			panic("SelectColumn expression is nil")
		} else {
			columns = append(columns, col.SQL())
		}
	}

	sb.WriteString(strings.Join(columns, ", "))

	sb.WriteString(" FROM ")
	if q.SampleLimit > 0 {
		sb.WriteString("(SELECT ")
		innerColumn := make([]string, 0)
		for _, col := range q.Columns {
			if _, ok := col.Expression.(TableColumnExpr); ok {
				innerColumn = append(innerColumn, AsString(col.Expression)) // TOOD: Maybe need a change
			}
		}
		if len(innerColumn) == 0 {
			innerColumn = append(innerColumn, "1")
		}
		sb.WriteString(strings.Join(innerColumn, ", "))
		sb.WriteString(" FROM ")
	}
	sb.WriteString(q.FromClause.SQL())
	if q.WhereClause != nil {
		sb.WriteString(" WHERE ")
		sb.WriteString(AsString(q.WhereClause))
	}
	if q.SampleLimit > 0 {
		sb.WriteString(fmt.Sprintf(" LIMIT %d)", q.SampleLimit))
	}

	groupBy := make([]string, 0, len(q.GroupBy))
	for _, col := range q.GroupBy {
		if col.Expression == nil {
			logger.Warn().Msgf("GroupBy column expression is nil, skipping. Column: %+v", col)
		} else {
			groupBy = append(groupBy, col.SQL())
		}
	}
	if len(groupBy) > 0 {
		sb.WriteString(" GROUP BY ")
		sb.WriteString(strings.Join(groupBy, ", "))
	}

	orderBy := make([]string, 0, len(q.OrderBy))
	for _, col := range q.OrderBy {
		if col.Expression == nil {
			logger.WarnWithCtx(ctx).Msgf("GroupBy column expression is nil, skipping. Column: %+v", col)
		} else {
			orderBy = append(orderBy, col.SQL())
		}
	}
	if len(orderBy) > 0 {
		sb.WriteString(" ORDER BY ")
		sb.WriteString(strings.Join(orderBy, ", "))
	}

	if q.Limit != noLimit {
		sb.WriteString(fmt.Sprintf(" LIMIT %d", q.Limit))
	}

	return sb.String()
}

func (q *Query) IsWildcard() bool {

	for _, col := range q.Columns {
		if col.Expression == NewWildcardExpr {
			return true
		}
	}

	return false
}

// CopyAggregationFields copies all aggregation fields from qwa to q
func (q *Query) CopyAggregationFields(qwa Query) {
	q.GroupBy = make([]SelectColumn, len(qwa.GroupBy))
	copy(q.GroupBy, qwa.GroupBy)

	q.Columns = make([]SelectColumn, len(qwa.Columns))
	copy(q.Columns, qwa.Columns)

	q.Aggregators = make([]Aggregator, len(qwa.Aggregators))
	copy(q.Aggregators, qwa.Aggregators)
}

// TrimKeywordFromFields trims .keyword from fields and group by fields
// In future probably handle it in a better way
func (q *Query) TrimKeywordFromFields() {

}

// somewhat hacky, can be improved
// only returns Order By columns, which are "tableColumn ASC/DESC",
// won't return complex ones, like e.g. toInt(int_field / 5).
// but it was like that before the refactor
func (q *Query) OrderByFieldNames() (fieldNames []string) {
	for _, col := range q.OrderBy {
		compositeExp, ok := col.Expression.(*CompositeExpr)
		if !ok {
			continue
		}
		if len(compositeExp.Expressions) != 2 {
			continue
		}
		orderExp, ok := compositeExp.Expressions[1].(StringExpr)
		if !ok || (orderExp.Value != Asc && orderExp.Value != Desc) {
			continue
		}

		tableColExp, ok := compositeExp.Expressions[0].(TableColumnExpr)
		if !ok {
			continue
		}

		fieldNames = append(fieldNames, tableColExp.ColumnRef.ColumnName)
	}
	return fieldNames
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

// TODO change whereClause type string -> some typed
func (q *Query) NewSelectColumnSubselectWithRowNumber(selectFields []SelectColumn, groupByFields []SelectColumn,
	whereClause string, orderByField string, orderByDesc bool) SelectColumn {

	const additionalArrayLength = 6
	/* used to be as string:
	fromSelect := fmt.Sprintf(
		"(SELECT %s, ROW_NUMBER() OVER (PARTITION BY %s ORDER BY %s %s) AS %s FROM %s WHERE %s)",
			fieldsAsString, fieldsAsString, orderField, asc/desc,
			model.RowNumberColumnName, query.FromClause, b.whereBuilder.WhereClauseAsNewStringExpr(),
	)
	*/

	fromSelect := make([]Expr, 0, 2*(len(selectFields)+len(groupByFields))+additionalArrayLength) // +6 without ORDER BY, +8 with ORDER BY
	fromSelect = append(fromSelect, NewStringExpr("SELECT"))
	for _, field := range selectFields {
		fromSelect = append(fromSelect, field.Expression)
		fromSelect = append(fromSelect, NewStringExpr(","))
	}

	// Maybe keep this ROW_NUMBER as SelectColumn? It'd introduce some problems, because it's not in schema.
	// Sticking to simpler solution now.
	fromSelect = append(fromSelect, NewStringExpr("ROW_NUMBER() OVER (PARTITION BY"))
	for i, field := range groupByFields {
		fromSelect = append(fromSelect, field.Expression)
		if i != len(groupByFields)-1 {
			fromSelect = append(fromSelect, NewStringExpr(","))
		}
	}
	if orderByField != "" {
		fromSelect = append(fromSelect, NewStringExpr("ORDER BY"))
		fromSelect = append(fromSelect, NewSortColumn(orderByField, orderByDesc).Expression)
	}
	fromSelect = append(fromSelect, NewStringExpr(") AS"))
	// TODO this formatting below is only to match the existing test cases,
	// window functions formatting (as everything else) should be systematically formatted at the printing stage
	fromSelect = append(fromSelect, NewLiteral(fmt.Sprintf("'%s'", RowNumberColumnName)))
	fromSelect = append(fromSelect, NewStringExpr("FROM"))
	fromSelect = append(fromSelect, q.FromClause.Expression)

	if whereClause != "" {
		fromSelect = append(fromSelect, NewStringExpr("WHERE "+whereClause))
	}

	return SelectColumn{Expression: NewFunction("", NewComposite(fromSelect...))}
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
