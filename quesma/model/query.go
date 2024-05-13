package model

import (
	"context"
	"mitmproxy/quesma/logger"
	"strconv"
	"strings"
)

const RowNumberColumnName = "row_number"
const EmptyFieldSelection = "''" // we can query SELECT '', that's why such quotes

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
	sb.WriteString(" FROM " + q.FromClause + where + q.WhereClause + " " + strings.Join(q.SuffixClauses, " "))
	if len(q.GroupByFields) > 0 {
		sb.WriteString(" GROUP BY (")
		for i, field := range q.GroupByFields {
			sb.WriteString(field)
			if i < len(q.GroupByFields)-1 {
				sb.WriteString(", ")
			}
		}
		sb.WriteString(")")

		sb.WriteString(" ORDER BY (")
		for i, field := range q.GroupByFields {
			sb.WriteString(field)
			if i < len(q.GroupByFields)-1 {
				sb.WriteString(", ")
			}
		}
		sb.WriteString(")")
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
	sb.WriteString(" FROM " + q.FromClause + where + q.WhereClause + " " + strings.Join(q.SuffixClauses, " "))
	if len(q.GroupByFields) > 0 {
		sb.WriteString(" GROUP BY (")
		for i, field := range q.GroupByFields {
			sb.WriteString(field)
			if i < len(q.GroupByFields)-1 {
				sb.WriteString(", ")
			}
		}
		sb.WriteString(")")

		sb.WriteString(" ORDER BY (")
		for i, field := range q.GroupByFields {
			sb.WriteString(field)
			if i < len(q.GroupByFields)-1 {
				sb.WriteString(", ")
			}
		}
		sb.WriteString(")")
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
