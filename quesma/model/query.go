package model

import (
	"strconv"
	"strings"
)

type Query struct {
	Fields          []string // Fields in 'SELECT Fields FROM ...'
	NonSchemaFields []string // Fields that are not in schema, but are in 'SELECT ...', e.g. count()
	WhereClause     string   // "WHERE ..." until next clause like GROUP BY/ORDER BY, etc.
	SuffixClauses   []string // GROUP BY, ORDER BY, etc.
	TableName       string
	CanParse        bool // true <=> query is valid
}

// returns string with * in SELECT
func (q *Query) String() string {
	var sb strings.Builder
	sb.WriteString("SELECT ")
	for i, field := range q.Fields {
		if field == "*" {
			sb.WriteString("*")
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
	sb.WriteString(" FROM " + q.TableName + where + q.WhereClause + " " + strings.Join(q.SuffixClauses, " "))
	return sb.String()
}

// returns string without * in SELECT
// colNames - list of columns (schema fields) for SELECT
func (q *Query) StringFromColumns(colNames []string) string {
	var sb strings.Builder
	sb.WriteString("SELECT ")
	for i, field := range colNames {
		sb.WriteString(strconv.Quote(field))
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
	sb.WriteString(" FROM " + q.TableName + where + q.WhereClause + " " + strings.Join(q.SuffixClauses, " "))
	return sb.String()
}

func (q *Query) IsWildcard() bool {
	return len(q.Fields) == 1 && q.Fields[0] == "*"
}

type AsyncSearchQueryType int
type SearchQueryType int

const (
	Histogram AsyncSearchQueryType = iota
	AggsByField
	ListByField
	ListAllFields
	EarliestLatestTimestamp // query for 2 timestamps: earliest and latest
	None                    // called None, not Normal, like below, as it basically never happens, I don't even know how to trigger it/reply to this
)

const (
	Count SearchQueryType = iota
	Normal
)

func (queryType AsyncSearchQueryType) String() string {
	return []string{"Histogram", "AggsByField", "ListByField", "ListAllFields", "EarliestLatestTimestamp, None"}[queryType]
}

func (queryType SearchQueryType) String() string {
	return []string{"Count", "NoneSearch"}[queryType]
}

type QueryInfoAsyncSearch struct {
	Typ       AsyncSearchQueryType
	FieldName string
	I1        int
	I2        int
}

func NewQueryInfoNone() QueryInfoAsyncSearch {
	return QueryInfoAsyncSearch{Typ: None}
}
