package testdata

import (
	"mitmproxy/quesma/model"
	"strconv"
	"strings"
	"time"
)

// selectFieldsInAnyOrderAsRegex returns a regex that matches all permutations of the fields in any order.
// They are quoted and separated by commas, just like in our SQL queries.
// Example: selectFieldsInAnyOrderAsRegex([]string{"a", "b", "c"}) returns
// `("a", "b", "c")|("a", "c", "b")|("b", "a", "c")|("b", "c", "a")|("c", "b", "a")|("c", "a", "b")`
func selectFieldsInAnyOrderAsRegex(fields []string) string {
	for i := range fields {
		fields[i] = strconv.Quote(fields[i])
	}
	var resultRegex string

	var permutate func(i int)
	permutate = func(i int) {
		if i > len(fields) {
			// adds permutation to the resultRegex
			resultRegex += "(" + strings.Join(fields, ", ") + ")|"
			return
		}
		permutate(i + 1)
		for j := i + 1; j < len(fields); j++ {
			fields[i], fields[j] = fields[j], fields[i]
			permutate(i + 1)
			fields[i], fields[j] = fields[j], fields[i]
		}
	}

	permutate(0)
	return resultRegex[:len(resultRegex)-1] // remove the last "|"
}

const TableName = "logs-generic-default"
const QuotedTableName = `"` + TableName + `"`
const queryparserFacetsSampleSize = "20000" // should be same value as queryparser.facetsSampleSize
const defaultLimit = model.DefaultSizeListQuery

const oneMinute = 60 * time.Second

func newSimplestQuery() model.Query {
	return model.Query{
		Fields:        []string{"*"},
		FromClause:    strconv.Quote(TableName),
		SuffixClauses: []string{"LIMIT " + strconv.Itoa(defaultLimit)},
		CanParse:      true,
	}
}

// qToStr is a simple helper function to help fill out test cases
func qToStr(query model.Query) string {
	return strings.Replace(query.String(), "*", `"message"`, 1)
}

// justWhere is a simple helper function to help fill out test cases
func justSimplestWhere(whereClause string) model.Query {
	query := newSimplestQuery()
	query.WhereClause = whereClause
	return query
}

func withLimit(query model.Query, limit int) model.Query {
	query.SuffixClauses = []string{"LIMIT " + strconv.Itoa(limit)}
	return query
}

// EscapeBrackets is a simple helper function used in sqlmock's tests.
// Example usage: sqlmock.ExpectQuery(EscapeBrackets(`SELECT count() FROM "logs-generic-default" WHERE `))
func EscapeBrackets(s string) string {
	s = strings.ReplaceAll(s, `(`, `\(`)
	s = strings.ReplaceAll(s, `)`, `\)`)
	s = strings.ReplaceAll(s, `[`, `\[`)
	s = strings.ReplaceAll(s, `]`, `\]`)
	return s
}

// EscapeWildcard is a simple helper function used in sqlmock's tests. It escapes the wildcard character '*'.
func EscapeWildcard(s string) string {
	return strings.ReplaceAll(s, "*", `\*`)
}
