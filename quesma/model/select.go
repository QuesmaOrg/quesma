package model

import (
	"fmt"
	"strings"
)

type SelectCommand struct {
	IsDistinct bool // true <=> query is SELECT DISTINCT

	Columns     []Expr        // Columns to select
	FromClause  Expr          // usually just "tableName", or databaseName."tableName". Sometimes a subquery e.g. (SELECT ...)
	WhereClause Expr          // "WHERE ..." until next clause like GROUP BY/ORDER BY, etc.
	GroupBy     []Expr        // if not empty, we do GROUP BY GroupBy...
	OrderBy     []OrderByExpr // if not empty, we do ORDER BY OrderBy...

	Limit       int // LIMIT clause, noLimit (0) means no limit
	SampleLimit int // LIMIT, but before grouping, 0 means no limit
}

func NewSelectCommand(columns, groupBy []Expr, orderBy []OrderByExpr, from, where Expr, limit, sampleLimit int, isDistinct bool) *SelectCommand {
	return &SelectCommand{
		IsDistinct: isDistinct,

		Columns:     columns,
		GroupBy:     groupBy,
		OrderBy:     orderBy,
		FromClause:  from,
		WhereClause: where,
		Limit:       limit,
		SampleLimit: sampleLimit,
	}
}

func (c *SelectCommand) String() string {
	var sb strings.Builder
	sb.WriteString("SELECT ")
	if c.IsDistinct {
		sb.WriteString("DISTINCT ")
	}

	columns := make([]string, 0)

	for _, col := range c.Columns {
		columns = append(columns, AsString(col))
	}

	sb.WriteString(strings.Join(columns, ", "))

	sb.WriteString(" FROM ")
	if c.SampleLimit > 0 {
		sb.WriteString("(SELECT ")
		innerColumn := make([]string, 0)
		for _, col := range c.Columns {
			if _, ok := col.(ColumnRef); ok {
				innerColumn = append(innerColumn, AsString(col))
			}
			if aliased, ok := col.(AliasedExpr); ok {
				if v, ok := aliased.Expr.(ColumnRef); ok {
					innerColumn = append(innerColumn, AsString(v))
				}
			}
		}
		if len(innerColumn) == 0 {
			innerColumn = append(innerColumn, "1")
		}
		sb.WriteString(strings.Join(innerColumn, ", "))
		sb.WriteString(" FROM ")
	}
	if c.FromClause != nil {
		sb.WriteString(AsString(c.FromClause))
	}
	if c.WhereClause != nil {
		sb.WriteString(" WHERE ")
		sb.WriteString(AsString(c.WhereClause))
	}
	if c.SampleLimit > 0 {
		sb.WriteString(fmt.Sprintf(" LIMIT %d)", c.SampleLimit))
	}

	groupBy := make([]string, 0, len(c.GroupBy))
	for _, col := range c.GroupBy {
		groupBy = append(groupBy, AsString(col))
	}
	if len(groupBy) > 0 {
		sb.WriteString(" GROUP BY ")
		sb.WriteString(strings.Join(groupBy, ", "))
	}

	orderBy := make([]string, 0, len(c.OrderBy))
	for _, col := range c.OrderBy {
		orderBy = append(orderBy, AsString(col))
	}
	if len(orderBy) > 0 {
		sb.WriteString(" ORDER BY ")
		sb.WriteString(strings.Join(orderBy, ", "))
	}

	if c.Limit != noLimit {
		sb.WriteString(fmt.Sprintf(" LIMIT %d", c.Limit))
	}

	return sb.String()
}

func (c *SelectCommand) IsWildcard() bool {
	for _, col := range c.Columns {
		if col == NewWildcardExpr {
			return true
		}
	}
	return false
}

// somewhat hacky, can be improved
// only returns Order By columns, which are "tableColumn ASC/DESC",
// won't return complex ones, like e.g. toInt(int_field / 5).
// but it was like that before the refactor
func (c *SelectCommand) OrderByFieldNames() (fieldNames []string) {
	for _, expr := range c.OrderBy {
		for _, colRefs := range GetUsedColumns(expr) {
			fieldNames = append(fieldNames, colRefs.ColumnName)
		}
	}
	return fieldNames
}
