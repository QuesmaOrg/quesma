package model

type SelectCommand struct {
	IsDistinct bool // true <=> query is SELECT DISTINCT

	Columns     []Expr        // Columns to select
	FromClause  Expr          // usually just "tableName", or databaseName."tableName". Sometimes a subquery e.g. (SELECT ...)
	WhereClause Expr          // "WHERE ..." until next clause like GROUP BY/ORDER BY, etc.
	GroupBy     []Expr        // if not empty, we do GROUP BY GroupBy...
	OrderBy     []OrderByExpr // if not empty, we do ORDER BY OrderBy...

	Limit       int // LIMIT clause, noLimit (0) means no limit
	SampleLimit int // LIMIT, but before grouping, 0 means no limit

	DisableHack bool // HACK ALERT: if true, the hacky code in AsString() will not be executed
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

// Accept implements the Visitor interface for SelectCommand,
func (c SelectCommand) Accept(v ExprVisitor) interface{} {
	// This is handy because it enables representing nested queries (e.g. `SELECT * FROM (SELECT * FROM table1) AS t1 WHERE ...`)
	return v.VisitSelectCommand(c)
}

func (c SelectCommand) String() string {
	// TODO - we might need to verify queries nested N-times (N>=3), perhaps this should strip the outermost braces
	return AsString(c)
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
