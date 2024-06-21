package quesma

import (
	"fmt"
	"mitmproxy/quesma/clickhouse"
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/model"
	"mitmproxy/quesma/schema"
	"sort"
	"strings"
)

type ArrayTypeVisitor struct {
	tableName string
	table     *clickhouse.Table

	// deps
	schemaRegistry schema.Registry
	logManager     *clickhouse.LogManager
	schema         schema.Schema
}

func (v *ArrayTypeVisitor) visitChildren(args []model.Expr) []model.Expr {
	var newArgs []model.Expr
	for _, arg := range args {
		if arg != nil {
			newArgs = append(newArgs, arg.Accept(v).(model.Expr))
		}
	}
	return newArgs
}

func (v *ArrayTypeVisitor) dbColumnType(fieldName string) string {

	//
	// This is a HACK to get the column database type from the schema
	//
	//
	fieldName = strings.TrimSuffix(fieldName, ".keyword")

	tableColumnName := strings.ReplaceAll(fieldName, ".", "::")
	col, ok := v.table.Cols[tableColumnName]
	if ok {
		return col.Type.String()
	}

	return ""
}

func (v *ArrayTypeVisitor) VisitLiteral(e model.LiteralExpr) interface{} { return e }
func (v *ArrayTypeVisitor) VisitInfix(e model.InfixExpr) interface{} {

	column, ok := e.Left.(model.ColumnRef)
	if ok {
		dbType := v.dbColumnType(column.ColumnName)

		if strings.HasPrefix(dbType, "Array") {
			switch {

			case e.Op == "ILIKE" && dbType == "Array(String)":

				// here we can use arrayJoin to "multiply" rows

				// or use arrayFilter  and provide "LIKE" as a predicate
				// it will require to support lambda expression in our AST

				//https://clickhouse.com/docs/en/sql-reference/functions/array-functions#arrayfilterfunc-arr1-

				wrapped := model.NewFunction("arrayJoin", e.Left)
				return model.NewInfixExpr(wrapped, "LIKE", e.Right.Accept(v).(model.Expr))

			case e.Op == "=":
				return model.NewFunction("has", e.Left, e.Right.Accept(v).(model.Expr))

			default:
				logger.Warn().Msgf("Unhandled array infix operation  %s, column %v (%v)", e.Op, column.ColumnName, dbType)
			}
		}
	}

	left := e.Left.Accept(v).(model.Expr)
	right := e.Right.Accept(v).(model.Expr)

	return model.NewInfixExpr(left, e.Op, right)
}

func (v *ArrayTypeVisitor) VisitPrefixExpr(e model.PrefixExpr) interface{} {

	args := v.visitChildren(e.Args)

	return model.NewPrefixExpr(e.Op, args)

}
func (v *ArrayTypeVisitor) VisitFunction(e model.FunctionExpr) interface{} {

	if len(e.Args) == 1 {
		arg := e.Args[0]
		column, ok := arg.(model.ColumnRef)
		if ok {
			dbType := v.dbColumnType(column.ColumnName)
			if strings.HasPrefix(dbType, "Array") {
				switch {

				// this is how we can transform aggregation on array fields
				// another option is to use rewrite query to arrayJoin

				case e.Name == "sumOrNull" && dbType == "Array(Int64)":
					fnName := model.LiteralExpr{Value: fmt.Sprintf("'%s'", e.Name)}
					wrapped := model.NewFunction("arrayReduce", fnName, column)
					wrapped = model.NewFunction(e.Name, wrapped)
					return wrapped

				default:
					logger.Warn().Msgf("Unhandled array function %s, column %v (%v)", e.Name, column.ColumnName, dbType)

				}
			}
		}
	}

	args := v.visitChildren(e.Args)
	return model.NewFunction(e.Name, args...)
}
func (v *ArrayTypeVisitor) VisitColumnRef(e model.ColumnRef) interface{} {

	return e
}

func (v *ArrayTypeVisitor) VisitNestedProperty(e model.NestedProperty) interface{} {

	return model.NestedProperty{
		ColumnRef:    e.ColumnRef.Accept(v).(model.ColumnRef),
		PropertyName: e.PropertyName.Accept(v).(model.LiteralExpr),
	}
}
func (v *ArrayTypeVisitor) VisitArrayAccess(e model.ArrayAccess) interface{} {
	return model.ArrayAccess{
		ColumnRef: e.ColumnRef.Accept(v).(model.ColumnRef),
		Index:     e.Index.Accept(v).(model.Expr),
	}
}
func (v *ArrayTypeVisitor) VisitMultiFunction(e model.MultiFunctionExpr) interface{} {

	args := v.visitChildren(e.Args)
	return model.MultiFunctionExpr{Name: e.Name, Args: args}
}

func (v *ArrayTypeVisitor) VisitString(e model.StringExpr) interface{} { return e }
func (v *ArrayTypeVisitor) VisitOrderByExpr(e model.OrderByExpr) interface{} {

	exprs := v.visitChildren(e.Exprs)

	return model.NewOrderByExpr(exprs, e.Direction)

}
func (v *ArrayTypeVisitor) VisitDistinctExpr(e model.DistinctExpr) interface{} {

	return model.NewDistinctExpr(e.Expr.Accept(v).(model.Expr))
}
func (v *ArrayTypeVisitor) VisitTableRef(e model.TableRef) interface{} {
	return model.NewTableRef(e.Name)
}
func (v *ArrayTypeVisitor) VisitAliasedExpr(e model.AliasedExpr) interface{} {
	return model.NewAliasedExpr(e.Expr.Accept(v).(model.Expr), e.Alias)
}
func (v *ArrayTypeVisitor) VisitWindowFunction(e model.WindowFunction) interface{} {

	return model.NewWindowFunction(e.Name, v.visitChildren(e.Args), v.visitChildren(e.PartitionBy), e.OrderBy.Accept(v).(model.OrderByExpr))

}

func (v *ArrayTypeVisitor) unique(columns []model.ColumnRef) []model.ColumnRef {
	var result []model.ColumnRef
	seen := make(map[string]bool)
	for _, col := range columns {
		if _, ok := seen[col.ColumnName]; !ok {
			result = append(result, col)
			seen[col.ColumnName] = true
		}
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].ColumnName < result[j].ColumnName
	})

	return result
}

func (v *ArrayTypeVisitor) splitIntoArrayAndNonArrayColumns(exprs []model.ColumnRef) ([]model.ColumnRef, []model.ColumnRef) {
	var arrayColumns []model.ColumnRef
	var nonArrayColumns []model.ColumnRef

	for _, expr := range exprs {
		columns := model.GetUsedColumns(expr)
		for _, col := range columns {
			if strings.HasPrefix(v.dbColumnType(col.ColumnName), "Array") {
				arrayColumns = append(arrayColumns, col)
			} else {
				nonArrayColumns = append(nonArrayColumns, col)
			}
		}
	}

	return v.unique(arrayColumns), v.unique(nonArrayColumns)
}

func (v *ArrayTypeVisitor) VisitSelectCommand(e model.SelectCommand) interface{} {
	if v.schemaRegistry == nil {
		logger.Error().Msg("Schema registry is not set")
		return e
	}
	sch, exists := v.schemaRegistry.FindSchema(schema.TableName(v.tableName))

	if !exists {
		logger.Error().Msgf("Schema fot table %s not found", v.tableName)
		return e
	}
	v.schema = sch

	table := v.logManager.FindTable(v.tableName)
	v.table = table

	// discovery

	var groupByColumns []model.ColumnRef
	for _, expr := range e.GroupBy {
		groupByColumns = append(groupByColumns, model.GetUsedColumns(expr)...)
	}

	arrayColumnsInGroupBy := make(map[string]bool)
	for _, col := range groupByColumns {
		if strings.HasPrefix(v.dbColumnType(col.ColumnName), "Array") {
			arrayColumnsInGroupBy[col.ColumnName] = true
		}
	}
	isGroupBy := len(arrayColumnsInGroupBy) > 0

	var hasTableNameInFromClause bool
	if e.FromClause != nil {
		if _, ok := e.FromClause.(model.TableRef); ok {
			hasTableNameInFromClause = true
		}
	}

	// transformation

	switch {

	case isGroupBy && hasTableNameInFromClause:

		// We can rewrite aggregation query as follows.
		// CTE could be more elegant but our AST does not support it.
		//
		// Not sure how clickhouses handles arrayJoin. What is a performance impact?
		//
		// FROM: select a, count() from table group by a
		//   TO: select a, count() from (select arrayJoin(a) as a from table) group by a

		var columnsToAdd []model.ColumnRef

		// here we gather all columns that are used in the main query
		for _, expr := range e.GroupBy {
			columnsToAdd = append(columnsToAdd, model.GetUsedColumns(expr)...)
		}
		for _, expr := range e.Columns {
			columnsToAdd = append(columnsToAdd, model.GetUsedColumns(expr)...)
		}
		for _, expr := range e.OrderBy {
			columnsToAdd = append(columnsToAdd, model.GetUsedColumns(expr)...)
		}
		if e.WhereClause != nil {
			columnsToAdd = append(columnsToAdd, model.GetUsedColumns(e.WhereClause)...)
		}

		// then add  them to the inner query
		var columns []model.Expr

		arrayColumns, otherColumns := v.splitIntoArrayAndNonArrayColumns(columnsToAdd)

		for _, name := range otherColumns {
			columns = append(columns, name)
		}

		for _, col := range arrayColumns {

			// this is a HACK
			dbColumName := strings.TrimSuffix(col.ColumnName, ".keyword")
			dbColumName = strings.ReplaceAll(dbColumName, ".", "::")
			dbColumName = fmt.Sprintf("%s", dbColumName)

			columns = append(columns, model.NewAliasedExpr(model.NewFunction("arrayJoin", model.NewColumnRef(dbColumName)), dbColumName))
		}

		from := model.TableRef{Name: v.tableName}

		subQuery := model.NewSelectCommand(columns, nil, nil,
			from, nil, 0, 0, false)

		subQuery.DisableHack = true

		query := model.NewSelectCommand(e.Columns, e.GroupBy, e.OrderBy, subQuery, e.WhereClause, e.Limit, e.SampleLimit, e.IsDistinct)
		query.DisableHack = true

		return query

	default:

		// this is naive transformation, only for simple queries

		var groupBy []model.Expr
		for _, expr := range e.GroupBy {
			groupBy = append(groupBy, expr.Accept(v).(model.Expr))
		}

		var columns []model.Expr
		for _, expr := range e.Columns {
			columns = append(columns, expr.Accept(v).(model.Expr))
		}

		var fromClause model.Expr
		if e.FromClause != nil {
			fromClause = e.FromClause.Accept(v).(model.Expr)
		}

		var whereClause model.Expr
		if e.WhereClause != nil {
			whereClause = e.WhereClause.Accept(v).(model.Expr)
		}

		return model.NewSelectCommand(columns, groupBy, e.OrderBy,
			fromClause, whereClause, e.Limit, e.SampleLimit, e.IsDistinct)

	}

}
