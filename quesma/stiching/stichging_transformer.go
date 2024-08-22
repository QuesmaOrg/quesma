package stiching

import (
	"fmt"
	"quesma/clickhouse"
	"quesma/logger"
	"quesma/model"
	"strings"
)

/*
Idea:

--
select foo,bar from  (table_1, table_2, table_3) -- fantasy syntax
--


--
WITH  __quesma_stitched_table as (
 SELECT * FROM
      table_1
      FULL OUTER JOIN table_2 ON (FALSE)
      FULL OUTER JOIN table_3 ON (FALSE)
)

SELECT foo, bar FROM __quesma_stitched_table
--



*/

type StitchingTransformer struct {
	SchemaLoader clickhouse.TableDiscovery
}

type stitchTable struct {
	tableName string
	cteName   string
	tableDef  *clickhouse.Table
}

type stitching struct {
	tableName     string
	joined        *model.SelectCommand
	stitches      []stitchTable
	origTableName string

	// TODO map of columns
}

func (w *stitching) stitch(stm model.SelectCommand) model.SelectCommand {

	where := stm.WhereClause

	var ctes []*model.CTE

	for _, sub := range w.stitches {
		// replace not existing columns with NULL

		var myWhere model.Expr

		if where != nil {
			visitor := &model.BaseExprVisitor{}
			visitor.OverrideVisitColumnRef = func(b *model.BaseExprVisitor, c model.ColumnRef) interface{} {
				if sub.tableDef.Cols[c.ColumnName] == nil {
					return model.NewLiteral("NULL")
				}
				return &c
			}

			myWhere = where.Accept(visitor).(model.Expr)
		}

		var columns []model.Expr
		for _, col := range sub.tableDef.Cols {
			var myCol model.Expr
			if strings.Contains(col.Type.StringWithNullable(), "Nullable") || strings.Contains(col.Type.StringWithNullable(), "Array") {
				myCol = model.NewColumnRef(col.Name)
			} else {
				myCol = model.NewAliasedExpr(model.NewFunction("toNullable", model.NewColumnRef(col.Name)), col.Name)
			}
			columns = append(columns, myCol)
		}

		// filter columns that are not present in the input query

		cteStm := &model.SelectCommand{
			Columns:     columns, // do we need to select all columns?
			FromClause:  model.NewTableRef(sub.tableName),
			WhereClause: myWhere,
			Limit:       stm.Limit,   // TODO filter columns that are not present in the input query
			OrderBy:     stm.OrderBy, // TODO filter columns that are not present in the input query
		}

		cte := &model.CTE{
			Name:          sub.cteName,
			SelectCommand: cteStm,
		}

		ctes = append(ctes, cte)
	}

	visitor := &model.BaseExprVisitor{}

	// here we replace the table name with the stitched CTE name
	visitor.OverrideVisitTableRef = func(b *model.BaseExprVisitor, t model.TableRef) interface{} {

		fmt.Println("XXXX stitching REPLACE table:  ", w.origTableName, t.Name, w.tableName)

		if t.Name == w.origTableName {
			return model.NewTableRef(w.tableName)
		}

		return model.NewTableRef(t.Name)
	}
	newStm := stm.Accept(visitor).(*model.SelectCommand)

	newStm.WhereClause = nil // tables are filtered in the CTEs

	stitchingCTE := &model.CTE{
		Name:          w.tableName,
		SelectCommand: w.joined, // TODO filter columns that are not present in the input query
	}
	ctes = append(ctes, stitchingCTE)

	newStm.NamedCTEs = append(ctes, newStm.NamedCTEs...)

	return *newStm
}

func (t *StitchingTransformer) Name() string {
	return "stitching"
}

func (t *StitchingTransformer) stitch(query []*model.Query) *stitching {

	// check if the index is a stitched table

	fmt.Println("stitching", query[0].TableName)

	var tables []string
	var origTableName string
	for k, v := range clickhouse.StitchedTable {

		if strings.Contains(query[0].TableName, k) {

			fmt.Println("XXXX stitching", query[0].TableName, k, v)
			origTableName = query[0].TableName
			tables = v
		}
	}

	if len(tables) == 0 {
		return nil
	}

	// tables to be stitched

	tableDefinition := t.SchemaLoader.TableDefinitions()

	tablesPerColumnName := map[string][]string{}

	res := &stitching{}
	res.origTableName = origTableName
	res.tableName = "__quesma_stitched_table"

	for count, table := range tables {

		tableDef, ok := tableDefinition.Load(table)

		if !ok {
			logger.Error().Msgf("Table %s not found", table)
			continue
		}

		subStitch := stitchTable{
			tableName: table,
			cteName:   fmt.Sprintf("__quesma_sub_%d", count),
			tableDef:  tableDef,
		}

		res.stitches = append(res.stitches, subStitch)

		for _, col := range tableDef.Cols {
			if _, ok := tablesPerColumnName[col.Name]; !ok {
				tablesPerColumnName[col.Name] = []string{subStitch.cteName}
			} else {
				tablesPerColumnName[col.Name] = append(tablesPerColumnName[col.Name], subStitch.cteName)
			}
		}
	}

	// collions resolution
	// if we have a column that is present in more than one table, we need to coalesce it
	var columns []model.Expr
	for col, tablesWithColumn := range tablesPerColumnName {

		if len(tablesWithColumn) == 1 {
			columns = append(columns, model.NewColumnRef(col))
		} else {

			var fullTableCols []model.Expr

			for _, tableName := range tablesWithColumn {
				fullTableCols = append(fullTableCols, model.NewLiteral(fmt.Sprintf("\"%s\".\"%s\"", tableName, col)))
			}
			columns = append(columns, model.NewAliasedExpr(model.NewFunction("COALESCE", fullTableCols...), col))
		}
	}

	stm := &model.SelectCommand{
		Columns: columns,
	}

	res.joined = stm

	joinType := "FULL OUTER"
	onExpr := model.NewLiteral("FALSE")

	var joiner func(first stitchTable, rest []stitchTable) model.JoinExpr

	joiner = func(first stitchTable, rest []stitchTable) model.JoinExpr {
		if len(rest) == 1 {
			return model.NewJoinExpr(model.NewTableRef(first.cteName), model.NewTableRef(rest[0].cteName), joinType, onExpr)
		}
		return model.NewJoinExpr(model.NewTableRef(first.cteName), joiner(rest[0], rest[1:]), joinType, onExpr)
	}

	join := joiner(res.stitches[0], res.stitches[1:])
	stm.FromClause = join

	return res
}

func (t *StitchingTransformer) Transform(query []*model.Query) ([]*model.Query, error) {

	var result []*model.Query

	stitch := t.stitch(query)

	if stitch == nil {
		return query, nil
	}

	for _, q := range query {

		if q.NoDBQuery {
			result = append(result, q)
			continue
		}

		stm := q.SelectCommand
		inputQuery := q.SelectCommand.String()

		q.SelectCommand = stitch.stitch(stm)
		outputQuery := q.SelectCommand.String()

		if inputQuery != outputQuery {
			q.TransformationHistory.SchemaTransformers = append(q.TransformationHistory.SchemaTransformers, t.Name())

			logger.Info().Msgf(t.Name()+" triggered, input query: %s", inputQuery)
			logger.Info().Msgf(t.Name()+" triggered, output query: %s", outputQuery)
		}

		result = append(result, q)
	}

	return result, nil
}
