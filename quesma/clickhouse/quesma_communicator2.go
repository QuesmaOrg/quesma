// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package clickhouse

import (
	"context"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	"quesma/end_user_errors"
	"quesma/model"
	quesma_api "quesma_v2/core"
	"strconv"
	"strings"
)

func (lm *LogManager2) Query(ctx context.Context, query string) (*quesma_api.Rows, error) {
	rows, err := lm.chDb.Query(ctx, query)
	return &rows, err
}

// ProcessQuery - only WHERE clause
// TODO query param should be type safe Query representing all parts of
// sql statement that were already parsed and not string from which
// we have to extract again different parts like where clause and columns to build a proper result
func (lm *LogManager2) ProcessQuery(ctx context.Context, table *Table, query *model.Query) (rows []model.QueryResultRow, performanceResult PerformanceResult, err error) {
	rowToScan := make([]interface{}, len(query.SelectCommand.Columns))
	columns := make([]string, 0, len(query.SelectCommand.Columns))

	for count, col := range query.SelectCommand.Columns {
		var colName string

		switch col := col.(type) {
		case model.ColumnRef:
			colName = col.ColumnName
		case model.AliasedExpr:
			colName = col.Alias
		case model.LiteralExpr:

			// This should be moved to the SchemaCheck pipeline. It'll require to change a lot of tests.
			//
			// It can be removed just after the pancake will be the only way to generate SQL.
			// Pancake SQL are aliased properly.

			if str, isStr := col.Value.(string); isStr {
				if unquoted, err := strconv.Unquote(str); err == nil {
					colName = unquoted
				} else {
					colName = str
				}
			} else {
				if colName == "" {
					colName = fmt.Sprintf("column_%d", count)
				}
			}
		default:
			if colName == "" {
				colName = fmt.Sprintf("column_%d", count)
			}
		}

		columns = append(columns, colName)

	}

	rows, performanceResult, err = executeQuery2(ctx, lm, query, columns, rowToScan)

	if err == nil {
		for _, row := range rows {
			row.Index = table.Name
		}
	}
	return rows, performanceResult, err
}

func executeQuery2(ctx context.Context, lm *LogManager2, query *model.Query, fields []string, rowToScan []interface{}) (res []model.QueryResultRow, performanceResult PerformanceResult, err error) {
	//span := lm.phoneHomeAgent.ClickHouseQueryDuration().Begin(). TODO THIS IS NIL

	queryAsString := query.SelectCommand.String()

	// We drop privileges for the query
	//
	// https://clickhouse.com/docs/en/operations/settings/permissions-for-queries
	//

	settings := make(clickhouse.Settings)
	// this "readonly" setting turned out to be causing problems with Hydrolix queries
	// the queries looked pretty legit, but the key difference was the use of schema (`FROM "schema"."tableName"`)
	// to be revisited in the future
	// settings["readonly"] = "1"
	settings["allow_ddl"] = "0"

	if query.OptimizeHints != nil {
		for k, v := range query.OptimizeHints.ClickhouseQuerySettings {
			settings[k] = v
		}

		if len(query.OptimizeHints.OptimizationsPerformed) > 0 {
			queryAsString = queryAsString + "\n-- optimizations: " + strings.Join(query.OptimizeHints.OptimizationsPerformed, ", ") + "\n"
		}
	}

	queryID := getQueryId(ctx)
	performanceResult.QueryID = queryID

	ctx = clickhouse.Context(ctx, clickhouse.WithSettings(settings), clickhouse.WithQueryID(queryID))

	rows, err := lm.chDb.Query(ctx, queryAsString)
	if err != nil {
		//elapsed := span.End(err)
		//performanceResult.Duration = elapsed
		performanceResult.Error = err
		return nil, performanceResult, end_user_errors.GuessClickhouseErrorType(err).InternalDetails("clickhouse: query failed. err: %v, query: %v", err, queryAsString)
	}
	res, err = read2(rows, fields, rowToScan)

	//elapsed := span.End(nil)
	//performanceResult.Duration = elapsed
	performanceResult.RowsReturned = len(res)
	if err == nil {
		//if shouldExplainQuery(elapsed) { TODO !!!!
		//	performanceResult.ExplainPlan = lm.explainQuery(ctx, queryAsString, elapsed)
		//}
	}

	return res, performanceResult, err
}

func read2(rows quesma_api.Rows, selectFields []string, rowToScan []interface{}) ([]model.QueryResultRow, error) {

	// read selected fields from the metadata

	rowDb := make([]interface{}, 0, len(rowToScan))
	for i := range rowToScan {
		rowDb = append(rowDb, &rowToScan[i])
	}
	resultRows := make([]model.QueryResultRow, 0)
	for rows.Next() {
		err := rows.Scan(rowDb...)
		if err != nil {
			return nil, fmt.Errorf("clickhouse: scan failed: %v", err)
		}
		resultRow := model.QueryResultRow{Cols: make([]model.QueryResultCol, len(selectFields))}
		for i, field := range selectFields {
			resultRow.Cols[i] = model.QueryResultCol{ColName: field, Value: rowToScan[i]}
		}
		resultRows = append(resultRows, resultRow)
	}
	if rows.Err() != nil {
		return nil, fmt.Errorf("clickhouse: iterating over rows failed:  %v", rows.Err())
	}
	//err := rows.Close()
	//if err != nil {
	//	return nil, fmt.Errorf("clickhouse: closing rows failed: %v", err)
	//}
	rows.Close()
	return resultRows, nil
}
