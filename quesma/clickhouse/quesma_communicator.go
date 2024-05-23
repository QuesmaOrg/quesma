package clickhouse

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"mitmproxy/quesma/end_user_errors"
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/model"
	"sort"
	"strings"
	"time"
)

// Implementation of API for Quesma

type FieldInfo = int

const (
	NotExists FieldInfo = iota
	ExistsAndIsBaseType
	ExistsAndIsArray
)

func (lm *LogManager) Query(ctx context.Context, query string) (*sql.Rows, error) {
	rows, err := lm.chDb.QueryContext(ctx, query)
	return rows, err
}

// GetAllColumns - returns all columns for a given table including non-schema fields
func (lm *LogManager) GetAllColumns(table *Table, query *model.Query) []string {
	columns, err := table.extractColumns(query, true)
	if err != nil {
		logger.Error().Msgf("Failed to extract columns from query: %v", err)
		return nil
	}
	return columns
}

// ProcessQuery - only WHERE clause
// TODO query param should be type safe Query representing all parts of
// sql statement that were already parsed and not string from which
// we have to extract again different parts like where clause and columns to build a proper result
func (lm *LogManager) ProcessQuery(ctx context.Context, table *Table, query *model.Query, columns []string) ([]model.QueryResultRow, error) {
	if query.NoDBQuery {
		return make([]model.QueryResultRow, 0), nil
	}
	colNames, err := table.extractColumns(query, false)
	if query.IsWildcard() {
		sort.Strings(colNames)
	}
	if columns == nil {
		columns = lm.GetAllColumns(table, query)
		// We should sort only if columns are not provided
		// Caller is responsible for providing columns in the right order
		if query.IsWildcard() {
			sort.Strings(columns)
		}
	}
	rowToScan := make([]interface{}, len(colNames)+len(query.NonSchemaFields))
	if err != nil {
		return nil, err
	}

	// will become: rows, err := executeQuery(ctx, lm, query.StringFromColumns(colNames, true), columns, rowToScan)
	rows, err := executeQuery(ctx, lm, query.StringFromColumns(colNames), columns, rowToScan)
	if err == nil {
		for _, row := range rows {
			row.Index = table.Name
		}
	}
	return rows, err
}

var random = rand.New(rand.NewSource(time.Now().UnixNano()))

const slowQueryThreshold = 30 * time.Second
const slowQuerySampleRate = 0.1

func (lm *LogManager) shouldExplainQuery(elapsed time.Duration) bool {
	return elapsed > slowQueryThreshold && random.Float64() < slowQuerySampleRate
}

func (lm *LogManager) explainQuery(ctx context.Context, query string, elapsed time.Duration) {

	explainQuery := "EXPLAIN json=1, indexes=1 " + query

	rows, err := lm.chDb.QueryContext(ctx, explainQuery)
	if err != nil {
		logger.ErrorWithCtx(ctx).Msgf("failed to explain slow query: %v", err)
	}

	defer rows.Close()
	if rows.Next() {
		var explain string
		err := rows.Scan(&explain)
		if err != nil {
			logger.ErrorWithCtx(ctx).Msgf("failed to scan slow query explain: %v", err)
			return
		}

		// reformat the explain output to make it one line and more readable
		explain = strings.ReplaceAll(explain, "\n", "")
		explain = strings.ReplaceAll(explain, "  ", "")

		logger.WarnWithCtx(ctx).Msgf("slow query (time: '%s')  query: '%s' -> explain: '%s'", elapsed, query, explain)
	}

	if rows.Err() != nil {
		logger.ErrorWithCtx(ctx).Msgf("failed to read slow query explain: %v", rows.Err())
	}
}

func executeQuery(ctx context.Context, lm *LogManager, queryAsString string, fields []string, rowToScan []interface{}) ([]model.QueryResultRow, error) {
	span := lm.phoneHomeAgent.ClickHouseQueryDuration().Begin()

	rows, err := lm.Query(ctx, queryAsString)
	if err != nil {
		span.End(err)
		return nil, end_user_errors.GuessClickhouseError(err).InternalDetails("clickhouse: query failed. err: %v, query: %v", err, queryAsString)
	}

	res, err := read(rows, fields, rowToScan)
	elapsed := span.End(nil)
	if err == nil {
		if lm.shouldExplainQuery(elapsed) {
			lm.explainQuery(ctx, queryAsString, elapsed)
		}
	}

	return res, err
}

// 'selectFields' are all values that we return from the query, both columns and non-schema fields,
// like e.g. count(), or toInt8(boolField)
func read(rows *sql.Rows, selectFields []string, rowToScan []interface{}) ([]model.QueryResultRow, error) {
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
	err := rows.Close()
	if err != nil {
		return nil, fmt.Errorf("clickhouse: closing rows failed: %v", err)
	}
	return resultRows, nil
}
