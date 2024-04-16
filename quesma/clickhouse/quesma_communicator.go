package clickhouse

import (
	"context"
	"database/sql"
	"fmt"
	"mitmproxy/quesma/model"
	"sort"
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
	span := lm.phoneHomeAgent.ClickHouseQueryDuration().Begin()
	rows, err := lm.chDb.QueryContext(ctx, query)
	span.End(err)
	return rows, err
}

// ProcessSimpleSelectQuery - only WHERE clause
// TODO query param should be type safe Query representing all parts of
// sql statement that were already parsed and not string from which
// we have to extract again different parts like where clause and columns to build a proper result
func (lm *LogManager) ProcessSelectQuery(ctx context.Context, table *Table, query *model.Query) ([]model.QueryResultRow, error) {
	colNames, err := table.extractColumns(query, false)
	rowToScan := make([]interface{}, len(colNames)+len(query.NonSchemaFields))
	if err != nil {
		return nil, err
	}
	rows, err := executeQuery(ctx, lm, table.Name, query.StringFromColumns(colNames), append(colNames, query.NonSchemaFields...), rowToScan)
	if err == nil {
		for _, row := range rows {
			row.Index = table.Name
		}
	}
	return rows, err
}

func (lm *LogManager) ProcessHistogramQuery(ctx context.Context, table *Table, query *model.Query, bucket time.Duration) ([]model.QueryResultRow, error) {
	result, err := executeQuery(ctx, lm, table.Name, query.String(), []string{"key", "doc_count"}, []interface{}{int64(0), uint64(0)})
	if err != nil {
		return nil, err
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].Cols[model.ResultColKeyIndex].Value.(int64) < result[j].Cols[model.ResultColKeyIndex].Value.(int64)
	})
	for i := range result {
		timestamp := result[i].Cols[model.ResultColKeyIndex].Value.(int64) * bucket.Milliseconds()
		result[i].Cols[model.ResultColKeyIndex].Value = timestamp
		result[i].Cols = append(result[i].Cols, model.QueryResultCol{
			ColName: "key_as_string",
			Value:   time.UnixMilli(timestamp).UTC().Format("2006-01-02T15:04:05.000"),
		})
	}
	return result, nil
}

// TODO add support for autocomplete for attributes, if we'll find it needed
func (lm *LogManager) ProcessFacetsQuery(ctx context.Context, table *Table, query *model.Query) ([]model.QueryResultRow, error) {
	colNames, err := table.extractColumns(query, false)
	if err != nil {
		return nil, err
	}
	rowToScan := make([]interface{}, len(colNames)+len(query.NonSchemaFields))
	return executeQuery(ctx, lm, table.Name, query.StringFromColumns(colNames), []string{"key", "doc_count"}, rowToScan)
}

func executeQuery(ctx context.Context, lm *LogManager, tableName string, queryAsString string, fields []string, rowToScan []interface{}) ([]model.QueryResultRow, error) {
	rows, err := lm.Query(ctx, queryAsString)
	if err != nil {
		return nil, fmt.Errorf("query >> %v", err)
	}
	return read(tableName, rows, fields, rowToScan)
}

func (lm *LogManager) ProcessAutocompleteSuggestionsQuery(ctx context.Context, table string, query *model.Query) ([]model.QueryResultRow, error) {
	return executeQuery(ctx, lm, table, query.String(), query.Fields, []interface{}{""})
}

func (lm *LogManager) ProcessTimestampQuery(ctx context.Context, table *Table, query *model.Query) ([]model.QueryResultRow, error) {
	return executeQuery(ctx, lm, table.Name, query.String(), query.Fields, []interface{}{time.Time{}})
}

func (lm *LogManager) ProcessGeneralAggregationQuery(ctx context.Context, table *Table, query *model.Query) ([]model.QueryResultRow, error) {
	colNames, err := table.extractColumns(query, true)
	if err != nil {
		return nil, err
	}
	rowToScan := make([]interface{}, len(colNames))
	return executeQuery(ctx, lm, table.Name, query.String(), colNames, rowToScan)
}

// 'selectFields' are all values that we return from the query, both columns and non-schema fields,
// like e.g. count(), or toInt8(boolField)
func read(tableName string, rows *sql.Rows, selectFields []string, rowToScan []interface{}) ([]model.QueryResultRow, error) {
	rowDb := make([]interface{}, 0, len(rowToScan))
	for i := range rowToScan {
		rowDb = append(rowDb, &rowToScan[i])
	}
	resultRows := make([]model.QueryResultRow, 0)
	for rows.Next() {
		err := rows.Scan(rowDb...)
		if err != nil {
			return nil, fmt.Errorf("scan >> %v", err)
		}
		resultRow := model.QueryResultRow{Index: tableName, Cols: make([]model.QueryResultCol, len(selectFields))}
		for i, field := range selectFields {
			resultRow.Cols[i] = model.QueryResultCol{ColName: field, Value: rowToScan[i]}
		}
		resultRows = append(resultRows, resultRow)
	}
	if rows.Err() != nil {
		return nil, fmt.Errorf("rows >> %v", rows.Err())
	}
	err := rows.Close()
	if err != nil {
		return nil, fmt.Errorf("close >> %v", err)
	}
	return resultRows, nil
}
