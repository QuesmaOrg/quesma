package clickhouse

import (
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

// ProcessSimpleSelectQuery - only WHERE clause
// TODO query param should be type safe Query representing all parts of
// sql statement that were already parsed and not string from which
// we have to extract again different parts like where clause and columns to build a proper result
func (lm *LogManager) ProcessSimpleSelectQuery(table *Table, query *model.Query) ([]model.QueryResultRow, error) {
	colNames, err := table.extractColumns(query, false)
	rowToScan := make([]interface{}, len(colNames)+len(query.NonSchemaFields))
	if err != nil {
		return nil, err
	}
	rowsDB, err := lm.chDb.Query(query.StringFromColumns(colNames))
	if err != nil {
		return nil, fmt.Errorf("query >> %v", err)
	}
	return read(rowsDB, append(colNames, query.NonSchemaFields...), rowToScan)
}

// fieldName = "*" -> we query all, otherwise only this 1 field
func (lm *LogManager) ProcessNRowsQuery(table *Table, query *model.Query) ([]model.QueryResultRow, error) {
	colNames, err := table.extractColumns(query, false)
	if err != nil {
		return nil, err
	}
	rowsDB, err := lm.chDb.Query(query.StringFromColumns(colNames))
	if err != nil {
		return nil, fmt.Errorf("query >> %v", err)
	}
	rowToScan := make([]interface{}, len(colNames))
	return read(rowsDB, append(colNames, query.NonSchemaFields...), rowToScan)
}

func (lm *LogManager) ProcessHistogramQuery(query *model.Query, bucket time.Duration) ([]model.QueryResultRow, error) {
	rows, err := lm.chDb.Query(query.String())
	if err != nil {
		return nil, fmt.Errorf("query >> %v", err)
	}
	result, err := read(rows, []string{"key", "doc_count"}, []interface{}{int64(0), uint64(0)})
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
func (lm *LogManager) ProcessFacetsQuery(table *Table, query *model.Query) ([]model.QueryResultRow, error) {
	colNames, err := table.extractColumns(query, false)
	rowToScan := make([]interface{}, len(colNames)+len(query.NonSchemaFields))
	if err != nil {
		return nil, err
	}
	rows, err := lm.chDb.Query(query.StringFromColumns(colNames))
	if err != nil {
		return nil, fmt.Errorf("query >> %v", err)
	}
	resultRows, err := read(rows, []string{"key", "doc_count"}, rowToScan)
	if err != nil {
		return nil, err
	}
	return resultRows, nil
}

func (lm *LogManager) ProcessAutocompleteSuggestionsQuery(query *model.Query) ([]model.QueryResultRow, error) {
	rowsDB, err := lm.chDb.Query(query.String())
	if err != nil {
		return nil, fmt.Errorf("query >> %v", err)
	}
	rowToScan := []interface{}{""}
	return read(rowsDB, query.Fields, rowToScan)
}

func (lm *LogManager) ProcessTimestampQuery(query *model.Query) ([]model.QueryResultRow, error) {
	rows, err := lm.chDb.Query(query.String())
	if err != nil {
		return nil, fmt.Errorf("query >> %v", err)
	}
	return read(rows, query.Fields, []interface{}{time.Time{}})
}

func (lm *LogManager) ProcessGeneralAggregationQuery(table *Table, query *model.Query) ([]model.QueryResultRow, error) {
	rows, err := lm.chDb.Query(query.String())
	if err != nil {
		return nil, fmt.Errorf("query >> %v", err)
	}
	colNames, err := table.extractColumns(query, true)
	if err != nil {
		return nil, err
	}
	rowToScan := make([]interface{}, len(colNames))
	result, err := read(rows, colNames, rowToScan)
	return result, err
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
			return nil, fmt.Errorf("scan >> %v", err)
		}
		resultRow := model.QueryResultRow{Cols: make([]model.QueryResultCol, len(selectFields))}
		for i, field := range selectFields {
			resultRow.Cols[i] = model.QueryResultCol{ColName: field, Value: rowToScan[i]}
		}
		resultRows = append(resultRows, resultRow)
	}
	return resultRows, nil
}
