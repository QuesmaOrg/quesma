package clickhouse

import (
	"database/sql"
	"fmt"
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

func (lm *LogManager) GetAttributesList(tableName string) []Attribute {
	table := lm.findSchema(tableName)
	if table == nil {
		return make([]Attribute, 0)
	}
	return table.Config.attributes
}

// TODO Won't work with tuples, e.g. trying to access via tupleName.tupleField will return NotExists,
// instead of some other response. Fix this when needed (we seem to not need tuples right now)
func (lm *LogManager) GetFieldInfo(tableName string, fieldName string) FieldInfo {
	table := lm.findSchema(tableName)
	if table == nil {
		return NotExists
	}
	col, ok := table.Cols[fieldName]
	if !ok {
		return NotExists
	}
	if col.isArray() {
		return ExistsAndIsArray
	}
	return ExistsAndIsBaseType
}

// TODO again, fix tuples.
// t tuple(a String, b String) should return [t.a, t.b], now returns [t]
func (lm *LogManager) GetFieldsList(tableName string) []string {
	table := lm.findSchema(tableName)
	if table == nil {
		return make([]string, 0)
	}
	fieldNames := make([]string, 0, len(table.Cols))
	for colName := range table.Cols {
		fieldNames = append(fieldNames, colName)
	}
	return fieldNames
}

// ProcessSimpleSelectQuery - only WHERE clause
// TODO query param should be type safe Query representing all parts of
// sql statement that were already parsed and not string from which
// we have to extract again different parts like where clause and columns to build a proper result
func (lm *LogManager) ProcessSimpleSelectQuery(query *model.Query) ([]model.QueryResultRow, error) {
	table, err := lm.findSchemaAndInitConnection(query.TableName)
	if table == nil {
		return nil, fmt.Errorf("table not found1 [%s]", query.TableName)
	}

	if err != nil {
		return nil, err
	}
	colNames, err := table.extractColumns(query)
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
func (lm *LogManager) ProcessNMostRecentRowsQuery(query *model.Query) ([]model.QueryResultRow, error) {
	table, err := lm.findSchemaAndInitConnection(query.TableName)
	if table == nil {
		return nil, fmt.Errorf("table not found2 [%s]", query.TableName)
	}
	if err != nil {
		return nil, err
	}
	colNames, err := table.extractColumns(query)
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
	if err := lm.initConnection(); err != nil {
		return nil, err
	}
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
			Value:   time.UnixMilli(timestamp).Format("2006-01-02T15:04:05.000"),
		})
	}
	return result, nil
}

// TODO add support for autocomplete for attributes, if we'll find it needed
func (lm *LogManager) ProcessFacetsQuery(query *model.Query) ([]model.QueryResultRow, error) {
	table, err := lm.findSchemaAndInitConnection(query.TableName)
	if table == nil {
		return nil, fmt.Errorf("table not found3 [%s]", query.TableName)
	}

	if err != nil {
		return nil, err
	}

	colNames, err := table.extractColumns(query)
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
	sort.Slice(resultRows, func(i, j int) bool {
		return resultRows[i].Cols[model.ResultColDocCountIndex].Value.(uint64) > resultRows[j].Cols[model.ResultColDocCountIndex].Value.(uint64)
	})
	return resultRows, nil
}

func (lm *LogManager) ProcessAutocompleteSuggestionsQuery(query *model.Query) ([]model.QueryResultRow, error) {
	if err := lm.initConnection(); err != nil {
		return nil, err
	}
	rowsDB, err := lm.chDb.Query(strings.Replace(query.String(), "SELECT", "SELECT DISTINCT", 1))
	if err != nil {
		return nil, fmt.Errorf("query >> %v", err)
	}
	rowToScan := []interface{}{""}
	return read(rowsDB, query.Fields, rowToScan)
}

func (lm *LogManager) ProcessTimestampQuery(query *model.Query) ([]model.QueryResultRow, error) {
	if err := lm.initConnection(); err != nil {
		return nil, err
	}
	rows, err := lm.chDb.Query(query.String())
	if err != nil {
		return nil, fmt.Errorf("query >> %v", err)
	}
	return read(rows, query.Fields, []interface{}{time.Time{}})
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
