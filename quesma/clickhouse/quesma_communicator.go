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
type FieldAtIndex = int // for facets/histogram what Cols[i] means

const (
	NotExists FieldInfo = iota
	ExistsAndIsBaseType
	ExistsAndIsArray
)

type QueryResultCol struct {
	ColName string // quoted, e.g. `"message"`
	Value   interface{}
}

type QueryResultRow struct {
	Cols []QueryResultCol
}

const (
	Key         FieldAtIndex = iota // for facets/histogram Col[0] == Key
	DocCount                        // for facets/histogram Col[1] == DocCount
	KeyAsString                     // for histogram Col[2] == KeyAsString
)

func NewQueryResultCol(colName string, value interface{}) QueryResultCol {
	return QueryResultCol{ColName: colName, Value: value}
}

func (c QueryResultCol) String() string {
	switch c.Value.(type) {
	case string, time.Time:
		return fmt.Sprintf(`"%s": "%v"`, c.ColName, c.Value)
	default:
		return fmt.Sprintf(`"%s": %v`, c.ColName, c.Value)
	}
}

func (r QueryResultRow) String() string {
	str := strings.Builder{}
	str.WriteString(indent(1) + "{\n")
	numCols := len(r.Cols)
	i := 0
	for _, col := range r.Cols {
		str.WriteString(indent(2) + col.String())
		if i < numCols-1 {
			str.WriteString(",")
		}
		str.WriteString("\n")
		i++
	}
	str.WriteString("\n" + indent(1) + "}")
	return str.String()
}

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
func (lm *LogManager) ProcessSimpleSelectQuery(query *model.Query) ([]QueryResultRow, error) {
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
func (lm *LogManager) ProcessNMostRecentRowsQuery(query *model.Query) ([]QueryResultRow, error) {
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

func (lm *LogManager) ProcessHistogramQuery(query *model.Query, bucket time.Duration) ([]QueryResultRow, error) {
	err := lm.initConnection()
	if err != nil {
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
		return result[i].Cols[Key].Value.(int64) < result[j].Cols[Key].Value.(int64)
	})

	for i := range result {
		result[i].Cols[Key].Value = result[i].Cols[Key].Value.(int64) * bucket.Milliseconds()
		result[i].Cols = append(result[i].Cols, QueryResultCol{
			ColName: "key_as_string",
			Value:   time.UnixMilli(result[i].Cols[Key].Value.(int64)).Format("2006-01-02T15:04:05.000"),
		})
	}
	return result, nil
}

// TODO add support for autocomplete for attributes, if we'll find it needed
func (lm *LogManager) ProcessFacetsQuery(query *model.Query) ([]QueryResultRow, error) {
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
		return resultRows[i].Cols[DocCount].Value.(uint64) > resultRows[j].Cols[DocCount].Value.(uint64)
	})
	return resultRows, nil
}

// TODO make it faster? E.g. not search in all rows?
// TODO add support for autocomplete for attributes, if we'll find it needed
// With autocomplete, I assume field is a string, so we can use iLIKE
func (lm *LogManager) ProcessAutocompleteSuggestionsQuery(query *model.Query) ([]QueryResultRow, error) {
	err := lm.initConnection()
	if err != nil {
		return nil, err
	}
	rowsDB, err := lm.chDb.Query(strings.Replace(query.String(), "SELECT", "SELECT DISTINCT", 1))
	if err != nil {
		return nil, fmt.Errorf("query >> %v", err)
	}
	rowToScan := []interface{}{""}
	return read(rowsDB, query.Fields, rowToScan)
}

func (lm *LogManager) ProcessTimestampQuery(query *model.Query) ([]QueryResultRow, error) {
	err := lm.initConnection()
	if err != nil {
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
func read(rows *sql.Rows, selectFields []string, rowToScan []interface{}) ([]QueryResultRow, error) {
	rowDb := make([]interface{}, 0, len(rowToScan))
	for i := range rowToScan {
		rowDb = append(rowDb, &rowToScan[i])
	}
	resultRows := make([]QueryResultRow, 0)
	for rows.Next() {
		err := rows.Scan(rowDb...)
		if err != nil {
			return nil, fmt.Errorf("scan >> %v", err)
		}
		resultRow := QueryResultRow{Cols: make([]QueryResultCol, len(selectFields))}
		for i, field := range selectFields {
			resultRow.Cols[i] = QueryResultCol{ColName: field, Value: rowToScan[i]}
		}
		resultRows = append(resultRows, resultRow)
	}
	return resultRows, nil
}
