package clickhouse

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"
)

// Implementation of API for Quesma

type FieldInfo int

const (
	NotExists FieldInfo = iota
	ExistsAndIsBaseType
	ExistsAndIsArray
)

type QueryResultCol struct {
	ColName string
	Value   interface{}
}

type QueryResultRow struct {
	Cols []QueryResultCol
}

func (c QueryResultCol) String() string {
	switch c.Value.(type) {
	case string:
		return fmt.Sprintf(`"%s": "%v"`, c.ColName, c.Value)
	default:
		return fmt.Sprintf(`"%s": %v`, c.ColName, c.Value)
	}
}

func (r QueryResultRow) String() string {
	str := strings.Builder{}
	str.WriteString(indent(1) + "{\n")
	for _, col := range r.Cols {
		str.WriteString(indent(2) + col.String() + ",\n")
	}
	str.WriteString("\n" + indent(1) + "}\n")
	return str.String()
}

// (int, error) just for the 1st version. Should be changed to something more: rows, etc.
func (lm *LogManager) ProcessSelectQuery(query string) (int, error) {
	if lm.db == nil {
		connection, err := sql.Open("clickhouse", url)
		if err != nil {
			return -1, fmt.Errorf("open >> %v", err)
		}
		lm.db = connection
	}

	query = strings.Replace(query, "SELECT *", "SELECT count(*)", 1)
	rows, err := lm.db.Query(query)
	if err != nil {
		return -1, fmt.Errorf("query >> %v", err)
	}
	var cnt int
	if !rows.Next() {
		return -1, fmt.Errorf("no rows")
	}
	err = rows.Scan(&cnt)
	if err != nil {
		return -1, fmt.Errorf("scan >> %v", err)
	}
	return cnt, nil
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

func (lm *LogManager) GetNMostRecentRows(tableName, timestampFieldName string, N int) ([]QueryResultRow, error) {
	table := lm.findSchema(tableName)
	if table == nil {
		table = lm.findSchema(tableName[1 : len(tableName)-1]) // try remove " " TODO improve this when we get out of the prototype phase
		if table == nil {
			return nil, fmt.Errorf("Table " + tableName + " not found")
		}
	}

	if lm.db == nil {
		connection, err := sql.Open("clickhouse", url)
		if err != nil {
			return nil, fmt.Errorf("open >> %v", err)
		}
		lm.db = connection
	}

	queryStr := strings.Builder{}
	queryStr.WriteString("SELECT ")
	row := make([]interface{}, 0, len(table.Cols))
	colNames := make([]string, 0, len(table.Cols))
	for colName, col := range table.Cols {
		colNames = append(colNames, colName)
		if col.Type.isBool() {
			queryStr.WriteString("toInt8(" + colName + "),")
		} else {
			queryStr.WriteString(colName + ",")
		}
		row = append(row, col.Type.newZeroValue())
	}

	queryStr.WriteString(" FROM " + tableName + " ORDER BY " + timestampFieldName + " DESC LIMIT " + strconv.Itoa(N))
	fmt.Println("query string: ", queryStr.String())
	rowsDB, err := lm.db.Query(queryStr.String())
	if err != nil {
		return nil, fmt.Errorf("query >> %v", err)
	}

	rowDB := make([]interface{}, len(table.Cols))
	for i := 0; i < len(table.Cols); i++ {
		rowDB[i] = &row[i]
	}

	rows := make([]QueryResultRow, 0, N)
	for rowsDB.Next() {
		err = rowsDB.Scan(rowDB...)
		if err != nil {
			return nil, fmt.Errorf("scan >> %v", err)
		}
		resultRow := QueryResultRow{Cols: make([]QueryResultCol, 0, len(table.Cols))}
		for i, v := range row {
			resultRow.Cols = append(resultRow.Cols, QueryResultCol{ColName: colNames[i], Value: v})
		}
		rows = append(rows, resultRow)
	}

	return rows, nil
}
