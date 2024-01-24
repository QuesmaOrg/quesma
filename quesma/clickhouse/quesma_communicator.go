package clickhouse

import (
	"database/sql"
	"fmt"
	"log"
	"sort"
	"strconv"
	"strings"
	"time"
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

type HistogramResult struct {
	start time.Time
	end   time.Time
	count int
}

func (c QueryResultCol) String() string {
	switch c.Value.(type) {
	case string:
		return fmt.Sprintf(`%s: "%v"`, c.ColName, c.Value)
	default:
		return fmt.Sprintf(`%s: "%v"`, c.ColName, c.Value)
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

func (hr HistogramResult) String() string {
	return fmt.Sprintf("%v - %v, count: %v", hr.start, hr.end, hr.count)
}

func extractTableName(query string) (string, error) {
	// Convert the query to lowercase for case-insensitivity
	queryLower := strings.ToLower(query)

	words := strings.Fields(queryLower)

	for i := 0; i < len(words)-1; i++ {
		if words[i] == "from" {
			// The table name is the next word after "from"
			tableName := words[i+1]
			return tableName, nil
		}
	}

	return "", fmt.Errorf("table name not found in the query")
}

func extractWhereClause(query string) (string, error) {
	// Convert the query to lowercase for case-insensitivity
	queryLower := strings.ToLower(query)

	// Find the index of "where"
	whereIndex := strings.Index(queryLower, "where")

	// Check if "where" is present
	if whereIndex == -1 {
		return "", fmt.Errorf("the 'where' keyword is not present in the query")
	}

	// Extract everything after "where"
	afterWhere := strings.TrimSpace(query[whereIndex+len("where"):])

	return afterWhere, nil
}

func extractColumns(query string) ([]string, error) {
	// Convert the query to lowercase for case-insensitivity
	queryLower := strings.ToLower(query)

	// Find the indices of "select" and "from"
	selectIndex := strings.Index(queryLower, "select")
	fromIndex := strings.Index(queryLower, "from")

	// Check if "select" and "from" are both present
	if selectIndex == -1 || fromIndex == -1 {
		return nil, fmt.Errorf("both 'select' and 'from' keywords are required in the query")
	}

	// Extract the part between "select" and "from"
	partBetween := strings.TrimSpace(query[selectIndex+len("select") : fromIndex])

	// Check if '*' is used in the SELECT statement
	if partBetween == "*" {
		return []string{"*"}, nil
	}

	// Split the part between "select" and "from" into individual columns
	columns := strings.FieldsFunc(partBetween, func(r rune) bool {
		// Split by commas and ignore spaces after commas
		return r == ',' || r == ' '
	})

	// Remove any empty strings from the resulting slice
	var cleanedColumns []string
	for _, col := range columns {
		if col != "" {
			cleanedColumns = append(cleanedColumns, col)
		}
	}

	return cleanedColumns, nil
}

// ProcessSelectQuery
// TODO query param should be type safe Query representing all parts of
// sql statement that were already parsed and not string from which
// we have to extract again different parts like where clause and columns to build a proper result
func (lm *LogManager) ProcessSelectQuery(query string) ([]QueryResultRow, error) {
	tableName, err := extractTableName(query)
	if err != nil {
		log.Println(err)
	}
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
	whereClause, err := extractWhereClause(query)
	if err != nil {
		log.Println(err)
	}

	columnsSql, err := extractColumns(query)
	_ = columnsSql
	if err != nil {
		log.Println(err)
	}

	queryStr := strings.Builder{}
	queryStr.WriteString("SELECT ")
	row := make([]interface{}, 0, len(table.Cols))
	colNames := make([]string, 0, len(table.Cols))

	neededColumns := make(map[string]*Column)

	if len(columnsSql) == 1 && columnsSql[0] == "*" {
		neededColumns = table.Cols
	} else {
		for _, col := range columnsSql {
			if k, ok := table.Cols[col]; ok {
				neededColumns[k.Name] = table.Cols[col]
			}
		}
	}

	for colName, col := range neededColumns {
		colNames = append(colNames, fmt.Sprintf("\"%s\"", colName))
		if col.Type.isBool() {
			queryStr.WriteString("toInt8(" + fmt.Sprintf("\"%s\"", colName) + "),")
		} else {
			queryStr.WriteString(fmt.Sprintf("\"%s\"", colName) + ",")
		}
		row = append(row, col.Type.newZeroValue())
	}
	if len(whereClause) > 0 {
		queryStr.WriteString(" FROM " + tableName + " WHERE " + whereClause)
	} else {
		queryStr.WriteString(" FROM " + tableName)
	}
	query = queryStr.String()

	rowsDB, err := lm.db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("query >> %v", err)
	}

	rowDB := make([]interface{}, len(table.Cols))
	for i := 0; i < len(table.Cols); i++ {
		rowDB[i] = &row[i]

	}

	rows := make([]QueryResultRow, 0)
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
		colNames = append(colNames, fmt.Sprintf("\"%s\"", colName))
		if col.Type.isBool() {
			queryStr.WriteString("toInt8(" + fmt.Sprintf("\"%s\"", colName) + "),")
		} else {
			queryStr.WriteString(fmt.Sprintf("\"%s\"", colName) + ",")
		}
		row = append(row, col.Type.newZeroValue())
	}

	queryStr.WriteString(" FROM " + tableName + " ORDER BY " + fmt.Sprintf("\"%s\"", timestampFieldName) + " DESC LIMIT " + strconv.Itoa(N))
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

func (lm *LogManager) GetHistogram(tableName, timestampFieldName string, duration time.Duration) ([]HistogramResult, error) {
	if lm.db == nil {
		connection, err := sql.Open("clickhouse", url)
		if err != nil {
			return nil, err
		}
		lm.db = connection
	}

	histogramOneBar := durationToHistogramInterval(duration) // 1 bar duration
	gbyStmt := "toInt64(toUnixTimestamp64Milli(" + timestampFieldName + ")/" + strconv.FormatInt(histogramOneBar.Milliseconds(), 10) + ")"
	whrStmt := timestampFieldName + ">=timestamp_sub(SECOND," + strconv.FormatInt(int64(duration.Seconds()), 10) + ", now64())"
	query := "SELECT " + gbyStmt + ", count() FROM " + tableName + " WHERE " + whrStmt + " GROUP BY " + gbyStmt
	rows, err := lm.db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("query >> %v", err)
	}
	histogram := make([]HistogramResult, 0)
	for rows.Next() {
		var start int64
		var count int
		err = rows.Scan(&start, &count)
		if err != nil {
			return nil, fmt.Errorf("scan >> %v", err)
		}
		startMs := start * histogramOneBar.Milliseconds()
		endMs := startMs + histogramOneBar.Milliseconds()
		histogram = append(histogram, HistogramResult{
			start: time.Unix(startMs/1_000, (startMs%1_000)*1_000_000),
			end:   time.Unix(endMs/1_000, (endMs%1_000)*1_000_000),
			count: count,
		})
	}
	sort.Slice(histogram, func(i, j int) bool {
		return histogram[i].start.Before(histogram[j].start)
	})
	return histogram, nil
}

/*
		How Kibana shows histogram (how long one bar is):
	    query duration -> one histogram's bar ...
	    10s  -> 200ms
		14s  -> 280ms
		20s  -> 400ms
		24s  -> 480ms
		25s  -> 1s
		[25s, 4m]   -> 1s
		[5m, 6m]    -> 5s
		[7m, 12m]   -> 10s
		[13m, 37m]  -> 30s
		[38m, 140m] -> 1m
		[150m, 7h]  -> 5m
		[8h, 16h]   -> 10m
		[17h, 37h]  -> 30m
		[38h, 99h]  -> 1h
		[100h, 12d] -> 3h
		[13d, 49d]  -> 12h
		[50d, 340d] -> 1d
		[350d, 34m] -> 7d
		[35m, 15y]  -> 1m
*/

func durationToHistogramInterval(d time.Duration) time.Duration {
	switch {
	case d < 25*time.Second:
		ms := d.Milliseconds() / 50
		ms += 20 - (ms % 20)
		return time.Millisecond * time.Duration(ms)
	case d <= 4*time.Minute:
		return time.Second
	case d < 7*time.Minute:
		return 5 * time.Second
	case d < 13*time.Minute:
		return 10 * time.Second
	case d < 38*time.Minute:
		return 30 * time.Second
	case d <= 140*time.Minute:
		return time.Minute
	case d <= 7*time.Hour:
		return 5 * time.Minute
	case d <= 16*time.Hour:
		return 10 * time.Minute
	case d <= 37*time.Hour:
		return 30 * time.Minute
	case d <= 99*time.Hour:
		return time.Hour
	case d <= 12*24*time.Hour:
		return 3 * time.Hour
	case d <= 49*24*time.Hour:
		return 12 * time.Hour
	case d <= 340*24*time.Hour:
		return 24 * time.Hour
	case d <= 35*30*24*time.Hour:
		return 7 * 24 * time.Hour
	default:
		return 30 * 24 * time.Hour
	}
}

// TODO add support for autocomplete for attributes, if we'll find it needed
func (lm *LogManager) GetFacets(tableName, fieldName string, limit int) ([]QueryResultRow, error) {
	table := lm.findSchema(tableName)
	if table == nil {
		table = lm.findSchema(tableName[1 : len(tableName)-1]) // try remove " " TODO improve this when we get out of the prototype phase
		if table == nil {
			return nil, fmt.Errorf("Table " + tableName + " not found")
		}
	}
	// TODO add support for autocomplete for attributes, if we'll find it needed
	col, ok := table.Cols[fieldName]
	if !ok {
		return nil, fmt.Errorf("Column " + fieldName + " not found")
	}

	if lm.db == nil {
		connection, err := sql.Open("clickhouse", url)
		if err != nil {
			return nil, err
		}
		lm.db = connection
	}

	query := "SELECT " + fieldName + ", count() FROM " + tableName + " GROUP BY " + fieldName
	if limit > 0 {
		query += " LIMIT " + strconv.Itoa(limit)
	}
	rows, err := lm.db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("query >> %v", err)
	}

	value := col.Type.newZeroValue()
	resultRows := make([]QueryResultRow, 0)
	total := 0
	for rows.Next() {
		var count int
		err = rows.Scan(&value, &count)
		if err != nil {
			return nil, fmt.Errorf("scan >> %v", err)
		}
		total += count
		resultRows = append(resultRows, QueryResultRow{Cols: []QueryResultCol{
			{ColName: fieldName, Value: value},
			{ColName: "count", Value: count},
			{ColName: "percentage", Value: ""},
		}})
	}
	for i := range resultRows {
		percentage := float64(resultRows[i].Cols[1].Value.(int)*100) / float64(total)
		resultRows[i].Cols[2].Value = strconv.FormatFloat(percentage, 'f', 1, 64) + "%"
	}
	sort.Slice(resultRows, func(i, j int) bool {
		return resultRows[i].Cols[1].Value.(int) > resultRows[j].Cols[1].Value.(int)
	})
	return resultRows, nil
}

// TODO make it faster? E.g. not search in all rows?
// TODO add support for autocomplete for attributes, if we'll find it needed
func (lm *LogManager) GetAutocompleteSuggestions(tableName, fieldName, prefix string, limit int) ([]QueryResultRow, error) {
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
			return nil, err
		}
		lm.db = connection
	}

	// TODO add support for autocomplete for attributes, if we'll find it needed
	col, ok := table.Cols[fieldName]
	if !ok {
		return nil, fmt.Errorf("Column " + fieldName + " not found")
	}

	query := "SELECT DISTINCT " + fieldName + " FROM " + tableName
	if prefix != "" {
		if !col.Type.isString() {
			query += " WHERE toString(" + fieldName + ")"
		} else {
			query += " WHERE " + fieldName
		}
		query += " LIKE '" + prefix + "%'"
	}
	if limit > 0 {
		query += " LIMIT " + strconv.Itoa(limit)
	}
	rowsDB, err := lm.db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("query >> %v", err)
	}

	value := col.Type.newZeroValue()
	rows := make([]QueryResultRow, 0)
	for rowsDB.Next() {
		err = rowsDB.Scan(&value)
		if err != nil {
			return nil, fmt.Errorf("scan >> %v", err)
		}
		rows = append(rows, QueryResultRow{Cols: []QueryResultCol{{ColName: fieldName, Value: value}}})
	}
	return rows, nil
}
