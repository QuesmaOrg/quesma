package clickhouse

import (
	"database/sql"
	"fmt"
	"mitmproxy/quesma/logger"
	"sort"
	"strings"
)

func createAllLogsView(db *sql.DB) error {

	allColumns := make(map[string]int)

	columns := make(map[string]map[string]int)
	columnType := make(map[string]map[string]string)

	query := `select column_name, table_name from information_schema.columns where table_schema = 'default' and table_name <> 'all_logs'`

	rows, err := db.Query(query)
	for rows.Next() {

		var columnName, tableName string
		err = rows.Scan(&columnName, &tableName)
		if err != nil {
			return err
		}

		allColumns[columnName] = 1

		if _, ok := columns[tableName]; !ok {
			columns[tableName] = make(map[string]int)
			columnType[tableName] = make(map[string]string)
		}
		columns[tableName][columnName] = 1

	}

	if err != nil {
		return err
	}

	var columnsInOrder []string
	for k := range allColumns {

		if k == "@timestamp" {
			continue
		}

		columnsInOrder = append(columnsInOrder, k)
	}
	sort.Strings(columnsInOrder)

	var subQueries []string

	var tablesInOrder []string
	for tableName := range columns {
		tablesInOrder = append(tablesInOrder, tableName)
	}
	sort.Strings(tablesInOrder)

	{
		// FAKE first table with all columns as empty STRINGs
		// first table in unions is used by clickhouse to determine the type of the column

		//
		// could not create all_logs view: code: 386, message: There is no supertype for types Nullable(Nothing), Array(String), Nullable(Nothing), Nullable(Nothing), Nullable(Nothing), Nullable(Nothing), Nullable(Nothing), Nullable(Nothing), Nullable(Nothing) because some of them are Array and some of them are not
		//

		var subQueryColumns []string
		subQueryColumns = append(subQueryColumns, "toDateTime('2000-01-01 00:00:00')"+` AS "@timestamp"`)
		subQueryColumns = append(subQueryColumns, "'fake'"+` AS "xxx_table_name"`)

		for _, columnName := range columnsInOrder {
			name := `"` + columnName + `"`

			subQueryColumns = append(subQueryColumns, "'' AS "+name)
		}
		q := `SELECT ` + strings.Join(subQueryColumns, ",\n") + "\n"
		subQueries = append(subQueries, q)
	}

	for _, tableName := range tablesInOrder {
		var subQueryColumns []string

		if _, ok := columns[tableName]["@timestamp"]; ok {
			subQueryColumns = append(subQueryColumns, `toDateTime("@timestamp")`)
		} else {
			switch tableName {
			case "device_logs":
				continue
				//  Cannot parse string '2024-06-06 15:53:30.000' as DateTime: syntax error at position 19 (parsed just '2024-06-06 15:53:30'): while executing 'FUNCTION toDateTime(toString(epoch_time) :: 3) -> toDateTime(toString(epoch_time)) DateTime
				//subQueryColumns = append(subQueryColumns, `toDateTime(epoch_time) AS "@timestamp"`)

			case "kibana_sample_data_logs":
				subQueryColumns = append(subQueryColumns, `toDateTime(timestamp) AS "@timestamp"`)

			case "kibana_sample_ecommerce":
				subQueryColumns = append(subQueryColumns, `toDateTime(order_date) AS "@timestamp"`)

			default:
				logger.Warn().Msgf("table %s does not have @timestamp column", tableName)
				// FAKE field for @timestamp
				subQueryColumns = append(subQueryColumns, "toDateTime('2000-01-01 00:00:00')"+` AS "@timestamp"`)
			}
		}

		subQueryColumns = append(subQueryColumns, "'"+tableName+"'"+` AS "xxx_table_name"`)

		for _, columnName := range columnsInOrder {
			name := `"` + columnName + `"`

			if _, ok := columns[tableName][columnName]; ok {
				subQueryColumns = append(subQueryColumns, "toString("+name+") AS "+name)
			} else {
				subQueryColumns = append(subQueryColumns, "NULL AS "+name)
			}
		}

		q := `SELECT ` + strings.Join(subQueryColumns, ",\n") + ` FROM "` + tableName + `"` + "\n"
		subQueries = append(subQueries, q)
	}
	createQuery := "CREATE VIEW all_logs AS  \n"
	createQuery += strings.Join(subQueries, "\n       UNION ALL    \n\n")

	_, err = db.Exec("DROP VIEW IF EXISTS all_logs")
	if err != nil {
		return err
	}

	fmt.Println("Creating view all_logs", createQuery)
	_, err = db.Exec(createQuery)
	if err != nil {
		return err
	}

	return nil
}
