// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package qpl_experiment

import (
	"fmt"
	"log"
	"strings"
)

func cellValue(a interface{}) string {

	switch a := a.(type) {
	case *int:
		if a != nil {
			return fmt.Sprintf("%d", *a)
		} else {
			return "nil"
		}

	case *string:
		if a != nil {
			return *a
		} else {
			return "nil"
		}
	case *int64:
		if a != nil {
			return fmt.Sprintf("%d", *a)
		} else {
			return "nil"
		}

	case *interface{}:
		if a != nil {
			return fmt.Sprintf("%v", *a)
		} else {
			return "nil"
		}
	default:
		return fmt.Sprintf("%v", a)
	}

}

type SQLTVF struct {
	Database string
	Query    string
}

func readTable(database, query string) (Table, error) {
	res := Table{}

	log.Println("Executing Query: ", database, query)

	db := databases[database]

	rows, err := db.Query(query)
	if err != nil {
		return res, err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return res, err
	}

	res.Names = cols

	log.Println("cols:", res.Names)
	for rows.Next() {
		vals := make([]interface{}, len(res.Names))
		for i := range cols {
			vals[i] = new(interface{})
		}

		err = rows.Scan(vals...)
		if err != nil {
			return res, err
		}

		row := make(Row, len(res.Names))
		for i := range res.Names {
			row[i] = cellValue(*(vals[i].(*interface{})))
		}

		res.Rows = append(res.Rows, row)
	}

	if err := rows.Err(); err != nil {
		return res, err
	}

	return res, nil
}

func writeTable(database, table string, tabular Table) error {
	db := databases[database]

	dropTableSQL := fmt.Sprintf("DROP TABLE IF EXISTS %s", table)

	_, err := db.Exec(dropTableSQL)
	if err != nil {
		//
	}

	t := "String"
	if database == "clickhouse" {
		t = "String"
	}
	if database == "postgres" {
		t = "TEXT"
	}

	createTableSQL := fmt.Sprintf("CREATE TABLE %s (", table)

	cols := make([]string, len(tabular.Names))

	for i, name := range tabular.Names {
		cols[i] = fmt.Sprintf("%s %s", fmt.Sprintf("\"%s\"", name), t)
	}

	createTableSQL += strings.Join(cols, ",") + ")"

	if database == "clickhouse" {
		createTableSQL += fmt.Sprintf(" Engine=MergeTree ORDER BY(\"%s\")", tabular.Names[0])
	}

	log.Println("Ceating table with SQL: ", database, createTableSQL)

	_, err = db.Exec(createTableSQL)
	if err != nil {
		return fmt.Errorf("error creating table: %w", err)
	}

	placeHolders := make([]string, len(tabular.Names))

	for i := range tabular.Names {
		placeHolders[i] = "?"
	}

	for _, row := range tabular.Rows {

		values := make([]string, len(row))
		for i, v := range row {
			values[i] = fmt.Sprintf("'%v'", v)
		}

		insertQuery := fmt.Sprintf("INSERT INTO %s VALUES (%s)", table, strings.Join(values, ","))

		log.Println("Inserting data with SQL: ", database, insertQuery)

		_, err = db.Exec(insertQuery)

		if err != nil {
			return fmt.Errorf("error inserting data: %w", err)
		}
	}

	return nil
}

func (s *SQLTVF) Fn(tabular Table) (Table, error) {

	// create a new table
	// insert data

	err := writeTable(s.Database, "input", tabular)

	if err != nil {
		return EmptyTable(), err
	}

	return readTable(s.Database, s.Query)
}

type ToSqlTVF struct {
	database string
	name     string
}

func (t *ToSqlTVF) Fn(tabular Table) (Table, error) {

	// create a new table
	// insert data

	err := writeTable(t.database, t.name, tabular)

	if err != nil {
		return EmptyTable(), err
	}

	return tabular, nil
}
