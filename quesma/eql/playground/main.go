// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package main

import (
	"bufio"
	"database/sql"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/QuesmaOrg/quesma/quesma/eql"
	"github.com/QuesmaOrg/quesma/quesma/eql/transform"
	"os"
	"strings"
	"text/tabwriter"
)

// EQL client
// It connects to the local clickhouse server and allows to execute EQL queries
// on windows_logs table.
//
// Notice table  will be available only if you have downloaded .json file.
// Please consult the docker/device-log-generator/windows_logs.go file for more details.

// You may run this client in the IDE or in the terminal.
//
// To run in the terminal:
// 1. install readline tool, you'll have better experience with command history and editing
// brew install with-readline
// 2. run the client
// cd quesma; with-readline go run eql/playground/main.go
//
// sample queries:
//
// process where process.name : "cmd.exe"

func main() {

	options := clickhouse.Options{Addr: []string{"localhost:9000"}}

	db := clickhouse.OpenDB(&options)

	defer db.Close()
	err := db.Ping()

	if err != nil {
		fmt.Println("Connection error:")
		fmt.Println(err)
		return
	} else {
		fmt.Println("Connection established")
	}

	fmt.Println("EQL client")

	for {
		reader := bufio.NewReader(os.Stdin)
		fmt.Print("EQL> ")
		cmd, err := reader.ReadString('\n')

		if err != nil {
			fmt.Println("read error:", err)
			return
		}

		if cmd == "exit\n" {
			break
		}

		query, parameters := translate(cmd)
		if query == "" {
			continue
		}

		execute(db, query, parameters)
	}
}

func translate(cmd string) (string, map[string]interface{}) {
	translateName := func(name *transform.Symbol) (*transform.Symbol, error) {
		res := strings.ReplaceAll(name.Name, ".", "::")
		res = "\"" + res + "\"" // TODO proper escaping
		return transform.NewSymbol(res), nil
	}

	trans := eql.NewTransformer()
	trans.FieldNameTranslator = translateName
	trans.ExtractParameters = false
	where, parameters, err := trans.TransformQuery(cmd)

	if err != nil {
		fmt.Println("tranform erors:")
		fmt.Println(err)
		return "", nil
	}

	fmt.Printf("where clause: '%s'\n", where)

	sql := `select "@timestamp", "event::category", "process::name", "process::pid", "process::executable" from windows_logs where ` + where
	fmt.Println("SQL: \n" + sql)
	return sql, parameters
}

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

func execute(db *sql.DB, query string, parameters map[string]interface{}) {

	fmt.Println("executing query:", query, parameters)

	var args []any

	for k, v := range parameters {
		args = append(args, sql.Named(k, v))
	}

	rows, err := db.Query(query, args...)

	if err != nil {
		fmt.Println("query error:")
		fmt.Println(err)
		return
	}

	cols, err := rows.Columns()
	if err != nil {
		fmt.Println("cols error:")
		fmt.Println(err)
		return
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 1, ' ', 0)

	fmt.Fprintln(w, strings.Join(cols, "\t"))

	types, err := rows.ColumnTypes()
	if err != nil {
		fmt.Println("types error:")
		fmt.Println(err)
		return
	}

	typesAsString := make([]string, len(types))
	for i, t := range types {
		typesAsString[i] = t.DatabaseTypeName() + " " + t.ScanType().Name()
	}
	fmt.Fprintln(w, strings.Join(typesAsString, "\t"))

	defer rows.Close()

	var count int
	for rows.Next() {
		row := make([]any, len(cols))
		for i := range row {
			row[i] = new(interface{})
		}
		err = rows.Scan(row...)
		if err != nil {
			fmt.Println("scan error:")
			fmt.Println(err)
			break
		} else {

			cels := make([]string, len(row))

			for i := range row {
				cels[i] = cellValue(*row[i].(*interface{}))
			}
			fmt.Fprintln(w, strings.Join(cels, "\t"))
		}
		count++
	}

	if rows.Err() != nil {
		fmt.Println("error while iterating rows:", rows.Err())
		return
	}

	w.Flush()
	fmt.Println("rows count:", count)
}
