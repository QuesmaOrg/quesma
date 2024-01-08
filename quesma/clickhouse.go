package main

import (
	"database/sql"
	"fmt"
	_ "github.com/mailru/go-clickhouse"
)

const createTableQuery = `CREATE TABLE my_second_table
	(
		user_id UInt32,
		message String,
		timestamp DateTime,
		metric Float32
	)
	ENGINE = MergeTree
	PRIMARY KEY (user_id, timestamp)`

func main() {
	connStr := "http://localhost:8123"
	driver := "clickhouse"
	connect, err := sql.Open(driver, connStr)
	if err != nil {
		fmt.Printf("Open >> %v\n", err)
	}

	_, err = connect.Query(createTableQuery)
	if err != nil {
		fmt.Printf("Query >> %v\n", err)
	}

	err = connect.Ping()
	if err != nil {
		fmt.Printf("Ping >> %v\n", err)
	} else {
		fmt.Print("Ping OK\n")
	}
}
