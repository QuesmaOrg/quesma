// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package qpl_experiment

import (
	"database/sql"
	"github.com/ClickHouse/clickhouse-go/v2"
	_ "github.com/lib/pq"
	"log"
)

var databases map[string]*sql.DB

func connectClickhouse() *sql.DB {
	log.Println("Connecting to Clickhouse")
	options := clickhouse.Options{Addr: []string{"localhost:9000"}}

	db := clickhouse.OpenDB(&options)

	err := db.Ping()

	if err != nil {
		log.Fatal(err)
	}

	return db
}

func connectPostgres() *sql.DB {

	log.Println("Connecting to Postgres")

	db, err := sql.Open("postgres", "host=localhost port=5432 user=postgres password=postgres dbname=postgres sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}

	err = db.Ping()
	if err != nil {
		log.Fatal(err)
	}

	return db

}

func main() {
	log.Println("Starting ")

	databases = make(map[string]*sql.DB)

	databases["clickhouse"] = connectClickhouse()
	databases["postgres"] = connectPostgres()

	log.Println("Connection: ", databases)

	pg := &postgreSqlServer{}
	pg.startAndListen("localhost:15432")
}
