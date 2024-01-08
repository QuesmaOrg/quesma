package clickhouse

import (
	"database/sql"
	"fmt"
	_ "github.com/mailru/go-clickhouse"
	"log"
)

const url = "http://clickhouse:8123"

const createTableQuery = `CREATE TABLE IF NOT EXISTS logs
	(
		timestamp DateTime,
		message String
	)
	ENGINE = Log`

type (
	ClickhouseTableManager struct {
	}
	ClickHouseLogManager struct {
		db *sql.DB
	}
	Log struct {
		timestamp string
		message   string
	}
)

func (lm *ClickHouseLogManager) Insert(log string) {
	if lm.db == nil {
		connection, err := sql.Open("clickhouse", url)
		if err != nil {
			fmt.Printf("Open >> %v", err)
		}
		lm.db = connection
	}

}

func (m *ClickhouseTableManager) Migrate() {
	db, err := sql.Open("clickhouse", url)
	if err != nil {
		fmt.Printf("Open >> %v", err)
	}

	defer db.Close()

	_, err = db.Exec(createTableQuery)
	if err != nil {
		log.Fatalf("clickhouse table creation failed: %s", err)
	}
}

func NewTableManager() *ClickhouseTableManager {
	return &ClickhouseTableManager{}
}
