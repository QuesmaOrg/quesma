package clickhouse

import (
	"database/sql"
	"encoding/json"
	"fmt"
	_ "github.com/mailru/go-clickhouse"
	"log"
)

const url = "http://clickhouse:8123"

const createTableQuery = `CREATE TABLE IF NOT EXISTS logs
	(
		timestamp DateTime,
		severity String,
		message String
	)
	ENGINE = Log`

type (
	TableManager struct {
	}
	LogManager struct {
		db *sql.DB
	}
	Log struct {
		Timestamp string `json:"timestamp,omitempty"`
		Severity  string `json:"severity,omitempty"`
		Message   string `json:"message,omitempty"`
	}
)

func (lm *LogManager) Insert(rawLog string) {
	if lm.db == nil {
		connection, err := sql.Open("clickhouse", url)
		if err != nil {
			fmt.Printf("Open >> %v", err)
		}
		lm.db = connection
	}

	var logs = Log{}
	err := json.Unmarshal([]byte(rawLog), &logs)
	if err != nil {
		log.Fatal(err)
	}
	_, err = lm.db.Exec("INSERT INTO logs (timestamp, severity, message) VALUES(toDateTime(?),?,?)", logs.Timestamp, logs.Severity, logs.Message)
	if err != nil {
		log.Fatal(err)
	}
}

func (m *TableManager) Migrate() {
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

func NewTableManager() *TableManager {
	return &TableManager{}
}

func NewLogManager() *LogManager {
	db, err := sql.Open("clickhouse", url)
	if err != nil {
		log.Fatal(err)
	}
	return &LogManager{db: db}
}
