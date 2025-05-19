// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package main

import (
	"crypto/tls"
	"database/sql"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	lexer_core "github.com/QuesmaOrg/quesma/platform/parsers/sql/lexer/core"
	"github.com/QuesmaOrg/quesma/platform/parsers/sql/lexer/dialect_sqlparse"
	"github.com/QuesmaOrg/quesma/platform/parsers/sql/parser/core"
	"github.com/QuesmaOrg/quesma/platform/parsers/sql/parser/pipe_syntax"
	"github.com/QuesmaOrg/quesma/platform/parsers/sql/parser/transforms"
	"log"
	"os"
)

var DefaultDB *sql.DB

func connectClickhouse() *sql.DB {
	log.Println("Connecting to Clickhouse")

	host := os.Getenv("CLICKHOUSE_HOST")
	if host == "" {
		log.Println("Warning: CLICKHOUSE_HOST is not set. Defaulting to host.docker.internal:9000")
		host = "host.docker.internal:9000"
	}

	options := clickhouse.Options{
		Addr: []string{host},
	}

	// Retrieve authentication details from environment variables.
	user := os.Getenv("CLICKHOUSE_USER")
	password := os.Getenv("CLICKHOUSE_PASSWORD")
	database := os.Getenv("CLICKHOUSE_DATABASE")

	if user == "" && password == "" && database == "" {
		log.Println("Warning: No ClickHouse authentication details provided; proceeding without authentication.")
	} else {
		if user == "" {
			log.Println("Warning: CLICKHOUSE_USER is not set.")
		}
		if password == "" {
			log.Println("Warning: CLICKHOUSE_PASSWORD is not set.")
		}
		if database == "" {
			log.Println("Warning: CLICKHOUSE_DATABASE is not set.")
		}
		options.Auth = clickhouse.Auth{
			Username: user,
			Password: password,
			Database: database,
		}
	}

	useTLS := os.Getenv("CLICKHOUSE_USE_TLS")
	if useTLS == "" {
		log.Println("Warning: CLICKHOUSE_USE_TLS is not set; defaulting to TLS disabled.")
	}
	if useTLS == "true" {
		options.TLS = &tls.Config{InsecureSkipVerify: true}
	}

	// FIXME: the transpiler should automatically generate queries with aliased subqueries
	options.Settings = clickhouse.Settings{
		"joined_subquery_requires_alias": "0",
	}

	db := clickhouse.OpenDB(&options)
	DefaultDB = db

	if err := DefaultDB.Ping(); err != nil {
		log.Println(err)
	}

	return db
}

func main() {
	connectClickhouse()

	query := `FROM openssh_logs
	|> WHERE timestamp BETWEEN $start AND $end
	|> ORDER BY timestamp DESC
	|> EXTEND ENRICH_LOG_CATEGORY(msg) AS category
	|> WHERE category <> 'Others'
	|> AGGREGATE COUNT(*) AS category_cnt GROUP BY TIME_BUCKET(timestamp), category
	|> ORDER BY TIME_BUCKET(timestamp) ASC, category_cnt DESC
`

	query = `FROM apache_logs
|> WHERE timestamp BETWEEN '2000-01-01' AND '2100-01-01'
|> ORDER BY timestamp DESC
|> SELECT timestamp, severity, msg, client
|> WHERE client IS NOT NULL 
|> AGGREGATE count(*) as client_count, any(msg) as sample_msg group by client
|> ORDER BY client_count DESC
|> LIMIT 10
`

	tokens := lexer_core.Lex(query, dialect_sqlparse.SqlparseRules)

	node := core.TokensToNode(tokens)

	transforms.GroupParenthesis(node)
	pipe_syntax.GroupPipeSyntax(node)
	pipe_syntax.ExpandMacros(node)
	pipe_syntax.ExpandEnrichments(node, DefaultDB)
	pipe_syntax.TranspileToCTE(node)

	fmt.Println(transforms.ConcatTokenNodes(node))
}
