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

	tokens := lexer_core.Lex(
		`FROM openssh_logs
-- OpenSSH logs (FIXME: this comment can't be in line above)
|> ORDER BY timestamp DESC
|> WHERE timestamp BETWEEN $start AND $end

-- Filter out sshd logs, break-in attempts logs
|> WHERE source = 'sshd'
|> WHERE msg ILIKE '%break-in attempt!%'

-- Parse IP, hostname from loglines
|> WHERE msg ILIKE 'reverse mapping checking getaddrinfo for % [%] failed - POSSIBLE BREAK-IN ATTEMPT!'
|> EXTEND extractGroups(msg, 'reverse mapping checking getaddrinfo for (\S+) \[(\S+)\] failed - POSSIBLE BREAK-IN ATTEMPT!') AS extracted_tmp
|> EXTEND extracted_tmp[1] AS extracted_host, extracted_tmp[2] AS extracted_ip

|> SELECT timestamp, msg, extracted_host, extracted_ip

-- IP enrichment (FIXME: this should be a |> CALL ENRICH_IP(extracted_ip) or |> EXTEND ENRICH_IP(extracted_ip) )
|> EXTEND ENRICH_IP(extracted_ip) AS enriched_ip
--|> LEFT JOIN ip_data ON ip_data.ip = extracted_ip

-- IP enrichment - bot IPs (FIXME: this should be a |> CALL ENRICH_IP_BOTS(extracted_ip) or |> EXTEND ENRICH_IP_BOTS(extracted_ip))
-- The production version of this would either have more rules or use a commercial DB of bot/spam/scam IPs
--|> EXTEND coalesce(hostname ILIKE '%amazonaws%' OR hostname ILIKE '%server%' OR hostname ILIKE '%cloud%', false) AS is_bot
--|> WHERE is_bot

-- Aggregate by country
--|> AGGREGATE count(*) AS country_count GROUP BY country_long
--|> ORDER BY country_count DESC

|> LIMIT 100`, dialect_sqlparse.SqlparseRules)

	node := core.TokensToNode(tokens)

	transforms.GroupParenthesis(node)
	pipe_syntax.GroupPipeSyntax(node)
	pipe_syntax.ExpandMacros(node)
	pipe_syntax.ExpandEnrichments(node, DefaultDB)
	pipe_syntax.Transpile(node)

	fmt.Println(transforms.ConcatTokenNodes(node))
}
