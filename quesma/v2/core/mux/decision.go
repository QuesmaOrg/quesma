// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package mux

import (
	"fmt"
	"strings"
)

type Decision struct {
	// input
	IndexPattern string "json:\"index_pattern\""

	// obvious fields
	IsClosed bool  "json:\"is_closed\""
	Err      error "json:\"error\""
	IsEmpty  bool  "json:\"is_empty\""

	EnableABTesting bool "json:\"enable_ab_testing\""

	// which connector to use, and how
	UseConnectors []ConnectorDecision "json:\"use_connectors\""

	// who made the decision and why
	Reason       string "json:\"reason\""
	ResolverName string "json:\"resolver_name\""
}

func (d *Decision) String() string {

	var lines []string

	if d.IsClosed {
		lines = append(lines, "Returns a closed index message.")
	}

	if d.IsEmpty {
		lines = append(lines, "Returns an empty result.")
	}

	if d.Err != nil {
		lines = append(lines, fmt.Sprintf("Returns error: '%v'.", d.Err))
	}

	for _, connector := range d.UseConnectors {
		lines = append(lines, connector.Message())
	}

	if d.EnableABTesting {
		lines = append(lines, "Enable AB testing.")
	}

	lines = append(lines, fmt.Sprintf("%s (%s).", d.Reason, d.ResolverName))

	return strings.Join(lines, " ")
}

type ConnectorDecision interface {
	Message() string
}

type ConnectorDecisionElastic struct {
	// TODO  instance of elastic connector
	ManagementCall bool "json:\"management_call\""
}

func (d *ConnectorDecisionElastic) Message() string {
	var lines []string
	lines = append(lines, "Pass to Elasticsearch.")
	if d.ManagementCall {
		lines = append(lines, "Management call.")
	}
	return strings.Join(lines, " ")
}

type ConnectorDecisionClickhouse struct {
	// TODO  instance of clickhouse connector

	ClickhouseTableName string   "json:\"clickhouse_table_name\""
	ClickhouseTables    []string "json:\"clickhouse_tables\""
	IsCommonTable       bool     "json:\"is_common_table\""
}

func (d *ConnectorDecisionClickhouse) Message() string {
	var lines []string

	lines = append(lines, "Pass to clickhouse.")
	if len(d.ClickhouseTableName) > 0 {
		lines = append(lines, fmt.Sprintf("Table: '%s' .", d.ClickhouseTableName))
	}
	if d.IsCommonTable {
		lines = append(lines, "Common table.")
	}
	if len(d.ClickhouseTables) > 0 {
		lines = append(lines, fmt.Sprintf("Indexes: %v.", d.ClickhouseTables))
	}

	return strings.Join(lines, " ")
}

// PatternDecisions is a struct that holds the pattern and the decisions made for that pattern
type PatternDecisions struct {
	Pattern   string
	Decisions map[string]*Decision
}

// TODO hardcoded pipeline names
const (
	QueryPipeline  = "Query"
	IngestPipeline = "Ingest"
)
