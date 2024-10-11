// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package table_resolver

import (
	"fmt"
	"quesma/logger"
	"strings"
)

type Decision struct {
	// input
	IndexPattern string

	// obvious fields
	IsClosed bool
	Err      error
	IsEmpty  bool

	EnableABTesting bool

	// which connector to use, and how
	UseConnectors []ConnectorDecision

	// who made the decision and why
	Message      string
	ResolverName string
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

	lines = append(lines, fmt.Sprintf("%s (%s).", d.Message, d.ResolverName))

	return strings.Join(lines, " ")
}

type ConnectorDecision interface {
	Message() string
}

type ConnectorDecisionElastic struct {
	// TODO  instance of elastic connector
	ManagementCall bool
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

	ClickhouseTableName string
	ClickhouseTables    []string
	IsCommonTable       bool
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

type TableResolver interface {
	Start()
	Stop()

	Resolve(pipeline string, indexPattern string) *Decision

	Pipelines() []string
	RecentDecisions() []PatternDecisions
}

// TODO will be removed in the next PR,
// right now it is used to mark places where we must refactor the code
func TODO(place string, decision *Decision) {
	var trace bool
	if trace {
		logger.Debug().Msgf("TODO: use table_resolver decision here  %s : %v", place, decision.String())
	}
}

// TODO hardcoded pipeline names
const (
	QueryPipeline  = "Query"
	IngestPipeline = "Ingest"
)
