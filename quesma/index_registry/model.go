// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package index_registry

import (
	"fmt"
	"quesma/logger"
	"strings"
)

// API for the customers (router, ingest processor and query processor)
type IndexResolver interface {
	Resolve(indexPattern string) *Decision
}

type Decision struct {

	// obvious fields
	IsClosed bool
	Err      error
	IsEmpty  bool

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

	lines = append(lines, fmt.Sprintf("%s (%s).", d.Message, d.ResolverName))

	return strings.Join(lines, " ")
}

type ConnectorDecision interface {
	Message() string
}

type ConnectorDecisionElastic struct {
	// TODO  instance of elastic connector
}

func (*ConnectorDecisionElastic) Message() string {
	return "Pass to Elasticsearch."
}

type ConnectorDecisionClickhouse struct {
	// TODO  instance of clickhouse connector

	ClickhouseTableName string
	Indexes             []string
	IsCommonTable       bool
}

func (d *ConnectorDecisionClickhouse) Message() string {
	lines := []string{}

	lines = append(lines, "Pass to clickhouse.")
	if len(d.ClickhouseTableName) > 0 {
		lines = append(lines, fmt.Sprintf("Table: '%s' .", d.ClickhouseTableName))
	}
	if d.IsCommonTable {
		lines = append(lines, "Common table.")
	}
	if len(d.Indexes) > 0 {
		lines = append(lines, fmt.Sprintf("Indexes: %v.", d.Indexes))
	}

	return strings.Join(lines, " ")
}

type PatternDecision struct {
	Pattern   string
	Decisions map[string]*Decision
}

type IndexRegistry interface {
	Start()
	Stop()

	Resolve(pipeline string, indexPattern string) *Decision


	Pipelines() []string
	RecentDecisions() []PatternDecision
}

func TODO(args ...any) {
	logger.Info().Msgf("TODO: use index_registry decision here  %v", args)
}

// TODO hardcoded pipeline names
const (
	QueryPipeline  = "Query"
	IngestPipeline = "Ingest"
)
