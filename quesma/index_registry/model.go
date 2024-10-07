// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package index_registry

import (
	"quesma/logger"
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

	// not so obvious fields

	// maybe it should be a list of sub-decisions for each connection

	PassToElastic bool

	PassToClickhouse    bool
	ClickhouseTableName string
	Indexes             []string
	IsCommonTable       bool

	// who made the decision and why
	Message      string
	ResolverName string
}

type PatternDecision struct {
	Pattern   string
	Decisions map[string]*Decision
}

type IndexRegistry interface {
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
