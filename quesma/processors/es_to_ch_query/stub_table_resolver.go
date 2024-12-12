// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package es_to_ch_query

import (
	"quesma/table_resolver"
	quesma_api "quesma_v2/core"
)

func NewNextGenTableResolver() table_resolver.TableResolver {
	return &NextGenTableResolver{}
}

// NextGenTableResolver is a stub implementation of the TableResolver interface, temporarily used to satisfy
// intermediate implementation of the Query Processor
type NextGenTableResolver struct{}

func (n *NextGenTableResolver) Start() {}
func (n *NextGenTableResolver) Stop()  {}
func (n *NextGenTableResolver) Resolve(_ string, tableName string) *quesma_api.Decision {
	decision := &quesma_api.Decision{
		UseConnectors: []quesma_api.ConnectorDecision{&quesma_api.ConnectorDecisionClickhouse{
			ClickhouseTableName: tableName,
			ClickhouseIndexes:   []string{tableName},
			IsCommonTable:       false,
		}}}
	return decision
}
func (n *NextGenTableResolver) Pipelines() []string {
	return []string{}
}
func (n *NextGenTableResolver) RecentDecisions() []quesma_api.PatternDecisions {
	return []quesma_api.PatternDecisions{}
}
