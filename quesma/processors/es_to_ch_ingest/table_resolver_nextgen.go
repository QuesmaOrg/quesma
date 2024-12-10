// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package es_to_ch_ingest

import (
	"quesma/table_resolver"
	quesma_api "quesma_v2/core"
)

func NewNextGenTableResolver() table_resolver.TableResolver {
	return &NextGenTableResolver{}
}

type NextGenTableResolver struct{}

func (n *NextGenTableResolver) Start() {}
func (n *NextGenTableResolver) Stop()  {}
func (n *NextGenTableResolver) Resolve(_ string, tableName string) *quesma_api.Decision {
	println("RESOLVE CALLED")
	decision := &quesma_api.Decision{
		UseConnectors: []quesma_api.ConnectorDecision{&quesma_api.ConnectorDecisionClickhouse{
			ClickhouseTableName: tableName,
			IsCommonTable:       tableName == "tab1" || tableName == "tab2",
		}}}
	return decision
}
func (n *NextGenTableResolver) Pipelines() []string {
	return []string{}
}
func (n *NextGenTableResolver) RecentDecisions() []quesma_api.PatternDecisions {
	return []quesma_api.PatternDecisions{}
}
