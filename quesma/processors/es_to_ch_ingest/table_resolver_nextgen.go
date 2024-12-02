// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package es_to_ch_ingest

import "quesma/table_resolver"

type NextGenTableResolver struct{}

func (n *NextGenTableResolver) Start() {}
func (n *NextGenTableResolver) Stop()  {}
func (n *NextGenTableResolver) Resolve(_ string, _ string) *table_resolver.Decision {
	decision := &table_resolver.Decision{
		UseConnectors: []table_resolver.ConnectorDecision{&table_resolver.ConnectorDecisionClickhouse{
			ClickhouseTableName: HARDCODED_INDEX_NAME,
		}}}
	return decision
}
func (n *NextGenTableResolver) Pipelines() []string {
	return []string{}
}
func (n *NextGenTableResolver) RecentDecisions() []table_resolver.PatternDecisions {
	return []table_resolver.PatternDecisions{}
}
