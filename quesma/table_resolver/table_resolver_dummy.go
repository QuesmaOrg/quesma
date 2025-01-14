// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package table_resolver

import (
	mux "github.com/QuesmaOrg/quesma/v2/core"
)

// DummyTableResolver is a dummy implementation of TableResolver to satisfy the QueryRunner and make it be compatible with the v2 api
// thanks to this we can reuse the existing QueryRunner implementation without any changes.
type DummyTableResolver struct{}

func NewDummyTableResolver() *DummyTableResolver {
	return &DummyTableResolver{}
}

func (t DummyTableResolver) Start() {}

func (t DummyTableResolver) Stop() {}

func (t DummyTableResolver) Resolve(_ string, indexPattern string) *mux.Decision {
	return &mux.Decision{
		UseConnectors: []mux.ConnectorDecision{
			&mux.ConnectorDecisionClickhouse{
				ClickhouseTableName: indexPattern,
				ClickhouseIndexes:   []string{indexPattern}, // TODO this won't work for 'common table' feature
				//IsCommonTable: false,
			},
		},
	}

}

func (t DummyTableResolver) Pipelines() []string { return []string{} }

func (t DummyTableResolver) RecentDecisions() []mux.PatternDecisions {
	return []mux.PatternDecisions{}
}
