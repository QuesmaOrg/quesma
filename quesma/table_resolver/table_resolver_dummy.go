// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package table_resolver

import (
	"github.com/QuesmaOrg/quesma/quesma/quesma/config"
	mux "github.com/QuesmaOrg/quesma/quesma/v2/core"
)

// DummyTableResolver is a dummy implementation of TableResolver to satisfy the QueryRunner and make it be compatible with the v2 api
// thanks to this we can reuse the existing QueryRunner implementation without any changes.
type DummyTableResolver struct {
	cfg config.IndicesConfigs
}

func NewDummyTableResolver(cfg config.IndicesConfigs) *DummyTableResolver {
	return &DummyTableResolver{cfg: cfg}
}

func (t DummyTableResolver) Start() {}

func (t DummyTableResolver) Stop() {}

func (t DummyTableResolver) Resolve(_ string, indexPattern string) *mux.Decision {
	_, ok := t.cfg[indexPattern] // TODO: if index doens't exist in config - route to Elasticsearch (just for now)
	if !ok {
		return &mux.Decision{
			UseConnectors: []mux.ConnectorDecision{
				&mux.ConnectorDecisionElastic{},
			},
		}
	} else {
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
}

func (t DummyTableResolver) Pipelines() []string { return []string{} }

func (t DummyTableResolver) RecentDecisions() []mux.PatternDecisions {
	return []mux.PatternDecisions{}
}
