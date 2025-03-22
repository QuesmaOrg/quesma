// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package table_resolver

import (
	"github.com/QuesmaOrg/quesma/platform/common_table"
	"github.com/QuesmaOrg/quesma/platform/config"
	"github.com/QuesmaOrg/quesma/platform/elasticsearch"
	mux "github.com/QuesmaOrg/quesma/platform/v2/core"
)

// DummyTableResolver is a dummy implementation of TableResolver to satisfy the QueryRunner and make it be compatible with the v2 api
// thanks to this we can reuse the existing QueryRunner implementation without any changes.
type DummyTableResolver struct {
	cfg                 config.IndicesConfigs
	wildcardCommonTable bool
}

func NewDummyTableResolver(cfg config.IndicesConfigs, wildcardCommonTable bool) *DummyTableResolver {
	return &DummyTableResolver{cfg: cfg, wildcardCommonTable: wildcardCommonTable}
}

func (t DummyTableResolver) Start() {}

func (t DummyTableResolver) Stop() {}

func (t DummyTableResolver) Resolve(_ string, indexPattern string) *mux.Decision {
	if elasticsearch.IsInternalIndex(indexPattern) { // e.g. `.kibana_analytics_8.11.1`
		return t.resolveElastic()
	}
	if t.wildcardCommonTable {
		return t.resolveCommonTable(indexPattern)
	}

	if indexCfg, ok := t.cfg[indexPattern]; !ok {
		return t.resolveElastic()
	} else {
		if indexCfg.UseCommonTable {
			return t.resolveCommonTable(indexPattern)
		} else {
			return t.resolveClickhouse(indexPattern)
		}
	}
}

func (t DummyTableResolver) Pipelines() []string { return []string{} }

func (t DummyTableResolver) RecentDecisions() []mux.PatternDecisions {
	return []mux.PatternDecisions{}
}

func (t DummyTableResolver) resolveElastic() *mux.Decision {
	return &mux.Decision{
		UseConnectors: []mux.ConnectorDecision{
			&mux.ConnectorDecisionElastic{},
		},
	}
}

func (t DummyTableResolver) resolveCommonTable(indexPattern string) *mux.Decision {
	return &mux.Decision{
		UseConnectors: []mux.ConnectorDecision{
			&mux.ConnectorDecisionClickhouse{
				ClickhouseTableName: common_table.TableName,
				ClickhouseIndexes:   []string{indexPattern},
				IsCommonTable:       true,
			},
		},
	}
}

func (t DummyTableResolver) resolveClickhouse(indexPattern string) *mux.Decision {
	return &mux.Decision{
		UseConnectors: []mux.ConnectorDecision{
			&mux.ConnectorDecisionClickhouse{
				ClickhouseTableName: indexPattern,
				ClickhouseIndexes:   []string{indexPattern},
			},
		},
	}
}
