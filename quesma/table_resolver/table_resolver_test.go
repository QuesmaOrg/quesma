// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package table_resolver

import (
	"fmt"
	"github.com/QuesmaOrg/quesma/quesma/clickhouse"
	"github.com/QuesmaOrg/quesma/quesma/common_table"
	"github.com/QuesmaOrg/quesma/quesma/config"
	"github.com/QuesmaOrg/quesma/quesma/elasticsearch"
	mux "github.com/QuesmaOrg/quesma/quesma/v2/core"
	"github.com/k0kubun/pp"
	"github.com/stretchr/testify/assert"
	"reflect"
	"strings"
	"testing"
)

func TestTableResolver(t *testing.T) {

	indexConf := map[string]config.IndexConfiguration{
		"index1": {
			QueryTarget:  []string{"clickhouse"},
			IngestTarget: []string{"clickhouse"},
		},
		"index2": {
			UseCommonTable: true,
			QueryTarget:    []string{"clickhouse"},
			IngestTarget:   []string{"clickhouse"},
		},
		"index3": {
			QueryTarget:  []string{"elasticsearch"},
			IngestTarget: []string{"elasticsearch"},
		},
		"logs": {
			QueryTarget:  []string{"clickhouse", "elasticsearch"},
			IngestTarget: []string{"clickhouse", "elasticsearch"},
		},
		"some-elastic-logs": {
			QueryTarget:  []string{"elasticsearch"},
			IngestTarget: []string{"elasticsearch"},
		},
		"closed": {
			QueryTarget:  []string{},
			IngestTarget: []string{},
		},
		"closed-common-table": {
			UseCommonTable: true,
			QueryTarget:    []string{},
			IngestTarget:   []string{},
		},
		"unknown-target": {
			QueryTarget:  []string{"unknown"},
			IngestTarget: []string{"unknown"},
		},
	}

	cfg := config.QuesmaConfiguration{IndexConfig: indexConf, DefaultQueryTarget: []string{config.ElasticsearchTarget}, DefaultIngestTarget: []string{config.ElasticsearchTarget}}

	cfgClickhouseOnlyUseCommonTable := config.QuesmaConfiguration{IndexConfig: indexConf, DefaultQueryTarget: []string{config.ClickhouseTarget}, DefaultIngestTarget: []string{config.ClickhouseTarget}, UseCommonTableForWildcard: true}

	tests := []struct {
		name              string
		pipeline          string
		pattern           string
		elasticIndexes    []string
		clickhouseIndexes []string
		virtualTables     []string
		indexConf         map[string]config.IndexConfiguration
		expected          mux.Decision
		quesmaConf        *config.QuesmaConfiguration
	}{
		{
			name:     "elastic fallback",
			pipeline: mux.IngestPipeline,
			pattern:  "some-index",
			expected: mux.Decision{
				UseConnectors: []mux.ConnectorDecision{&mux.ConnectorDecisionElastic{}},
			},
			indexConf: make(map[string]config.IndexConfiguration),
		},
		{
			name:     "all",
			pipeline: mux.QueryPipeline,
			pattern:  "*",
			expected: mux.Decision{
				Err: fmt.Errorf("inconsistent A/B testing configuration"),
			},
			indexConf: indexConf,
		},
		{
			name:              "empty *",
			pipeline:          mux.QueryPipeline,
			pattern:           "*",
			clickhouseIndexes: []string{"index1", "index2"},
			expected: mux.Decision{
				Err: fmt.Errorf(""),
			},
			indexConf: indexConf,
		},
		{
			name:              "query all, indices in both connectors",
			pipeline:          mux.QueryPipeline,
			pattern:           "*",
			clickhouseIndexes: []string{"index1", "index2"},
			elasticIndexes:    []string{"index3"},
			expected: mux.Decision{
				Err: fmt.Errorf(""),
			},
			indexConf: indexConf,
		},
		{
			name:              "ingest with a pattern",
			pipeline:          mux.IngestPipeline,
			pattern:           "*",
			clickhouseIndexes: []string{"index1", "index2"},
			elasticIndexes:    []string{"index3"},
			expected: mux.Decision{
				Err: fmt.Errorf("pattern is not allowed"),
			},
			indexConf: indexConf,
		},
		{
			name:              "query closed index",
			pipeline:          mux.QueryPipeline,
			pattern:           "closed",
			clickhouseIndexes: []string{"closed"},
			expected: mux.Decision{
				IsClosed: true,
			},
			indexConf: indexConf,
		},
		{
			name:              "ingest closed index",
			pipeline:          mux.QueryPipeline,
			pattern:           "closed",
			clickhouseIndexes: []string{"closed"},
			expected: mux.Decision{
				IsClosed: true,
			},
			indexConf: indexConf,
		},
		{
			name:              "ingest closed index",
			pipeline:          mux.QueryPipeline,
			pattern:           "closed-common-table",
			clickhouseIndexes: []string{"closed"},
			expected: mux.Decision{
				IsClosed: true,
			},
			indexConf: indexConf,
		},
		{
			name:              "ingest closed index",
			pipeline:          mux.QueryPipeline,
			pattern:           "unknown-target",
			clickhouseIndexes: []string{"closed"},
			expected: mux.Decision{
				Err: fmt.Errorf("unsupported target"),
			},
			indexConf: indexConf,
		},
		{
			name:              "ingest to index1",
			pipeline:          mux.IngestPipeline,
			pattern:           "index1",
			clickhouseIndexes: []string{"index1"},
			expected: mux.Decision{
				UseConnectors: []mux.ConnectorDecision{&mux.ConnectorDecisionClickhouse{
					ClickhouseTableName: "index1",
					ClickhouseIndexes:   []string{"index1"}},
				},
			},
			indexConf: indexConf,
		},
		{
			name:              "query from index1",
			pipeline:          mux.QueryPipeline,
			pattern:           "index1",
			clickhouseIndexes: []string{"index1"},
			expected: mux.Decision{
				UseConnectors: []mux.ConnectorDecision{&mux.ConnectorDecisionClickhouse{
					ClickhouseTableName: "index1",
					ClickhouseIndexes:   []string{"index1"}},
				},
			},
			indexConf: indexConf,
		},
		{
			name:              "ingest to index2",
			pipeline:          mux.IngestPipeline,
			pattern:           "index2",
			clickhouseIndexes: []string{"index2"},
			expected: mux.Decision{
				UseConnectors: []mux.ConnectorDecision{&mux.ConnectorDecisionClickhouse{
					ClickhouseTableName: common_table.TableName,
					ClickhouseIndexes:   []string{"index2"},
					IsCommonTable:       true,
				}},
			},
			indexConf: indexConf,
		},
		{
			name:              "query from index2",
			pipeline:          mux.QueryPipeline,
			pattern:           "index2",
			clickhouseIndexes: []string{"index2"},
			expected: mux.Decision{
				UseConnectors: []mux.ConnectorDecision{&mux.ConnectorDecisionClickhouse{
					ClickhouseTableName: common_table.TableName,
					ClickhouseIndexes:   []string{"index2"},
					IsCommonTable:       true,
				}},
			},
			indexConf: indexConf,
		},
		{
			name:           "query from index1,index2",
			pipeline:       mux.QueryPipeline,
			pattern:        "index1,index2",
			elasticIndexes: []string{"index3"},
			expected: mux.Decision{
				Err: fmt.Errorf(""),
			},
			indexConf: indexConf,
		},
		{
			name:           "query from index1,index-not-existing",
			pipeline:       mux.QueryPipeline,
			pattern:        "index1,index-not-existing",
			elasticIndexes: []string{"index1,index-not-existing"},
			expected: mux.Decision{
				Err: fmt.Errorf(""), // index1 in Clickhouse, index-not-existing in Elastic ('*')
			},
			indexConf: indexConf,
		},
		{
			name:           "ingest to index3",
			pipeline:       mux.IngestPipeline,
			pattern:        "index3",
			elasticIndexes: []string{"index3"},
			expected: mux.Decision{
				UseConnectors: []mux.ConnectorDecision{&mux.ConnectorDecisionElastic{}},
			},
			indexConf: indexConf,
		},
		{
			name:           "query from index3",
			pipeline:       mux.QueryPipeline,
			pattern:        "index3",
			elasticIndexes: []string{"index3"},
			expected: mux.Decision{
				UseConnectors: []mux.ConnectorDecision{&mux.ConnectorDecisionElastic{}},
			},
			indexConf: indexConf,
		},

		{
			name:          "query pattern",
			pipeline:      mux.QueryPipeline,
			pattern:       "index2,foo*",
			virtualTables: []string{"index2"},
			expected: mux.Decision{
				UseConnectors: []mux.ConnectorDecision{&mux.ConnectorDecisionClickhouse{
					ClickhouseTableName: common_table.TableName,
					ClickhouseIndexes:   []string{"index2"},
					IsCommonTable:       true,
				}},
			},
			indexConf: indexConf,
		},
		{
			name:          "query pattern (not existing virtual table)",
			pipeline:      mux.QueryPipeline,
			pattern:       "common-index1,common-index2",
			virtualTables: []string{"common-index1"},
			expected: mux.Decision{
				UseConnectors: []mux.ConnectorDecision{&mux.ConnectorDecisionClickhouse{
					ClickhouseTableName: common_table.TableName,
					ClickhouseIndexes:   []string{"common-index1", "common-index2"},
					IsCommonTable:       true,
				}},
			},
			indexConf:  indexConf,
			quesmaConf: &cfgClickhouseOnlyUseCommonTable,
		},
		{
			name:     "query kibana internals",
			pipeline: mux.QueryPipeline,
			pattern:  ".kibana",
			expected: mux.Decision{
				UseConnectors: []mux.ConnectorDecision{&mux.ConnectorDecisionElastic{ManagementCall: true}},
			},
			indexConf: indexConf,
		},
		{
			name:     "ingest kibana internals",
			pipeline: mux.IngestPipeline,
			pattern:  ".kibana",
			expected: mux.Decision{
				UseConnectors: []mux.ConnectorDecision{&mux.ConnectorDecisionElastic{ManagementCall: true}},
			},
			indexConf: indexConf,
		},
		{
			name:     "ingest not configured index",
			pipeline: mux.IngestPipeline,
			pattern:  "not-configured",
			expected: mux.Decision{
				UseConnectors: []mux.ConnectorDecision{&mux.ConnectorDecisionElastic{}},
			},
			indexConf: indexConf,
		},
		{
			name:     "double write",
			pipeline: mux.IngestPipeline,
			pattern:  "logs",
			expected: mux.Decision{
				UseConnectors: []mux.ConnectorDecision{&mux.ConnectorDecisionClickhouse{
					ClickhouseTableName: "logs",
					ClickhouseIndexes:   []string{"logs"},
				},
					&mux.ConnectorDecisionElastic{}},
			},
			indexConf: indexConf,
		},
		{
			name:     "A/B testing",
			pipeline: mux.QueryPipeline,
			pattern:  "logs",
			expected: mux.Decision{
				EnableABTesting: true,
				UseConnectors: []mux.ConnectorDecision{&mux.ConnectorDecisionClickhouse{
					ClickhouseTableName: "logs",
					ClickhouseIndexes:   []string{"logs"},
				},
					&mux.ConnectorDecisionElastic{}},
			},
			indexConf: indexConf,
		},
		{
			name:     "A/B testing (pattern)",
			pipeline: mux.QueryPipeline,
			pattern:  "logs*",
			expected: mux.Decision{
				EnableABTesting: true,
				UseConnectors: []mux.ConnectorDecision{&mux.ConnectorDecisionClickhouse{
					ClickhouseTableName: "logs",
					ClickhouseIndexes:   []string{"logs"},
				},
					&mux.ConnectorDecisionElastic{}},
			},
			indexConf: indexConf,
		},
		{
			name:              "query both connectors",
			pipeline:          mux.QueryPipeline,
			pattern:           "logs,index1",
			indexConf:         indexConf,
			clickhouseIndexes: []string{"index1"},
			elasticIndexes:    []string{"logs"},
			expected: mux.Decision{
				Err: fmt.Errorf(""),
			},
		},
		{
			name:           "query elastic with pattern",
			pipeline:       mux.QueryPipeline,
			pattern:        "some-elastic-logs*",
			elasticIndexes: []string{"logs"},
			expected: mux.Decision{
				UseConnectors: []mux.ConnectorDecision{&mux.ConnectorDecisionElastic{
					ManagementCall: false,
				}},
			},
		},
		{
			name:           "non matching pattern",
			pipeline:       mux.QueryPipeline,
			pattern:        "some-non-matching-pattern*",
			elasticIndexes: []string{"logs"},
			expected: mux.Decision{
				IsEmpty: true,
			},
		},
		{
			name:     "query internal index",
			pipeline: mux.QueryPipeline,
			pattern:  "quesma_common_table",
			expected: mux.Decision{
				Err: fmt.Errorf("common table"),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			currentQuesmaConf := cfg
			if tt.quesmaConf != nil {
				currentQuesmaConf = *tt.quesmaConf
			}

			tableDiscovery := clickhouse.NewEmptyTableDiscovery()

			for _, index := range tt.clickhouseIndexes {
				tableDiscovery.TableMap.Store(index, &clickhouse.Table{
					Name: index,
				})
			}

			for _, index := range tt.virtualTables {
				tableDiscovery.TableMap.Store(index, &clickhouse.Table{
					Name:         index,
					VirtualTable: true,
				})
			}

			elasticResolver := elasticsearch.NewFixedIndexManagement(tt.elasticIndexes...)

			resolver := NewTableResolver(currentQuesmaConf, tableDiscovery, elasticResolver)

			decision := resolver.Resolve(tt.pipeline, tt.pattern)

			assert.NotNil(t, decision)
			if tt.expected.Err != nil {

				assert.NotNil(t, decision.Err, "Expected error, but got nil")

				if !strings.Contains(decision.Err.Error(), tt.expected.Err.Error()) {
					t.Errorf("Error is not an instance of the expected error: got %v, expected %v", decision.Err, tt.expected.Err)
				}
			} else {
				assert.Nil(t, decision.Err)
			}
			assert.Equal(t, tt.expected.IsClosed, decision.IsClosed, "expected %v, got %v", tt.expected.IsClosed, decision.IsClosed)
			assert.Equal(t, tt.expected.IsEmpty, decision.IsEmpty, "expected %v, got %v", tt.expected.IsEmpty, decision.IsEmpty)
			assert.Equal(t, tt.expected.EnableABTesting, decision.EnableABTesting, "expected %v, got %v", tt.expected.EnableABTesting, decision.EnableABTesting)

			if !reflect.DeepEqual(tt.expected.UseConnectors, decision.UseConnectors) {
				pp.Println(tt.expected)
				pp.Println(decision)
				t.Errorf("UseConnectors didn't match, expected %v, got %v", tt.expected.UseConnectors, decision.UseConnectors)
			}

		})
	}

}
