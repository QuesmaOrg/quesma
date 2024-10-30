// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package table_resolver

import (
	"fmt"
	"github.com/k0kubun/pp"
	"github.com/stretchr/testify/assert"
	"quesma/clickhouse"
	"quesma/common_table"
	"quesma/elasticsearch"
	"quesma/end_user_errors"
	"quesma/quesma/config"
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

	tests := []struct {
		name              string
		pipeline          string
		pattern           string
		elasticIndexes    []string
		clickhouseIndexes []string
		virtualTables     []string
		indexConf         map[string]config.IndexConfiguration
		expected          Decision
	}{
		{
			name:     "elastic fallback",
			pipeline: IngestPipeline,
			pattern:  "some-index",
			expected: Decision{
				UseConnectors: []ConnectorDecision{&ConnectorDecisionElastic{}},
			},
			indexConf: make(map[string]config.IndexConfiguration),
		},
		{
			name:     "all",
			pipeline: QueryPipeline,
			pattern:  "*",
			expected: Decision{
				Err: end_user_errors.ErrSearchCondition.New(fmt.Errorf("")),
			},
			indexConf: indexConf,
		},
		{
			name:              "empty *",
			pipeline:          QueryPipeline,
			pattern:           "*",
			clickhouseIndexes: []string{"index1", "index2"},
			expected: Decision{
				Err: end_user_errors.ErrSearchCondition.New(fmt.Errorf("")),
			},
			indexConf: indexConf,
		},
		{
			name:              "query all, indices in both connectors",
			pipeline:          QueryPipeline,
			pattern:           "*",
			clickhouseIndexes: []string{"index1", "index2"},
			elasticIndexes:    []string{"index3"},
			expected: Decision{
				Err: end_user_errors.ErrSearchCondition.New(fmt.Errorf("")),
			},
			indexConf: indexConf,
		},
		{
			name:              "ingest with a pattern",
			pipeline:          IngestPipeline,
			pattern:           "*",
			clickhouseIndexes: []string{"index1", "index2"},
			elasticIndexes:    []string{"index3"},
			expected: Decision{
				Err: fmt.Errorf("pattern is not allowed"),
			},
			indexConf: indexConf,
		},
		{
			name:              "query closed index",
			pipeline:          QueryPipeline,
			pattern:           "closed",
			clickhouseIndexes: []string{"closed"},
			expected: Decision{
				IsClosed: true,
			},
			indexConf: indexConf,
		},
		{
			name:              "ingest closed index",
			pipeline:          QueryPipeline,
			pattern:           "closed",
			clickhouseIndexes: []string{"closed"},
			expected: Decision{
				IsClosed: true,
			},
			indexConf: indexConf,
		},
		{
			name:              "ingest closed index",
			pipeline:          QueryPipeline,
			pattern:           "closed-common-table",
			clickhouseIndexes: []string{"closed"},
			expected: Decision{
				IsClosed: true,
			},
			indexConf: indexConf,
		},
		{
			name:              "ingest closed index",
			pipeline:          QueryPipeline,
			pattern:           "unknown-target",
			clickhouseIndexes: []string{"closed"},
			expected: Decision{
				Err: fmt.Errorf("unsupported target"),
			},
			indexConf: indexConf,
		},
		{
			name:              "ingest to index1",
			pipeline:          IngestPipeline,
			pattern:           "index1",
			clickhouseIndexes: []string{"index1"},
			expected: Decision{
				UseConnectors: []ConnectorDecision{&ConnectorDecisionClickhouse{
					ClickhouseTableName: "index1",
					ClickhouseTables:    []string{"index1"}},
				},
			},
			indexConf: indexConf,
		},
		{
			name:              "query from index1",
			pipeline:          QueryPipeline,
			pattern:           "index1",
			clickhouseIndexes: []string{"index1"},
			expected: Decision{
				UseConnectors: []ConnectorDecision{&ConnectorDecisionClickhouse{
					ClickhouseTableName: "index1",
					ClickhouseTables:    []string{"index1"}},
				},
			},
			indexConf: indexConf,
		},
		{
			name:              "ingest to index2",
			pipeline:          IngestPipeline,
			pattern:           "index2",
			clickhouseIndexes: []string{"index2"},
			expected: Decision{
				UseConnectors: []ConnectorDecision{&ConnectorDecisionClickhouse{
					ClickhouseTableName: common_table.TableName,
					ClickhouseTables:    []string{"index2"},
					IsCommonTable:       true,
				}},
			},
			indexConf: indexConf,
		},
		{
			name:              "query from index2",
			pipeline:          QueryPipeline,
			pattern:           "index2",
			clickhouseIndexes: []string{"index2"},
			expected: Decision{
				UseConnectors: []ConnectorDecision{&ConnectorDecisionClickhouse{
					ClickhouseTableName: common_table.TableName,
					ClickhouseTables:    []string{"index2"},
					IsCommonTable:       true,
				}},
			},
			indexConf: indexConf,
		},
		{
			name:           "query from index1,index2",
			pipeline:       QueryPipeline,
			pattern:        "index1,index2",
			elasticIndexes: []string{"index3"},
			expected: Decision{
				Err: end_user_errors.ErrSearchCondition.New(fmt.Errorf("")),
			},
			indexConf: indexConf,
		},
		{
			name:           "query from index1,index-not-existing",
			pipeline:       QueryPipeline,
			pattern:        "index1,index-not-existing",
			elasticIndexes: []string{"index1,index-not-existing"},
			expected: Decision{
				UseConnectors: []ConnectorDecision{&ConnectorDecisionClickhouse{
					ClickhouseTableName: "index1",
					ClickhouseTables:    []string{"index1"},
					IsCommonTable:       false,
				}},
			},
			indexConf: indexConf,
		},
		{
			name:           "ingest to index3",
			pipeline:       IngestPipeline,
			pattern:        "index3",
			elasticIndexes: []string{"index3"},
			expected: Decision{
				UseConnectors: []ConnectorDecision{&ConnectorDecisionElastic{}},
			},
			indexConf: indexConf,
		},
		{
			name:           "query from index3",
			pipeline:       QueryPipeline,
			pattern:        "index3",
			elasticIndexes: []string{"index3"},
			expected: Decision{
				UseConnectors: []ConnectorDecision{&ConnectorDecisionElastic{}},
			},
			indexConf: indexConf,
		},

		{
			name:          "query pattern",
			pipeline:      QueryPipeline,
			pattern:       "index2,foo*",
			virtualTables: []string{"index2"},
			expected: Decision{
				UseConnectors: []ConnectorDecision{&ConnectorDecisionClickhouse{
					ClickhouseTableName: common_table.TableName,
					ClickhouseTables:    []string{"index2"},
					IsCommonTable:       true,
				}},
			},
			indexConf: indexConf,
		},
		{
			name:          "query pattern",
			pipeline:      QueryPipeline,
			pattern:       "indexa,index2",
			virtualTables: []string{"index2"},
			expected: Decision{
				UseConnectors: []ConnectorDecision{&ConnectorDecisionClickhouse{
					ClickhouseTableName: common_table.TableName,
					ClickhouseTables:    []string{"index2"},
					IsCommonTable:       true,
				}},
			},
			indexConf: indexConf,
		},
		{
			name:     "query kibana internals",
			pipeline: QueryPipeline,
			pattern:  ".kibana",
			expected: Decision{
				UseConnectors: []ConnectorDecision{&ConnectorDecisionElastic{ManagementCall: true}},
			},
			indexConf: indexConf,
		},
		{
			name:     "ingest kibana internals",
			pipeline: IngestPipeline,
			pattern:  ".kibana",
			expected: Decision{
				UseConnectors: []ConnectorDecision{&ConnectorDecisionElastic{ManagementCall: true}},
			},
			indexConf: indexConf,
		},
		{
			name:     "ingest not configured index",
			pipeline: IngestPipeline,
			pattern:  "not-configured",
			expected: Decision{
				UseConnectors: []ConnectorDecision{&ConnectorDecisionElastic{}},
			},
			indexConf: indexConf,
		},
		{
			name:     "double write",
			pipeline: IngestPipeline,
			pattern:  "logs",
			expected: Decision{
				UseConnectors: []ConnectorDecision{&ConnectorDecisionClickhouse{
					ClickhouseTableName: "logs",
					ClickhouseTables:    []string{"logs"},
				},
					&ConnectorDecisionElastic{}},
			},
			indexConf: indexConf,
		},
		{
			name:     "A/B testing",
			pipeline: QueryPipeline,
			pattern:  "logs",
			expected: Decision{
				EnableABTesting: true,
				UseConnectors: []ConnectorDecision{&ConnectorDecisionClickhouse{
					ClickhouseTableName: "logs",
					ClickhouseTables:    []string{"logs"},
				},
					&ConnectorDecisionElastic{}},
			},
			indexConf: indexConf,
		},
		{
			name:              "query both connectors",
			pipeline:          QueryPipeline,
			pattern:           "logs,index1",
			indexConf:         indexConf,
			clickhouseIndexes: []string{"index1"},
			elasticIndexes:    []string{"logs"},
			expected: Decision{
				Err: end_user_errors.ErrSearchCondition.New(fmt.Errorf("")),
			},
		},
		{
			name:           "query elastic with pattern",
			pipeline:       QueryPipeline,
			pattern:        "some-elastic-logs*",
			elasticIndexes: []string{"logs"},
			expected: Decision{
				UseConnectors: []ConnectorDecision{&ConnectorDecisionElastic{
					ManagementCall: false,
				}},
			},
		},
		{
			name:           "non matching pattern",
			pipeline:       QueryPipeline,
			pattern:        "some-non-matching-pattern*",
			elasticIndexes: []string{"logs"},
			expected: Decision{
				IsEmpty: true,
			},
		},
		{
			name:     "query internal index",
			pipeline: QueryPipeline,
			pattern:  "quesma_common_table",
			expected: Decision{
				Err: fmt.Errorf("common table"),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

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

			resolver := NewTableResolver(cfg, tableDiscovery, elasticResolver)

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
