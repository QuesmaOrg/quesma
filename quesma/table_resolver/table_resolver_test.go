// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package table_resolver

import (
	"fmt"
	"github.com/k0kubun/pp"
	"github.com/stretchr/testify/assert"
	"quesma/clickhouse"
	"quesma/elasticsearch"
	"quesma/end_user_errors"
	"quesma/quesma/config"
	"reflect"
	"strings"
	"testing"
)

func TestTableResolver(t *testing.T) {

	indexConf := map[string]config.IndexConfiguration{
		"index1": config.IndexConfiguration{
			QueryTarget:  []string{"clickhouse"},
			IngestTarget: []string{"clickhouse"},
		},
	}

	cfg := config.QuesmaConfiguration{IndexConfig: indexConf}

	tests := []struct {
		name              string
		pipeline          string
		pattern           string
		elasticIndexes    []string
		clickhouseIndexes []string

		expected Decision
	}{
		{
			name:     "elastic fallback",
			pipeline: IngestPipeline,
			pattern:  "some-index",
			expected: Decision{
				UseConnectors: []ConnectorDecision{&ConnectorDecisionElastic{}},
			},
		},
		{
			name:     "all",
			pipeline: QueryPipeline,
			pattern:  "*",
			expected: Decision{
				IsEmpty: true,
			},
		},

		{
			name:              "empty *",
			pipeline:          QueryPipeline,
			pattern:           "*",
			clickhouseIndexes: []string{"index1", "index2"},
			expected: Decision{
				IsEmpty: true,
			},
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
		},
		{
			name:              "ingest to index1",
			pipeline:          IngestPipeline,
			pattern:           "index1",
			clickhouseIndexes: []string{"index1"},
			expected: Decision{
				UseConnectors: []ConnectorDecision{&ConnectorDecisionClickhouse{
					ClickhouseTableName: "index1",
					Indexes:             []string{"index1"}},
				},
			},
		},
		{
			name:              "query from index1",
			pipeline:          QueryPipeline,
			pattern:           "index1",
			clickhouseIndexes: []string{"index1"},
			expected: Decision{
				UseConnectors: []ConnectorDecision{&ConnectorDecisionClickhouse{
					ClickhouseTableName: "index1",
					Indexes:             []string{"index1"}},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			tableDiscovery := clickhouse.NewEmptyTableDiscovery()

			if len(tt.clickhouseIndexes) > 0 {
				for _, index := range tt.clickhouseIndexes {
					tableDiscovery.TableMap.Store(index, &clickhouse.Table{
						Name: index,
					})
				}
			}

			elasticResolver := elasticsearch.NewEmptyIndexResolver()

			sources := elasticsearch.Sources{
				Indices: make([]elasticsearch.Index, 0),
			}

			for _, index := range tt.elasticIndexes {
				sources.Indices = append(sources.Indices, elasticsearch.Index{
					Name: index,
				})
			}
			elasticResolver.Indexes["*"] = sources

			resolver := NewTableResolver(cfg, tableDiscovery, elasticResolver)

			decision := resolver.Resolve(tt.pipeline, tt.pattern)

			assert.NotNil(t, decision)
			if tt.expected.Err != nil {
				if !strings.Contains(decision.Err.Error(), tt.expected.Err.Error()) {
					t.Errorf("Error is not an instance of the expected error: got %v, expected %v", decision.Err, tt.expected.Err)
				}
			} else {
				assert.Nil(t, decision.Err)
			}
			assert.Equal(t, tt.expected.IsClosed, decision.IsClosed, "expected %v, got %v", tt.expected.IsClosed, decision.IsClosed)
			assert.Equal(t, tt.expected.IsEmpty, decision.IsEmpty, "expected %v, got %v", tt.expected.IsEmpty, decision.IsEmpty)

			if !reflect.DeepEqual(tt.expected.UseConnectors, decision.UseConnectors) {
				pp.Println(tt.expected)
				pp.Println(decision)
				t.Errorf("UseConnectors didn't match, expected %v, got %v", tt.expected.UseConnectors, decision.UseConnectors)
			}

		})
	}

}
