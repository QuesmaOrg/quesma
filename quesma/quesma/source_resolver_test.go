// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package quesma

import (
	"github.com/stretchr/testify/assert"
	"quesma/elasticsearch"
	"quesma/quesma/config"
	"quesma/schema"
	"quesma/util"
	"testing"
)

func TestResolveSources(t *testing.T) {
	type args struct {
		indexPattern string
		cfg          config.QuesmaConfiguration
		im           elasticsearch.IndexManagement
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "Index only in Clickhouse,pattern:",
			args: args{
				indexPattern: "test",
				cfg:          config.QuesmaConfiguration{IndexConfig: map[string]config.IndexConfiguration{"test": {QueryTarget: []string{config.ClickhouseTarget}, IngestTarget: []string{config.ClickhouseTarget}}}},
				im:           NewFixedIndexManagement(),
			},
			want: sourceClickhouse,
		},
		{
			name: "Index only in Clickhouse,pattern:",
			args: args{
				indexPattern: "*",
				cfg:          config.QuesmaConfiguration{IndexConfig: map[string]config.IndexConfiguration{"test": {QueryTarget: []string{config.ClickhouseTarget}, IngestTarget: []string{config.ClickhouseTarget}}}},
				im:           NewFixedIndexManagement(),
			},
			want: sourceClickhouse,
		},
		{
			name: "Index only in Elasticsearch,pattern:",
			args: args{
				indexPattern: "test",
				cfg:          config.QuesmaConfiguration{IndexConfig: map[string]config.IndexConfiguration{}},
				im:           NewFixedIndexManagement("test"),
			},
			want: sourceElasticsearch,
		},
		{
			name: "Index only in Elasticsearch,pattern:",
			args: args{
				indexPattern: "*",
				cfg:          config.QuesmaConfiguration{IndexConfig: map[string]config.IndexConfiguration{}},
				im:           NewFixedIndexManagement("test"),
			},
			want: sourceElasticsearch,
		},
		{
			name: "Indexes both in Elasticsearch and Clickhouse",
			args: args{
				indexPattern: "*",
				cfg:          config.QuesmaConfiguration{IndexConfig: map[string]config.IndexConfiguration{"kibana-sample-data-logs": {QueryTarget: []string{config.ClickhouseTarget}, IngestTarget: []string{config.ClickhouseTarget}}}},
				im:           NewFixedIndexManagement("logs-generic-default"),
			},
			want: sourceBoth,
		},
		{
			name: "Indexes both in Elasticsearch and Clickhouse, but configured to Elastic",
			args: args{
				indexPattern: "*",
				cfg:          config.QuesmaConfiguration{IndexConfig: map[string]config.IndexConfiguration{"logs-generic-default": {QueryTarget: []string{config.ElasticsearchTarget}, IngestTarget: []string{config.ElasticsearchTarget}}}},
				im:           NewFixedIndexManagement("logs-generic-default"),
			},
			want: sourceElasticsearch,
		},
		{
			name: "Index neither in Clickhouse nor in Elasticsearch",
			args: args{
				indexPattern: "*",
				cfg:          config.QuesmaConfiguration{IndexConfig: map[string]config.IndexConfiguration{}},
				im:           NewFixedIndexManagement(),
			},
			want: sourceNone,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name+tt.args.indexPattern, func(t *testing.T) {
			got, _, _ := ResolveSources(tt.args.indexPattern, &tt.args.cfg, tt.args.im, &schema.StaticRegistry{})
			assert.Equalf(t, tt.want, got, "ResolveSources(%v, %v, %v)", tt.args.indexPattern, tt.args.cfg, tt.args.im)
		})
	}
}

func NewFixedIndexManagement(indexes ...string) elasticsearch.IndexManagement {
	return stubIndexManagement{indexes: indexes}
}

type stubIndexManagement struct {
	indexes []string
}

func (s stubIndexManagement) Start()         {}
func (s stubIndexManagement) Stop()          {}
func (s stubIndexManagement) ReloadIndices() {}
func (s stubIndexManagement) GetSources() elasticsearch.Sources {
	var dataStreams = []elasticsearch.DataStream{}
	for _, index := range s.indexes {
		dataStreams = append(dataStreams, elasticsearch.DataStream{Name: index})
	}
	return elasticsearch.Sources{DataStreams: dataStreams}
}

func (s stubIndexManagement) GetSourceNames() map[string]bool {
	var result = make(map[string]bool)
	for _, index := range s.indexes {
		result[index] = true
	}
	return result
}

func (s stubIndexManagement) GetSourceNamesMatching(indexPattern string) map[string]bool {
	var result = make(map[string]bool)
	for _, index := range s.indexes {
		if util.IndexPatternMatches(indexPattern, index) {
			result[index] = true
		}
	}
	return result
}
