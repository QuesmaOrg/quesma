// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package resolve

import (
	"github.com/stretchr/testify/assert"
	"quesma/elasticsearch"
	"quesma/schema"
	"testing"
)

func Test_combineSourcesFromElasticWithRegistry(t *testing.T) {
	// Expected behavior:
	//
	// #  | In Elastic? | Exists in data source? | Enabled in the config (= present in schema.Registry)? | Quesma response
	// 1  | NO          | NO                     | NO                                                    | Not exists
	// 2  | NO          | NO                     | YES                                                   | Not exists
	// 3  | YES         | NO                     | NO                                                    | Exists
	// 4  | YES         | NO                     | YES                                                   | Not exist
	// 5  | NO          | YES                    | NO                                                    | Not exist
	// 6  | NO          | YES                    | YES                                                   | Exists
	// 7  | YES         | YES                    | NO                                                    | Exists
	// 8  | YES         | YES                    | YES                                                   | Exists

	tests := []struct {
		name               string
		sourcesFromElastic elasticsearch.Sources
		schemas            map[schema.IndexName]schema.Schema
		normalizedPattern  string
		expectedResult     elasticsearch.Sources
	}{
		// Cases 1, 3 (index1), 5, 7 (index1):
		{
			name: "index not enabled in config, some unrelated index in Elastic",
			sourcesFromElastic: elasticsearch.Sources{
				Indices:     []elasticsearch.Index{{Name: "index1"}},
				Aliases:     []elasticsearch.Alias{},
				DataStreams: []elasticsearch.DataStream{},
			},
			schemas:           map[schema.IndexName]schema.Schema{}, // schema.Registry won't contain disabled indexes, even if they exist in the data source (manually created by the user)
			normalizedPattern: "index*",
			expectedResult: elasticsearch.Sources{
				Indices:     []elasticsearch.Index{{Name: "index1"}},
				Aliases:     []elasticsearch.Alias{},
				DataStreams: []elasticsearch.DataStream{},
			},
		},
		// Cases 2 (index2), 4 (index1):
		{
			name: "index enabled in config, not present in the data source; decoy index in Elastic",
			sourcesFromElastic: elasticsearch.Sources{
				Indices:     []elasticsearch.Index{{Name: "index1"} /* decoy */, {Name: "index3"}},
				Aliases:     []elasticsearch.Alias{},
				DataStreams: []elasticsearch.DataStream{},
			},
			schemas: map[schema.IndexName]schema.Schema{
				"index1": schema.Schema{ExistsInDataSource: false},
				"index2": schema.Schema{ExistsInDataSource: false},
				"quesma": schema.Schema{ExistsInDataSource: true},
			},
			normalizedPattern: "index*",
			expectedResult: elasticsearch.Sources{
				Indices:     []elasticsearch.Index{{Name: "index1"}, {Name: "index3"}},
				Aliases:     []elasticsearch.Alias{},
				DataStreams: []elasticsearch.DataStream{},
			},
		},
		// Cases 6 (index2), 8 (index1, index3):
		{
			name: "index enabled in config, present in the data source",
			sourcesFromElastic: elasticsearch.Sources{
				Indices:     []elasticsearch.Index{{Name: "index1"}, {Name: "index4"}},
				Aliases:     []elasticsearch.Alias{},
				DataStreams: []elasticsearch.DataStream{{Name: "index3"}, {Name: "index5"}},
			},
			schemas: map[schema.IndexName]schema.Schema{
				"index1": schema.Schema{ExistsInDataSource: true},
				"index2": schema.Schema{ExistsInDataSource: true},
				"index3": schema.Schema{ExistsInDataSource: true},
				"quesma": schema.Schema{ExistsInDataSource: true},
			},
			normalizedPattern: "index*",
			expectedResult: elasticsearch.Sources{
				Indices: []elasticsearch.Index{{Name: "index1"}, {Name: "index4"}},
				Aliases: []elasticsearch.Alias{},
				DataStreams: []elasticsearch.DataStream{
					{Name: "index3"},
					{Name: "index5"},
					{Name: "index1", BackingIndices: []string{"index1"}, TimestampField: `@timestamp`},
					{Name: "index2", BackingIndices: []string{"index2"}, TimestampField: `@timestamp`},
					{Name: "index3", BackingIndices: []string{"index3"}, TimestampField: `@timestamp`},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			addClickHouseTablesToSourcesFromElastic(&tt.sourcesFromElastic, getMatchingClickHouseTables(tt.schemas, tt.normalizedPattern))
			assert.ElementsMatchf(t, tt.sourcesFromElastic.Aliases, tt.expectedResult.Aliases, "Aliases don't match")
			assert.ElementsMatchf(t, tt.sourcesFromElastic.Indices, tt.expectedResult.Indices, "Indices don't match")
			assert.ElementsMatchf(t, tt.sourcesFromElastic.DataStreams, tt.expectedResult.DataStreams, "DataStreams don't match")
		})
	}
}
