// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package resolve

import (
	"github.com/QuesmaOrg/quesma/platform/common_table"
	"github.com/QuesmaOrg/quesma/platform/config"
	"github.com/QuesmaOrg/quesma/platform/elasticsearch"
	"github.com/QuesmaOrg/quesma/platform/logger"
	"github.com/QuesmaOrg/quesma/platform/schema"
)

// HandleResolve combines results from both schema.Registry (ClickHouse) and Elasticsearch,
// This endpoint is used in Kibana/OSD when creating Data Views/Index Patterns.
func HandleResolve(pattern string, sr schema.Registry, ir elasticsearch.IndexResolver) (elasticsearch.Sources, error) {
	sourcesToShow := &elasticsearch.Sources{}

	normalizedPattern := elasticsearch.NormalizePattern(pattern) // changes `_all` to `*`

	sourcesFromElasticsearch, _, err := ir.Resolve(normalizedPattern)
	if err != nil {
		logger.Warn().Msgf("Failed fetching resolving sources matching `%s`: %v", pattern, err)
	} else {
		sourcesToShow = &sourcesFromElasticsearch
	}

	var filteredOut []elasticsearch.Index
	for _, index := range sourcesToShow.Indices {
		if index.Name == common_table.VirtualTableElasticIndexName {
			// don't include the common table in the results
			// it's internal table used by Quesma, and should be exposed as an index / data stream
			continue
		}
		filteredOut = append(filteredOut, index)

	}
	sourcesToShow.Indices = filteredOut

	tablesFromClickHouse := getMatchingClickHouseTables(sr.AllSchemas(), normalizedPattern)

	addClickHouseTablesToSourcesFromElastic(sourcesToShow, tablesFromClickHouse)

	return *sourcesToShow, nil
}

func getMatchingClickHouseTables(schemas map[schema.IndexName]schema.Schema, normalizedPattern string) (tables []string) {
	for name, currentSchema := range schemas {
		indexName := name.AsString()
		if config.MatchName(normalizedPattern, indexName) && currentSchema.ExistsInDataSource {
			tables = append(tables, indexName)
		}
	}
	return tables
}

func addClickHouseTablesToSourcesFromElastic(sourcesFromElastic *elasticsearch.Sources, chTableNames []string) {
	for _, name := range chTableNames {

		if name == common_table.TableName {
			// don't include the common table in the results
			// it's internal table used by Quesma, and should be exposed as an index / data stream
			continue
		}

		// Quesma presents CH tables as Elasticsearch Data Streams.
		sourcesFromElastic.DataStreams = append(sourcesFromElastic.DataStreams, elasticsearch.DataStream{
			Name:           name,
			BackingIndices: []string{name},
			TimestampField: `@timestamp`,
		})
	}
}
