// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package resolve

import (
	"quesma/elasticsearch"
	"quesma/logger"
	"quesma/quesma/config"
	"quesma/schema"
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
	for _, name := range chTableNames { // Quesma presents CH tables as Elasticsearch Data Streams.
		sourcesFromElastic.DataStreams = append(sourcesFromElastic.DataStreams, elasticsearch.DataStream{
			Name:           name,
			BackingIndices: []string{name},
			TimestampField: `@timestamp`,
		})
	}
}
