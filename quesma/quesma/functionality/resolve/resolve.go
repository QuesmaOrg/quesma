// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package resolve

import (
	"quesma/elasticsearch"
	"quesma/quesma/config"
	"quesma/schema"
	"slices"
)

func HandleResolve(pattern string, sr schema.Registry, cfg *config.QuesmaConfiguration) (elasticsearch.Sources, error) {
	// In the _resolve endpoint we want to combine the results from both schema.Registry and Elasticsearch

	normalizedPattern := elasticsearch.NormalizePattern(pattern)

	// Optimization: if it's not a pattern, let's try avoiding querying Elasticsearch - let's first try
	// finding that index in schema.Registry:
	if !elasticsearch.IsIndexPattern(normalizedPattern) {
		if foundSchema, found := sr.FindSchema(schema.TableName(normalizedPattern)); found {
			if !foundSchema.ExistsInDataSource {
				// index configured by the user, but not present in the data source
				return elasticsearch.Sources{}, nil
			}

			return elasticsearch.Sources{
				Indices: []elasticsearch.Index{},
				Aliases: []elasticsearch.Alias{},
				DataStreams: []elasticsearch.DataStream{
					{
						Name:           normalizedPattern,
						BackingIndices: []string{normalizedPattern},
						TimestampField: `@timestamp`,
					},
				},
			}, nil
		}

		// ...index not found in schema.Registry (meaning the user did not configure it), but it could exist in Elastic
	}

	// Combine results from both schema.Registry and Elasticsearch:

	// todo avoid creating new instances all the time
	sourcesFromElastic, _, err := elasticsearch.NewIndexResolver(cfg.Elasticsearch).Resolve(normalizedPattern)
	if err != nil {
		return elasticsearch.Sources{}, err
	}

	combineSourcesFromElasticWithRegistry(&sourcesFromElastic, sr.AllSchemas(), normalizedPattern)
	return sourcesFromElastic, nil
}

func combineSourcesFromElasticWithRegistry(sourcesFromElastic *elasticsearch.Sources, schemas map[schema.TableName]schema.Schema, normalizedPattern string) {
	sourcesFromElastic.Indices =
		slices.DeleteFunc(sourcesFromElastic.Indices, func(i elasticsearch.Index) bool {
			_, exists := schemas[schema.TableName(i.Name)]
			return exists
		})
	sourcesFromElastic.DataStreams = slices.DeleteFunc(sourcesFromElastic.DataStreams, func(i elasticsearch.DataStream) bool {
		_, exists := schemas[schema.TableName(i.Name)]
		return exists
	})

	for name, currentSchema := range schemas {
		indexName := name.AsString()

		if config.MatchName(normalizedPattern, indexName) && currentSchema.ExistsInDataSource {
			sourcesFromElastic.DataStreams = append(sourcesFromElastic.DataStreams, elasticsearch.DataStream{
				Name:           indexName,
				BackingIndices: []string{indexName},
				TimestampField: `@timestamp`,
			})
		}
	}
}
