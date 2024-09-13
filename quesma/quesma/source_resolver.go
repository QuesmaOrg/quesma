// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package quesma

import (
	"quesma/elasticsearch"
	"quesma/logger"
	"quesma/quesma/config"
	"quesma/schema"
	"quesma/util"
	"slices"
	"strings"
)

const (
	sourceElasticsearch = "elasticsearch"
	sourceClickhouse    = "clickhouse"
	sourceBoth          = "both"
	sourceNone          = "none"
)

func ResolveSources(indexPattern string, cfg *config.QuesmaConfiguration, im elasticsearch.IndexManagement, sr schema.Registry) (string, []string, []string) {
	if elasticsearch.IsIndexPattern(indexPattern) {
		matchesElastic := []string{}
		matchesClickhouse := []string{}

		for _, pattern := range strings.Split(indexPattern, ",") {
			for indexName := range im.GetSourceNamesMatching(pattern) {
				if !strings.HasPrefix(indexName, ".") {
					matchesElastic = append(matchesElastic, indexName)
				}
			}
			if cfg.IndexAutodiscoveryEnabled() {
				for tableName := range sr.AllSchemas() {
					if config.MatchName(elasticsearch.NormalizePattern(indexPattern), string(tableName)) {
						matchesClickhouse = append(matchesClickhouse, string(tableName))
					}
				}
			}

			for indexName, indexConfig := range cfg.IndexConfig {
				if util.IndexPatternMatches(pattern, indexName) && !indexConfig.Disabled {
					matchesClickhouse = append(matchesClickhouse, indexName)
				}
			}
		}
		matchesElastic = util.Distinct(matchesElastic)
		matchesClickhouse = util.Distinct(matchesClickhouse)

		matchesElastic = slices.DeleteFunc(matchesElastic, func(s string) bool {
			return slices.Contains(matchesClickhouse, s)
		})

		logger.Debug().Msgf("Resolved sources for index pattern %s: (Elasticsearch: %s), (Clickhouse: %s)", indexPattern, strings.Join(matchesElastic, ", "), strings.Join(matchesClickhouse, ", "))

		switch {
		case len(matchesElastic) > 0 && len(matchesClickhouse) > 0:
			return sourceBoth, matchesElastic, matchesClickhouse
		case len(matchesElastic) > 0:
			return sourceElasticsearch, matchesElastic, matchesClickhouse
		case len(matchesClickhouse) > 0:
			return sourceClickhouse, matchesElastic, matchesClickhouse
		default:
			return sourceNone, matchesElastic, matchesClickhouse
		}
	} else {
		if c, exists := cfg.IndexConfig[indexPattern]; exists {
			if !c.Disabled {
				return sourceClickhouse, []string{}, []string{indexPattern}
			} else {
				return sourceElasticsearch, []string{indexPattern}, []string{}
			}
		} else {
			return sourceElasticsearch, []string{indexPattern}, []string{}
		}
	}
}
