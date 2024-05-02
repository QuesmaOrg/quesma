package quesma

import (
	"context"
	"mitmproxy/quesma/clickhouse"
	"mitmproxy/quesma/elasticsearch"
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/quesma/config"
	"slices"
	"strings"
)

const (
	sourceElasticsearch = "elasticsearch"
	sourceClickhouse    = "clickhouse"
	sourceBoth          = "both"
	sourceNone          = "none"
)

func ResolveSources(indexPattern string, cfg config.QuesmaConfiguration, im elasticsearch.IndexManagement, lm *clickhouse.LogManager) string {
	if elasticsearch.IsIndexPattern(indexPattern) {
		matchesElastic := []string{}
		matchesClickhouse := []string{}

		for _, pattern := range strings.Split(indexPattern, ",") {
			for indexName := range im.GetSourceNamesMatching(pattern) {
				if !strings.HasPrefix(indexName, ".") {
					matchesElastic = append(matchesElastic, indexName)
				}
			}

			matchesClickhouse = append(matchesClickhouse, lm.ResolveIndexes(context.Background(), pattern)...)
		}
		slices.Sort(matchesElastic)
		slices.Sort(matchesClickhouse)
		matchesElastic = slices.Compact(matchesElastic)
		matchesClickhouse = slices.Compact(matchesClickhouse)
		matchesElastic = slices.DeleteFunc(matchesElastic, func(s string) bool {
			return slices.Contains(matchesClickhouse, s)
		})

		logger.Debug().Msgf("Resolved sources for index pattern %s: (Elasticsearch: %s), (Clickhouse: %s)", indexPattern, strings.Join(matchesElastic, ", "), strings.Join(matchesClickhouse, ", "))

		switch {
		case len(matchesElastic) > 0 && len(matchesClickhouse) > 0:
			return sourceBoth
		case len(matchesElastic) > 0:
			return sourceElasticsearch
		case len(matchesClickhouse) > 0:
			return sourceClickhouse
		default:
			return sourceNone
		}
	} else {
		if c, exists := cfg.IndexConfig[indexPattern]; exists {
			if c.Enabled {
				return sourceClickhouse
			} else {
				return sourceElasticsearch
			}
		} else {
			return sourceElasticsearch
		}
	}
}
